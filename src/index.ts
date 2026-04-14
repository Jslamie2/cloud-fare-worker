export interface Env {
  API_BASE_URL: string;
  CRON_API_SECRET: string;
  RIDE_SCHEDULER: DurableObjectNamespace;
}

const IOWA_TIMEZONE = "America/Chicago";
const ANCHOR_DATE = "2026-03-30";
const TARGET_HOUR = 16;
const PREWARM_MINUTE = 44;
const TARGET_MINUTE = 45;

// =================================================================
// RIDE SCHEDULER DURABLE OBJECT
// =================================================================
export class RideScheduler implements DurableObject {
  constructor(private state: DurableObjectState, private env: Env) {}

  async fetch(request: Request): Promise<Response> {
    const authHeader = request.headers.get("Authorization");

    if (
      !this.env.CRON_API_SECRET ||
      authHeader !== `Bearer ${this.env.CRON_API_SECRET}`
    ) {
      return new Response("Unauthorized", { status: 401 });
    }

    const { rideId, startTime, endTime, timezone } = (await request.json()) as {
      rideId?: string;
      startTime?: string;
      endTime?: string;
      timezone?: string;
    };

    if (!rideId || !startTime || !endTime) {
      return new Response(
        "Bad request: Missing rideId, startTime, or endTime",
        { status: 400 }
      );
    }

    const startTimeMs = new Date(startTime).getTime();
    const endTimeMs = new Date(endTime).getTime();

    if (!Number.isFinite(startTimeMs) || !Number.isFinite(endTimeMs)) {
      return new Response("Bad request: Invalid startTime or endTime", {
        status: 400,
      });
    }

    if (endTimeMs <= startTimeMs) {
      return new Response("Bad request: endTime must be after startTime", {
        status: 400,
      });
    }

    await this.state.storage.put("rideId", rideId);
    await this.state.storage.put("startTime", startTime);
    await this.state.storage.put("endTime", endTime);
    await this.state.storage.put("timezone", timezone || IOWA_TIMEZONE);
    await this.state.storage.put("phase", "scheduled_start");

    await this.state.storage.setAlarm(startTimeMs);

    return json({
      success: true,
      rideId,
      startAlarmAt: startTime,
      endAlarmAt: endTime,
    });
  }

  async alarm(): Promise<void> {
    const rideId = await this.state.storage.get<string>("rideId");
    const endTime = await this.state.storage.get<string>("endTime");
    const phase =
      (await this.state.storage.get<string>("phase")) || "scheduled_start";

    if (!rideId || !endTime) {
      console.error("Missing ride scheduler state", { rideId, endTime, phase });
      return;
    }

    let newStatus: string;

    if (phase === "scheduled_start") {
      newStatus = "ongoing";
      await this.state.storage.put("phase", "scheduled_end");
      await this.state.storage.setAlarm(new Date(endTime).getTime());
    } else {
      newStatus = "completed";
      await this.state.storage.put("phase", "finished");
    }

    const targetUrl = `${this.env.API_BASE_URL.replace(/\/$/, "")}/api/rides/${rideId}/update-status`;

    try {
      const response = await fetch(targetUrl, {
        method: "PATCH",
        headers: {
          Authorization: `Bearer ${this.env.CRON_API_SECRET}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ status: newStatus }),
      });

      if (!response.ok) {
        const text = await response.text();
        console.error(`Failed to update ride ${rideId}`, {
          status: response.status,
          body: text,
          sentStatus: newStatus,
        });
      }
    } catch (e) {
      console.error(`Exception while updating ride ${rideId}:`, e);
    }
  }
}

// =================================================================
// MAIN WORKER (Entry Point)
// =================================================================
export default {
  async scheduled(
    controller: ScheduledController,
    env: Env,
    ctx: ExecutionContext
  ): Promise<void> {
    ctx.waitUntil(handleScheduledRun(controller, env));
  },

  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname.startsWith("/schedule-ride")) {
      const rideId = url.searchParams.get("rideId");
      if (!rideId) {
        return new Response("rideId query param is required", { status: 400 });
      }

      const id = env.RIDE_SCHEDULER.idFromName(rideId);
      const stub = env.RIDE_SCHEDULER.get(id);
      return stub.fetch(request);
    }

    if (url.pathname === "/") {
      return new Response("Hello World");
    }

    if (url.pathname === "/test-run" && request.method === "POST") {
      const result = await triggerCampaign(env);
      return json(result, result.success ? 200 : 500);
    }

    if (url.pathname === "/health") {
      return json({
        success: true,
        service: "campaign-cron-worker",
        apiBaseUrl: env.API_BASE_URL,
        timezone: IOWA_TIMEZONE,
        now: new Date().toISOString(),
      });
    }

    return new Response("Not found", { status: 404 });
  },
} satisfies ExportedHandler<Env>;

// --- Helper Functions ---

async function handleScheduledRun(
  controller: ScheduledController,
  env: Env
) {
  const now = new Date();
  const local = getZonedParts(now, IOWA_TIMEZONE);

  if (!isScheduledMinute(local) || !isFiveDaySchedule(local.date)) return;

  if (isPrewarmTime(local)) {
    await prewarmCampaignEndpoint(env);
  } else {
    await triggerCampaign(env);
  }
}

function isScheduledMinute(local: ZonedParts) {
  return isPrewarmTime(local) || isTargetTime(local);
}

function isPrewarmTime(local: ZonedParts) {
  return local.hour === TARGET_HOUR && local.minute === PREWARM_MINUTE;
}

function isTargetTime(local: ZonedParts) {
  return local.hour === TARGET_HOUR && local.minute === TARGET_MINUTE;
}

function isFiveDaySchedule(localDate: string) {
  const daysSinceAnchor = diffDaysUtc(ANCHOR_DATE, localDate);
  return daysSinceAnchor >= 0 && daysSinceAnchor % 5 === 0;
}

async function triggerCampaign(env: Env) {
  const endpoint = `${env.API_BASE_URL.replace(/\/$/, "")}/api/notifications/campaign/five-day`;
  try {
    const response = await fetch(endpoint, {
      method: "POST",
      headers: {
        "x-cron-secret": env.CRON_API_SECRET,
        "content-type": "application/json",
      },
      body: JSON.stringify({
        source: "cloudflare-worker",
        scheduledAt: new Date().toISOString(),
      }),
    });
    return { success: response.ok, status: response.status, data: await response.text() };
  } catch (error) {
    return { success: false, error: String(error) };
  }
}

async function prewarmCampaignEndpoint(env: Env) {
  const endpoint = `${env.API_BASE_URL.replace(/\/$/, "")}/api/notifications/campaign/five-day`;
  try {
    const response = await fetch(endpoint, {
      method: "GET",
      headers: { "x-cron-secret": env.CRON_API_SECRET, "x-prewarm": "true" },
    });
    return { success: response.ok, status: response.status };
  } catch (error) {
    return { success: false, error: String(error) };
  }
}

// --- Date Utilities ---

type ZonedParts = {
  date: string;
  year: number;
  month: number;
  day: number;
  hour: number;
  minute: number;
};

function getZonedParts(date: Date, timeZone: string): ZonedParts {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
  });
  const parts = formatter.formatToParts(date);
  const map: Record<string, string> = {};
  parts.forEach((p) => {
    if (p.type !== "literal") map[p.type] = p.value;
  });
  return {
    date: `${map.year}-${map.month}-${map.day}`,
    year: Number(map.year),
    month: Number(map.month),
    day: Number(map.day),
    hour: Number(map.hour),
    minute: Number(map.minute),
  };
}

function diffDaysUtc(fromDate: string, toDate: string) {
  const from = new Date(`${fromDate}T00:00:00Z`).getTime();
  const to = new Date(`${toDate}T00:00:00Z`).getTime();
  return Math.floor((to - from) / 86_400_000);
}

function json(data: unknown, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}
