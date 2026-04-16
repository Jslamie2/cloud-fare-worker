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
// VALIDATION UTILITIES
// =================================================================
function isValidRideId(rideId: string): boolean {
  return typeof rideId === 'string' && rideId.length > 0 && /^[a-zA-Z0-9\-_]+$/.test(rideId);
}

function isValidTimezone(timezone: string): boolean {
  try {
    Intl.DateTimeFormat(undefined, { timeZone: timezone });
    return true;
  } catch {
    return false;
  }
}

function isValidISOString(dateStr: string): boolean {
  const date = new Date(dateStr);
  return !isNaN(date.getTime()) && dateStr.includes('T');
}

// =================================================================
// RIDE SCHEDULER DURABLE OBJECT
// =================================================================
export class RideScheduler implements DurableObject {
  constructor(private state: DurableObjectState, private env: Env) {}

  async fetch(request: Request): Promise<Response> {
    // Enhanced authentication
    const authHeader = request.headers.get("Authorization");
    if (!this.env.CRON_API_SECRET || authHeader !== `Bearer ${this.env.CRON_API_SECRET}`) {
      return new Response("Unauthorized", { status: 401 });
    }

    // Validate content type
    const contentType = request.headers.get("content-type");
    if (!contentType?.includes("application/json")) {
      return new Response("Content-Type must be application/json", { status: 400 });
    }

    let body;
    try {
      body = await request.json();
    } catch {
      return new Response("Invalid JSON body", { status: 400 });
    }

    const { rideId, startTime, endTime, timezone } = body as {
      rideId?: string;
      startTime?: string;
      endTime?: string;
      timezone?: string;
    };

    // Enhanced validation
    if (!rideId || !startTime || !endTime) {
      return new Response(
        JSON.stringify({ 
          error: "Missing required fields", 
          required: ["rideId", "startTime", "endTime"],
          received: { rideId: !!rideId, startTime: !!startTime, endTime: !!endTime }
        }), 
        { status: 400, headers: { "content-type": "application/json" } }
      );
    }

    if (!isValidRideId(rideId)) {
      return new Response(
        JSON.stringify({ error: "Invalid rideId format" }), 
        { status: 400, headers: { "content-type": "application/json" } }
      );
    }

    if (!isValidISOString(startTime) || !isValidISOString(endTime)) {
      return new Response(
        JSON.stringify({ error: "startTime and endTime must be valid ISO 8601 strings" }), 
        { status: 400, headers: { "content-type": "application/json" } }
      );
    }

    const startTimeMs = new Date(startTime).getTime();
    const endTimeMs = new Date(endTime).getTime();

    if (!Number.isFinite(startTimeMs) || !Number.isFinite(endTimeMs)) {
      return new Response(
        JSON.stringify({ error: "Invalid startTime or endTime" }), 
        { status: 400, headers: { "content-type": "application/json" } }
      );
    }

    if (endTimeMs <= startTimeMs) {
      return new Response(
        JSON.stringify({ error: "endTime must be after startTime" }), 
        { status: 400, headers: { "content-type": "application/json" } }
      );
    }

    // Validate timezone
    const resolvedTimezone = timezone && isValidTimezone(timezone) ? timezone : IOWA_TIMEZONE;

    // Check if start time is in the future (with 5-minute buffer)
    const now = Date.now();
    const fiveMinuteBuffer = 5 * 60 * 1000;
    if (startTimeMs < now - fiveMinuteBuffer) {
      return new Response(
        JSON.stringify({ error: "startTime cannot be more than 5 minutes in the past" }), 
        { status: 400, headers: { "content-type": "application/json" } }
      );
    }

    try {
      await this.state.storage.put("rideId", rideId);
      await this.state.storage.put("startTime", startTime);
      await this.state.storage.put("endTime", endTime);
      await this.state.storage.put("timezone", resolvedTimezone);
      await this.state.storage.put("phase", "scheduled_start");
      await this.state.storage.put("createdAt", new Date().toISOString());

      await this.state.storage.setAlarm(startTimeMs);

      return new Response(JSON.stringify({
        success: true,
        rideId,
        startAlarmAt: startTime,
        endAlarmAt: endTime,
        timezone: resolvedTimezone,
      }), {
        status: 200,
        headers: { "content-type": "application/json" }
      });

    } catch (error) {
      console.error("Failed to schedule ride:", error);
      return new Response(
        JSON.stringify({ error: "Internal server error" }), 
        { status: 500, headers: { "content-type": "application/json" } }
      );
    }
  }

  async alarm(): Promise<void> {
    const rideId = await this.state.storage.get<string>("rideId");
    const endTime = await this.state.storage.get<string>("endTime");
    const phase = await this.state.storage.get<string>("phase") || "scheduled_start";
    const createdAt = await this.state.storage.get<string>("createdAt");

    if (!rideId || !endTime) {
      console.error("Missing ride scheduler state", { rideId, endTime, phase, createdAt });
      return;
    }

    let newStatus: string;
    let nextAlarmTime: number | null = null;

    if (phase === "scheduled_start") {
      newStatus = "ongoing";
      await this.state.storage.put("phase", "scheduled_end");
      nextAlarmTime = new Date(endTime).getTime();
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
          "User-Agent": "Cloudflare-Worker/1.0"
        },
        body: JSON.stringify({ 
          status: newStatus,
          updatedAt: new Date().toISOString()
        }),
      });

      if (!response.ok) {
        const text = await response.text();
        console.error(`Failed to update ride ${rideId}`, {
          status: response.status,
          body: text,
          sentStatus: newStatus,
          targetUrl
        });
        
        // Store failure for potential retry
        await this.state.storage.put("lastFailure", {
          timestamp: new Date().toISOString(),
          status: response.status,
          body: text,
          sentStatus: newStatus
        });
      } else {
        console.log(`Successfully updated ride ${rideId} to status: ${newStatus}`);
        // Clear any previous failures
        await this.state.storage.delete("lastFailure");
      }

      // Set next alarm if needed
      if (nextAlarmTime) {
        await this.state.storage.setAlarm(nextAlarmTime);
      }

    } catch (error) {
      console.error(`Exception while updating ride ${rideId}:`, error);
      
      // Store exception for potential retry
      await this.state.storage.put("lastFailure", {
        timestamp: new Date().toISOString(),
        error: String(error),
        sentStatus: newStatus
      });

      // Retry logic for start phase
      if (phase === "scheduled_start") {
        const retryDelay = 60 * 1000; // 1 minute retry
        await this.state.storage.setAlarm(Date.now() + retryDelay);
      }
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
    const method = request.method;

    // CORS headers
    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, PATCH, DELETE, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    };

    if (method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    if (url.pathname.startsWith("/schedule-ride")) {
      const rideId = url.searchParams.get("rideId");
      if (!rideId) {
        return new Response(
          JSON.stringify({ error: "rideId query param is required" }), 
          { status: 400, headers: { "content-type": "application/json", ...corsHeaders } }
        );
      }

      if (!isValidRideId(rideId)) {
        return new Response(
          JSON.stringify({ error: "Invalid rideId format" }), 
          { status: 400, headers: { "content-type": "application/json", ...corsHeaders } }
        );
      }

      try {
        const id = env.RIDE_SCHEDULER.idFromName(rideId);
        const stub = env.RIDE_SCHEDULER.get(id);
        const response = await stub.fetch(request);
        
        // Add CORS headers to response
        const newHeaders = new Headers(response.headers);
        Object.entries(corsHeaders).forEach(([key, value]) => {
          newHeaders.set(key, value);
        });
        
        return new Response(response.body, {
          status: response.status,
          headers: newHeaders
        });
      } catch (error) {
        console.error("Failed to create/access Durable Object:", error);
        return new Response(
          JSON.stringify({ error: "Failed to schedule ride" }), 
          { status: 500, headers: { "content-type": "application/json", ...corsHeaders } }
        );
      }
    }

    if (url.pathname === "/") {
      return new Response("Cloudflare Worker - Ride Scheduler Service", {
        headers: { "content-type": "text/plain", ...corsHeaders }
      });
    }

    if (url.pathname === "/test-run" && method === "POST") {
      const result = await triggerCampaign(env);
      return new Response(JSON.stringify(result), {
        status: result.success ? 200 : 500,
        headers: { "content-type": "application/json", ...corsHeaders }
      });
    }

    if (url.pathname === "/health") {
      return new Response(JSON.stringify({
        success: true,
        service: "campaign-cron-worker",
        apiBaseUrl: env.API_BASE_URL,
        timezone: IOWA_TIMEZONE,
        now: new Date().toISOString(),
        timestamp: Date.now()
      }), {
        status: 200,
        headers: { "content-type": "application/json", ...corsHeaders }
      });
    }

    return new Response(JSON.stringify({ error: "Not found" }), {
      status: 404,
      headers: { "content-type": "application/json", ...corsHeaders }
    });
  },
} satisfies ExportedHandler<Env>;

// =================================================================
// CAMPAIGN FUNCTIONS
// =================================================================
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
        Authorization: `Bearer ${env.CRON_API_SECRET}`, // Fixed: Use consistent auth
        "content-type": "application/json",
        "User-Agent": "Cloudflare-Worker/1.0"
      },
      body: JSON.stringify({
        source: "cloudflare-worker",
        scheduledAt: new Date().toISOString(),
      }),
    });
    
    const responseText = await response.text();
    return { 
      success: response.ok, 
      status: response.status, 
      data: responseText,
      endpoint
    };
  } catch (error) {
    console.error("Campaign trigger failed:", error);
    return { 
      success: false, 
      error: String(error),
      endpoint
    };
  }
}

async function prewarmCampaignEndpoint(env: Env) {
  const endpoint = `${env.API_BASE_URL.replace(/\/$/, "")}/api/notifications/campaign/five-day`;
  try {
    const response = await fetch(endpoint, {
      method: "GET",
      headers: { 
        Authorization: `Bearer ${env.CRON_API_SECRET}`, // Fixed: Use consistent auth
        "x-prewarm": "true",
        "User-Agent": "Cloudflare-Worker/1.0"
      },
    });
    
    return { 
      success: response.ok, 
      status: response.status,
      endpoint
    };
  } catch (error) {
    console.error("Campaign prewarm failed:", error);
    return { 
      success: false, 
      error: String(error),
      endpoint
    };
  }
}

// =================================================================
// DATE UTILITIES
// =================================================================
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
