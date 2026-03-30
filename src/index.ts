export interface Env {
	API_BASE_URL: string;
	CRON_API_SECRET: string;
}

const IOWA_TIMEZONE = "America/Chicago";
const ANCHOR_DATE = "2026-03-30";
const TARGET_HOUR = 15;
const PREWARM_MINUTE = 14;
const TARGET_MINUTE = 15;

export default {
	async scheduled(
		controller: ScheduledController,
		env: Env,
		ctx: ExecutionContext,
	): Promise<void> {
		ctx.waitUntil(handleScheduledRun(controller, env));
	},

	async fetch(request: Request, env: Env): Promise<Response> {
		const url = new URL(request.url);

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
				anchorDate: ANCHOR_DATE,
				cadenceDays: 5,
				prewarmTime: "3:14 PM",
				targetTime: "3:15 PM",
				now: new Date().toISOString(),
			});
		}

		return new Response("Not found", { status: 404 });
	},
} satisfies ExportedHandler<Env>;

async function handleScheduledRun(controller: ScheduledController, env: Env) {
	const now = new Date();
	const local = getZonedParts(now, IOWA_TIMEZONE);

	console.log(
		JSON.stringify({
			event: "scheduled_run_received",
			cron: controller.cron,
			utcNow: now.toISOString(),
			localDate: local.date,
			localHour: local.hour,
			localMinute: local.minute,
		}),
	);

	if (!isScheduledMinute(local)) {
		console.log("Skipping run: not a scheduled 3:14 PM or 3:15 PM Iowa time");
		return;
	}

	if (!isFiveDaySchedule(local.date)) {
		console.log(
			`Skipping run: ${local.date} is not on the 5-day cadence from ${ANCHOR_DATE}`,
		);
		return;
	}

	if (isPrewarmTime(local)) {
		const result = await prewarmCampaignEndpoint(env);
		console.log(JSON.stringify({ event: "campaign_prewarm_result", result }));
		return;
	}

	const result = await triggerCampaign(env);
	console.log(JSON.stringify({ event: "campaign_result", result }));
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

		const text = await response.text();
		let data: unknown = text;

		try {
			data = JSON.parse(text);
		} catch {
			// Keep raw text if the upstream response is not JSON.
		}

		return {
			success: response.ok,
			status: response.status,
			endpoint,
			data,
		};
	} catch (error) {
		return {
			success: false,
			endpoint,
			error: error instanceof Error ? error.message : String(error),
		};
	}
}

async function prewarmCampaignEndpoint(env: Env) {
	const endpoint = `${env.API_BASE_URL.replace(/\/$/, "")}/api/notifications/campaign/five-day`;

	try {
		const response = await fetch(endpoint, {
			method: "GET",
			headers: {
				"x-cron-secret": env.CRON_API_SECRET,
				"x-prewarm": "true",
			},
		});

		const text = await response.text();
		let data: unknown = text;

		try {
			data = JSON.parse(text);
		} catch {
			// Keep raw text if the upstream response is not JSON.
		}

		return {
			success: response.ok,
			status: response.status,
			endpoint,
			data,
		};
	} catch (error) {
		return {
			success: false,
			endpoint,
			error: error instanceof Error ? error.message : String(error),
		};
	}
}

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

	for (const part of parts) {
		if (part.type !== "literal") {
			map[part.type] = part.value;
		}
	}

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
		headers: {
			"content-type": "application/json; charset=utf-8",
		},
	});
}
