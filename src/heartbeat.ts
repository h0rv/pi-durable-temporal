import { Context } from "@temporalio/activity";

export async function withHeartbeat<T>(run: () => Promise<T>): Promise<T> {
	const context = Context.current();
	const timeout = context.info.heartbeatTimeoutMs;
	if (!timeout) return run();
	context.heartbeat();
	const timer = setInterval(() => context.heartbeat(), Math.min(1000, timeout / 2));
	try {
		return await run();
	} finally {
		clearInterval(timer);
	}
}
