import type { Context as ChordContext, JsonValue } from "@earendil-works/chord";
import { BACKGROUND_CONTEXT, withAbortSignal } from "@earendil-works/chord/context";
import {
	createRegistry,
	defineExtension,
	type ToolExecutionApi,
	type ToolRegistration,
} from "@earendil-works/pi-durable";
import type { ExecutionEnv } from "@earendil-works/pi-durable/env";
import { Context, getClient } from "@temporalio/activity";
import { ApplicationFailure } from "@temporalio/common";
import { withHeartbeat } from "./heartbeat.js";
import type { ToolActivityResult, ToolReport } from "./tool-reports.js";
import type { ToolCallInfo } from "./types.js";

export type ToolActivityOptions = { env: (call: ToolCallInfo) => ExecutionEnv | Promise<ExecutionEnv> };

/** Adapt environment-based Pi tools. Session operations belong in workflow-side tools. */
export function createToolActivities(tools: readonly ToolRegistration[], options: ToolActivityOptions) {
	const registry = createRegistry();
	registry.install(defineExtension({ name: "worker-tools", tools }));
	const activities: Record<string, (args: unknown, call: ToolCallInfo) => Promise<ToolActivityResult>> = {};
	const unsupported = () => {
		throw ApplicationFailure.nonRetryable("Pi session operations must run in the workflow", "PiToolSessionOperation");
	};
	for (const tool of tools) {
		activities[tool.name] = async (args: unknown, call: ToolCallInfo): Promise<ToolActivityResult> =>
			withHeartbeat(async () => {
				const context = withAbortSignal(Context.current().cancellationSignal, BACKGROUND_CONTEXT);
				const env = await options.env(call);
				const reports: ToolReport[] = [];
				let published = 0;
				let publication = Promise.resolve();
				let lastPublication = 0;
				const target = call.progress;
				const attempt = Context.current().info.attempt;
				const flush = () => {
					if (!target) return;
					const offset = published;
					const retained = reports.slice(offset);
					published = reports.length;
					publication = publication.then(() =>
						getClient()
							.workflow.getHandle(target.workflowId, target.runId)
							.signal(`pi_tool_progress:${tool.name}`, {
								requestId: target.requestId,
								attempt,
								reports: retained,
								offset,
							}),
					);
					lastPublication = Date.now();
				};
				const api: ToolExecutionApi = {
					...call,
					registry: registry.snapshot(),
					env,
					outputWindow: call.outputWindow,
					agent: unsupported,
					snapshot: unsupported,
					snapshotAsOf: unsupported,
					watchDoc: unsupported,
					commit: unsupported,
					memo: unsupported,
					createTask: unsupported,
					getTask: unsupported,
					waitForTask: unsupported,
					conversation: unsupported,
					output(chunk, skipped) {
						reports.push({
							type: "output",
							chunk: typeof chunk === "string" ? chunk : Array.from(chunk),
							skipped,
						});
						if (Date.now() - lastPublication >= 100) flush();
					},
					diagnostic(value) {
						reports.push({ type: "diagnostic", value: structuredClone(value) });
					},
					async details(value: JsonValue, _context: ChordContext) {
						reports.push({ type: "details", value: structuredClone(value) });
					},
				};
				try {
					const result = await tool.execute(args, api, context);
					flush();
					await publication;
					return { ...result, reports: { piToolReports: reports, attempt } };
				} catch (error) {
					if (context.abortSignal?.aborted) throw error;
					throw ApplicationFailure.fromError(error, {
						details: [
							...(error instanceof ApplicationFailure ? (error.details ?? []) : []),
							{ piToolReports: reports, attempt },
						],
					});
				} finally {
					try {
						await publication;
					} finally {
						await env.cleanup(BACKGROUND_CONTEXT);
					}
				}
			});
	}
	return activities;
}
