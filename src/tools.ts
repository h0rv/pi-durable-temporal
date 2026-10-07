import type { Context as ChordContext, JsonValue } from "@earendil-works/chord";
import { BACKGROUND_CONTEXT, withAbortSignal } from "@earendil-works/chord/context";
import {
	createRegistry,
	defineExtension,
	type ToolDiagnostic,
	type ToolExecutionApi,
	type ToolExecutionResult,
	type ToolRegistration,
} from "@earendil-works/pi-durable";
import type { ExecutionEnv } from "@earendil-works/pi-durable/env";
import { Context, getClient } from "@temporalio/activity";
import { ApplicationFailure } from "@temporalio/common";
import { Value } from "typebox/value";
import type { ToolCallInfo } from "./types.js";

export type ToolActivityOptions = { env: (call: ToolCallInfo) => ExecutionEnv | Promise<ExecutionEnv> };

/** Adapt environment-based Pi tools. Session operations belong in workflow-side tools. */
export function createToolActivities(tools: readonly ToolRegistration[], options: ToolActivityOptions) {
	const registry = createRegistry();
	registry.install(defineExtension({ name: "worker-tools", tools }));
	const activities: Record<string, (args: unknown, call: ToolCallInfo) => Promise<ToolExecutionResult>> = {};
	const unsupported = () => {
		throw ApplicationFailure.nonRetryable("Pi session operations must run in the workflow", "PiToolSessionOperation");
	};
	for (const tool of tools) {
		activities[tool.name] = async (args: unknown, call: ToolCallInfo): Promise<ToolExecutionResult> => {
			const context = withAbortSignal(Context.current().cancellationSignal, BACKGROUND_CONTEXT);
			const env = await options.env(call);
			const diagnostics: ToolDiagnostic[] = [];
			let output = "";
			let details: JsonValue | undefined;
			let publication = Promise.resolve();
			let lastPublication = 0;
			const target = call.progress;
			const attempt = Context.current().info.attempt;
			const flush = () => {
				if (!target) return;
				const retained = output;
				publication = publication.then(() =>
					getClient()
						.workflow.getHandle(target.workflowId, target.runId)
						.signal(`pi_tool_progress:${tool.name}`, { requestId: target.requestId, attempt, output: retained }),
				);
				lastPublication = Date.now();
			};
			const api: ToolExecutionApi = {
				...call,
				registry: registry.snapshot(),
				env,
				outputWindow: undefined,
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
				output(chunk) {
					const text = typeof chunk === "string" ? chunk : new TextDecoder().decode(chunk);
					const value = output + text;
					const maxBytes = tool.outputLimits?.maxBytes ?? 50 * 1024;
					const maxLines = tool.outputLimits?.maxLines ?? 2000;
					const bytes = new TextEncoder().encode(value);
					const retainTail = tool.outputLimits?.retain === "tail";
					output = new TextDecoder().decode(retainTail ? bytes.slice(-maxBytes) : bytes.slice(0, maxBytes));
					const lines = output.split("\n");
					output = (retainTail ? lines.slice(-maxLines) : lines.slice(0, maxLines)).join("\n");
					if (Date.now() - lastPublication >= 100) flush();
				},
				diagnostic(value) {
					diagnostics.push(value);
				},
				async details(value: JsonValue, _context: ChordContext) {
					details = value;
				},
			};
			try {
				const result = await tool.execute(Value.Parse(tool.parameters, args), api, context);
				flush();
				await publication;
				return {
					...result,
					content: result.content ?? [{ type: "text", text: output }],
					details: result.details ?? details,
					diagnostics: [...diagnostics, ...(result.diagnostics ?? [])],
				};
			} finally {
				try {
					await publication;
				} finally {
					await env.cleanup(BACKGROUND_CONTEXT);
				}
			}
		};
	}
	return activities;
}
