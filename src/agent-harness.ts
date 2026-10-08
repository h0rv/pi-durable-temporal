import "./workflow-globals.js";
import { defineExtension, GenerationTask, hook, ToolTask } from "@earendil-works/pi-durable";
import type { Protocol, ToolApprovalDecision } from "@temporalio/agent-harness-client";
import { condition, defineSignal, patched, setHandler, uuid4, workflowInfo } from "@temporalio/workflow";

export type ApprovalRequest = Extract<Protocol.AgentStreamItem, { type: "tool_approval_requested" }>;
export type ApprovalDecision = ToolApprovalDecision & { tool_id: string; rememberScope?: "tool" | "call" };
export type ApprovalEvaluation = Pick<
	Extract<Protocol.AgentStreamItem, { type: "auto_approval_evaluation_ended" }>,
	"verdict" | "reason" | "details"
>;
export const toolApprovalResolved = defineSignal<[ApprovalDecision]>("approval_resolved");
export type AgentTraceOptions = {
	model: string;
	publish: (event: Protocol.AgentStreamItem) => Promise<void>;
	approvalMode?: "manual" | "auto";
	allowedTools?: readonly string[];
	allowedCalls?: readonly string[];
	evaluate?: (request: ApprovalRequest) => Promise<ApprovalEvaluation>;
	evaluator?: string;
};

function canonicalJson(value: unknown): string {
	if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
	if (value !== null && typeof value === "object")
		return `{${Object.entries(value)
			.filter(([, item]) => item !== undefined)
			.sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
			.map(([key, item]) => `${JSON.stringify(key)}:${canonicalJson(item)}`)
			.join(",")}}`;
	return JSON.stringify(value) ?? "null";
}

export function approvalCallKey(tool: string, args: ApprovalRequest["tool_input"]): string {
	return canonicalJson([tool, args]);
}

/** Trace Pi's generation and tool hooks and wait for approval before tool execution. */
export function createAgentTrace(options: AgentTraceOptions) {
	const decisions = new Map<string, ApprovalDecision>();
	const waiting = new Set<string>();
	const allowed = new Set(options.allowedTools);
	const allowedCalls = new Set(options.allowedCalls);
	const resolve = (decision: ApprovalDecision) => {
		if (waiting.has(decision.tool_id) && !decisions.has(decision.tool_id)) decisions.set(decision.tool_id, decision);
	};
	setHandler(toolApprovalResolved, resolve);
	const extension = defineExtension({
		name: "agent-harness-trace",
		hooks: [
			hook(GenerationTask, {
				beforeRequest: async () => {
					await options.publish({ type: "model_interaction_started", model: options.model });
					return undefined;
				},
				afterResponse: async (message) => {
					for (const block of message.content) {
						if (block.type === "text") await options.publish({ type: "reply_delta", text: block.text });
					}
					await options.publish({
						type: "model_interaction_ended",
						model: options.model,
						usage: {
							input_tokens: message.usage.input,
							output_tokens: message.usage.output,
							cached_tokens: message.usage.cacheRead,
							thought_tokens: null,
							tool_use_tokens: null,
							total_tokens: message.usage.totalTokens,
						},
					});
				},
			}),
			hook(ToolTask, {
				beforeTool: async (call) => {
					const toolId = `${workflowInfo().workflowId}/${call.id}`;
					const fields = { tool_id: toolId, tool_name: call.name, tool_input: call.arguments };
					await options.publish({ type: "tool_requested", ...fields });
					const scopedApprovals = patched("remembered-call-approvals-v1");
					const callKey = approvalCallKey(call.name, call.arguments);
					if (
						patched("tool-approvals-v1") &&
						!allowed.has(call.name) &&
						!(scopedApprovals && allowedCalls.has(callKey))
					) {
						let decision: ApprovalDecision | undefined;
						if (options.approvalMode === "auto" && options.evaluate) {
							const evaluation = {
								tool_id: toolId,
								tool_name: call.name,
								evaluation_id: uuid4(),
								evaluator: options.evaluator ?? "approval",
							};
							await options.publish({ type: "auto_approval_evaluation_started", ...evaluation });
							try {
								const result = await options.evaluate({
									type: "tool_approval_requested",
									...fields,
								});
								await options.publish({ type: "auto_approval_evaluation_ended", ...evaluation, ...result });
								if (result.verdict !== "escalate")
									decision = {
										tool_id: toolId,
										approved: result.verdict === "approve",
										reason: result.reason,
										remember: false,
									};
							} catch (error) {
								await options.publish({
									type: "auto_approval_evaluation_error",
									...evaluation,
									message: error instanceof Error ? error.message : String(error),
								});
							}
						}
						if (!decision) {
							waiting.add(toolId);
							await options.publish({ type: "tool_approval_requested", ...fields });
							await condition(() => decisions.has(toolId));
							decision = decisions.get(toolId)!;
							waiting.delete(toolId);
							decisions.delete(toolId);
						}
						if (decision.approved && decision.remember) {
							if (decision.rememberScope === "call") allowedCalls.add(callKey);
							else allowed.add(call.name);
						}
						await options.publish({
							type: "tool_approval_resolved",
							tool_id: toolId,
							tool_name: call.name,
							approved: decision.approved,
							reason: decision.reason ?? null,
							remember: decision.remember ?? false,
						});
						if (!decision.approved) {
							return { block: decision.reason ?? "Tool denied" };
						}
					}
					await options.publish({ type: "tool_start", ...fields });
					return undefined;
				},
				afterTool: async (call, result) => {
					const fields = { tool_id: `${workflowInfo().workflowId}/${call.id}`, tool_name: call.name };
					if (result.isError)
						await options.publish({ type: "tool_error", ...fields, message: JSON.stringify(result) });
					else await options.publish({ type: "tool_end", ...fields, tool_output: JSON.stringify(result) });
					return undefined;
				},
			}),
		],
	});
	return { extension, resolve, pending: () => [...waiting] };
}

/** Publish a named state using the community harness's versioned state events. */
export function createObservableState<T extends Extract<Protocol.AgentStreamItem, { type: "state_snapshot" }>["value"]>(
	name: string,
	initial: T,
	publish: (event: Protocol.AgentStreamItem) => void,
) {
	let version = 0;
	let value = structuredClone(initial);
	publish({ type: "state_snapshot", state_id: name, version, value: structuredClone(value) });
	return {
		get: () => structuredClone(value),
		set(next: T) {
			value = structuredClone(next);
			publish({
				type: "state_patch",
				state_id: name,
				version: ++version,
				ops: [{ op: "replace", path: "", value: structuredClone(value) }],
			});
		},
	};
}
