import type { AttachedReplicatedState } from "@earendil-works/chord";
import type { ConversationView, InboxState, LiveState, Registry } from "@earendil-works/pi-durable";
import type { Protocol } from "@temporalio/agent-harness-client";
import { setHandler } from "@temporalio/workflow";
import { WorkflowStream } from "@temporalio/workflow-streams/workflow";
import { createAgentTrace } from "../../src/agent-harness.js";
import { status, traceSnapshot } from "../agent-harness/protocol.js";

export function nativeTrace(registry: Registry, model: string) {
	const stream = new WorkflowStream();
	const events = stream.topic<Protocol.AgentEvent>("turn_events");
	let turn = 0;
	let taskId: number | undefined;
	let messageId: string | null = null;
	const messages = new Set<string>();
	let stateVersion = -1;
	let previousState = "";
	const seen = new Set<number>();
	const publish = (event: Protocol.AgentStreamItem) => {
		if (taskId === undefined) return;
		events.publish({
			agent_id: "pi",
			turn_id: String(taskId),
			turn_number: turn,
			message_id: event.type === "turn_started" || event.type === "turn_end" ? null : messageId,
			timestamp: Date.now() / 1000,
			event,
		});
	};
	registry.install(
		createAgentTrace({
			model,
			allowedTools: registry
				.snapshot()
				.tools()
				.map(({ tool }) => tool.name),
			publish: async (event) => publish(event),
		}).extension,
	);
	setHandler(traceSnapshot, () => stream.getState());
	return {
		attach(view: AttachedReplicatedState<ConversationView>, closed: () => boolean) {
			for (const entry of view.value.entries) seen.add(entry.id);
			setHandler(status, () => ({
				agent_id: "pi",
				current_turn: turn,
				turn_active: taskId !== undefined,
				turn_participants: messages.size,
				pending_turns: (view.value.docs["pi.inbox"] as InboxState).items,
				pending_approvals: [],
				pending_callbacks: [],
				subagents: [],
				closed: closed(),
				has_auto_approval_evaluator: false,
				approval_policy: {
					dangerously_skip_all_approvals: true,
					auto_approve_inherently_safe: false,
					auto_approve_tools: [],
					auto_mode_enabled: false,
				},
			}));
			return view.subscribe((next) => {
				const live = next.docs["pi.live"] as LiveState;
				if (live.run && taskId === undefined) {
					taskId = live.run.taskId;
					turn++;
					publish({ type: "turn_started" });
				}
				for (const entry of next.entries) {
					if (seen.has(entry.id)) continue;
					seen.add(entry.id);
					if (entry.kind !== "pi.user") continue;
					for (const message of entry.model ?? []) {
						if (message.role !== "user") continue;
						const joined = messageId !== null;
						messageId = String(entry.id);
						messages.add(messageId);
						publish({
							type: "message_accepted",
							handler: "ask",
							payload: { text: message.content },
							disposition: joined ? "joined" : "opened",
						});
						publish({ type: "message_handler_start" });
					}
				}
				const usage = next.docs["pi.usage"];
				const state = JSON.stringify(usage);
				if (taskId !== undefined && state !== previousState) {
					previousState = state;
					publish(
						stateVersion < 0
							? { type: "state_snapshot", state_id: "pi.usage", version: ++stateVersion, value: usage }
							: {
									type: "state_patch",
									state_id: "pi.usage",
									version: ++stateVersion,
									ops: [{ op: "replace", path: "", value: usage }],
								},
					);
				}
				if (!live.run && taskId !== undefined) {
					const answer = next.entries
						.filter((entry) => entry.id > Number(messages.values().next().value))
						.flatMap((entry) => entry.model ?? [])
						.findLast((m) => m.role === "assistant");
					for (const id of messages) {
						messageId = id;
						if (
							answer?.role === "assistant" &&
							answer.stopReason !== "toolUse" &&
							answer.stopReason !== "aborted" &&
							answer.stopReason !== "error"
						)
							publish({
								type: "message_handler_end",
								output: {
									text: answer.content
										.filter((b) => b.type === "text")
										.map((b) => b.text)
										.join(""),
								},
							});
						else publish({ type: "message_handler_error", message: "Run ended before a final response" });
					}
					publish({ type: "turn_end" });
					taskId = undefined;
					messageId = null;
					messages.clear();
				}
			});
		},
		close: () => stream.detachPollers(),
	};
}
