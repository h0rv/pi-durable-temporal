import type { Protocol, SubmitMessageResponse } from "@temporalio/agent-harness-client";
import { ApplicationFailure } from "@temporalio/common";
import {
	condition,
	defineSignal,
	executeChild,
	getExternalWorkflowHandle,
	patched,
	setHandler,
	uuid4,
	workflowInfo,
} from "@temporalio/workflow";
import { WorkflowStream, type WorkflowStreamState } from "@temporalio/workflow-streams/workflow";
import { approvalCallKey } from "../../src/agent-harness.js";
import type { PiCheckpoint } from "../../src/state.js";
import {
	type ApprovalDecision,
	approvalResolved,
	approveTool,
	ask,
	close,
	type PendingApproval,
	type SessionOptions,
	status,
	traceSnapshot,
} from "./protocol.js";
import { piAgent } from "./workflows.js";

export { piAgent } from "./workflows.js";

const trace = defineSignal<[Protocol.AgentStreamItem]>("pi_trace");
type Turn = SubmitMessageResponse & { text: string };
function sessionStatus(
	turn: number,
	active: Turn | undefined,
	pending: Turn[],
	closed: boolean,
	approvals: PendingApproval[],
	options: SessionOptions,
	allowedTools: string[],
) {
	return {
		agent_id: "pi",
		current_turn: turn,
		turn_active: Boolean(active),
		turn_participants: active ? 1 : 0,
		pending_turns: pending.map((turn) => ({
			turn_number: turn.turn_number,
			turn_id: turn.turn_id,
			message_id: turn.message_id,
			message: turn.text,
		})),
		pending_approvals: approvals,
		pending_callbacks: [],
		subagents: [],
		closed,
		has_auto_approval_evaluator: options.approvalMode === "auto",
		approval_policy: {
			dangerously_skip_all_approvals: false,
			auto_approve_inherently_safe: false,
			auto_approve_tools: allowedTools,
			auto_mode_enabled: options.approvalMode === "auto",
		},
	};
}

type SessionCheckpoint = {
	piState?: PiCheckpoint;
	pending: Turn[];
	admitted: number;
	current: number;
	allowed: string[];
	allowedCalls?: string[];
	resolved: string[];
	plan: { phase: string; completedTools: number; pendingApprovals: number; turn: number };
	planVersion: number;
	stream: WorkflowStreamState;
};
export async function piSession(options: SessionOptions = {}, prior?: SessionCheckpoint) {
	const persistent = patched("persistent-session-v1");
	const scopedTurns = patched("turn-approval-cleanup-v1");
	let scopedApprovals = patched("remembered-call-approvals-v1");
	const stream = new WorkflowStream(prior?.stream);
	setHandler(traceSnapshot, () => stream.getState());
	const events = stream.topic<Protocol.AgentEvent>("turn_events");
	const pending: Turn[] = prior?.pending ?? [];
	let piState = prior?.piState;
	let runTurns = 0;
	let active: Turn | undefined;
	let admitted = prior?.admitted ?? 0;
	let current = prior?.current ?? 0;
	let closed = false;
	const approvals = new Map<string, PendingApproval>();
	const resolved = new Set<string>(prior?.resolved);
	const allowed = new Set<string>(
		prior?.allowed ??
			(options.task === "mcp"
				? ["mcpReadInventory", "codemode"]
				: options.task === "coding"
					? ["readCandidate", "writeCandidate", "runTests"]
					: []),
	);
	const allowedCalls = new Set<string>(prior?.allowedCalls);
	let planVersion = prior?.planVersion ?? -1;
	const plan = prior?.plan ?? { phase: "idle", completedTools: 0, pendingApprovals: 0, turn: 0 };
	const publish = (turn: Turn, event: Protocol.AgentStreamItem) =>
		events.publish({
			agent_id: "pi",
			turn_id: turn.turn_id,
			turn_number: turn.turn_number,
			message_id: event.type === "turn_started" || event.type === "turn_end" ? null : turn.message_id,
			timestamp: Date.now() / 1000,
			event,
		});
	const publishPlan = () => {
		if (!active) return;
		plan.pendingApprovals = approvals.size;
		plan.turn = current;
		if (planVersion < 0)
			publish(active, { type: "state_snapshot", state_id: "plan", version: ++planVersion, value: { ...plan } });
		else
			publish(active, {
				type: "state_patch",
				state_id: "plan",
				version: ++planVersion,
				ops: [{ op: "replace", path: "", value: { ...plan } }],
			});
	};
	setHandler(status, () =>
		sessionStatus(current, active, pending, closed, [...approvals.values()], options, [...allowed]),
	);
	const resolveApproval = async (decision: ApprovalDecision) => {
		const request = approvals.get(decision.tool_id);
		if (resolved.has(decision.tool_id))
			throw ApplicationFailure.nonRetryable("This approval already has a decision", "ToolApprovalAlreadyResolved");
		if (!request || !active) throw ApplicationFailure.nonRetryable("Unknown pending approval", "UnknownApproval");
		if (scopedTurns && request.turn_number !== active.turn_number)
			throw ApplicationFailure.nonRetryable("Unknown pending approval", "UnknownApproval");
		if (decision.remember && !decision.approved)
			throw ApplicationFailure.nonRetryable("Only approvals can be remembered", "InvalidApproval");
		resolved.add(decision.tool_id);
		try {
			await getExternalWorkflowHandle(
				`${workflowInfo().workflowId}/turn-${scopedTurns ? request.turn_number : active.turn_number}`,
			).signal(approvalResolved, decision);
		} catch (error) {
			resolved.delete(decision.tool_id);
			throw error;
		}
		approvals.delete(decision.tool_id);
		plan.phase = approvals.size ? "awaitingApproval" : "working";
		publishPlan();
		if (decision.approved && decision.remember) {
			if (decision.rememberScope === "call") {
				scopedApprovals = patched("remembered-call-approvals-v1");
				allowedCalls.add(approvalCallKey(request.tool_name, request.tool_input));
			} else allowed.add(request.tool_name);
		}
		return { tool_id: decision.tool_id, accepted: true as const };
	};
	setHandler(approveTool, resolveApproval);
	setHandler(close, async () => {
		closed = true;
		pending.length = 0;
		for (const request of [...approvals.values()])
			if (!resolved.has(request.tool_id))
				await resolveApproval({ tool_id: request.tool_id, approved: false, reason: "Session closed" });
	});
	setHandler(trace, async (event) => {
		if (!active) return;
		publish(active, event);
		if (event.type === "tool_end") {
			plan.completedTools++;
			publishPlan();
		}
		if (event.type === "tool_approval_requested") {
			approvals.set(event.tool_id, {
				...event,
				turn_number: active.turn_number,
				short_id: String(approvals.size + 1),
			});
			plan.phase = "awaitingApproval";
			publishPlan();
			if (closed) await resolveApproval({ tool_id: event.tool_id, approved: false, reason: "Session closed" });
		}
	});
	setHandler(ask, ({ text }) => {
		if (closed || (!persistent && admitted >= 20))
			throw ApplicationFailure.nonRetryable("Create a new session after 20 turns", "SessionLimit");
		if (!text.trim() || text.length > 32_000)
			throw ApplicationFailure.nonRetryable("Text must contain 1 to 32000 characters", "InvalidText");
		const turn: Turn = {
			text,
			turn_number: ++admitted,
			turn_id: uuid4(),
			message_id: uuid4(),
			accepted_offset: stream.getState().base_offset + stream.getState().log.length,
			disposition: active || pending.length ? "queued" : "opened",
		};
		publish(turn, { type: "message_accepted", handler: "ask", payload: { text }, disposition: turn.disposition });
		pending.push(turn);
		const { text: _text, ...receipt } = turn;
		return receipt;
	});
	while (true) {
		await condition(() => pending.length > 0 || closed);
		active = pending.shift();
		if (!active) break;
		current = active.turn_number;
		publish(active, { type: "turn_started" });
		plan.phase = "working";
		publishPlan();
		publish(active, { type: "message_handler_start" });
		try {
			const result = await executeChild(piAgent, {
				workflowId: `${workflowInfo().workflowId}/turn-${current}`,
				args: [
					{
						prompt: active.text,
						parentWorkflowId: workflowInfo().workflowId,
						options,
						allowedTools: [...allowed],
						...(scopedApprovals ? { allowedCalls: [...allowedCalls] } : {}),
						state: piState,
					},
				],
			});
			if (result.state) piState = result.state;
			publish(active, { type: "message_handler_end", output: { text: result.text } });
			plan.phase = "complete";
			publishPlan();
		} catch (error) {
			if (scopedTurns)
				for (const request of approvals.values()) {
					if (request.turn_number !== current) continue;
					approvals.delete(request.tool_id);
					publish(active, {
						type: "tool_approval_resolved",
						tool_id: request.tool_id,
						tool_name: request.tool_name,
						approved: false,
						reason: "Turn failed",
						remember: false,
					});
				}
			plan.phase = "failed";
			publishPlan();
			publish(active, {
				type: "message_handler_error",
				message: error instanceof Error ? error.message : String(error),
			});
		}
		publish(active, { type: "turn_end" });
		active = undefined;
		runTurns++;
		if (
			persistent &&
			!closed &&
			(runTurns >= (options.maxTurnsPerRun ?? 20) || workflowInfo().continueAsNewSuggested)
		) {
			await stream.continueAsNew<typeof piSession>((stream) => [
				options,
				{
					piState,
					pending,
					admitted,
					current,
					allowed: [...allowed],
					...(scopedApprovals ? { allowedCalls: [...allowedCalls] } : {}),
					resolved: [...resolved],
					plan,
					planVersion,
					stream,
				},
			]);
		}
	}
	stream.detachPollers();
}
