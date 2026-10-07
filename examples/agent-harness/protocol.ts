import type { Api, Model } from "@earendil-works/pi-ai";
import type {
	AgentStatusSnapshot,
	Protocol,
	SubmitMessageResponse,
	ToolApprovalDecision,
} from "@temporalio/agent-harness-client";
import { defineQuery, defineSignal, defineUpdate } from "@temporalio/workflow";
import type { WorkflowStreamState } from "@temporalio/workflow-streams/workflow";

export const ask = defineUpdate<SubmitMessageResponse, [{ text: string }]>("ask");
export const status = defineQuery<AgentStatusSnapshot & { pending_approvals: PendingApproval[] }>("status");
export const close = defineSignal("close");

export type ApprovalRequest = Extract<Protocol.AgentStreamItem, { type: "tool_approval_requested" }>;
export type ApprovalDecision = ToolApprovalDecision & { tool_id: string };
export type PendingApproval = ApprovalRequest & { turn_number: number; short_id: string };
export type SessionOptions = {
	model?: Model<Api>;
	task?: "calculator" | "coding" | "mcp" | "workspace";
	maxTurnsPerRun?: number;
	approvalMode?: "manual" | "auto";
};
export const approveTool = defineUpdate<{ tool_id: string; accepted: true }, [ApprovalDecision]>("approve_tool");
export const approvalResolved = defineSignal<[ApprovalDecision]>("approval_resolved");

export const traceSnapshot = defineQuery<WorkflowStreamState>("trace_snapshot");
