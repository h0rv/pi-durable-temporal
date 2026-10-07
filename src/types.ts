import type {
	Api,
	AssistantMessageEvent,
	Context,
	DeferredHandle,
	Model,
	SimpleStreamOptions,
} from "@earendil-works/pi-ai";
import type { ToolExecutionApi } from "@earendil-works/pi-durable";

export type ModelRequest = {
	model: Pick<Model<Api>, "provider" | "id">;
	context: Context;
	progress?: ProgressTarget;
	options: Pick<
		SimpleStreamOptions,
		| "temperature"
		| "maxTokens"
		| "reasoning"
		| "thinkingBudgets"
		| "toolChoice"
		| "sessionId"
		| "cacheRetention"
		| "metadata"
		| "timeoutMs"
		| "maxRetries"
		| "maxRetryDelayMs"
		| "transport"
		| "deferred"
		| "websocketConnectTimeoutMs"
	>;
};

export type ToolCallInfo = Pick<ToolExecutionApi, "callId" | "taskId" | "conversationId"> & {
	progress?: ProgressTarget;
};
export type ToolProgress = { requestId: string; attempt: number; output: string };

export type ProgressTarget = { workflowId: string; runId: string; requestId: string };
export type ModelProgress = { requestId: string; attempt: number; events: AssistantMessageEvent[] };
export type DeferredRequest = {
	model: ModelRequest["model"];
	handle: DeferredHandle;
	wait?: number;
	progress?: ProgressTarget;
};
