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
	options: Omit<
		SimpleStreamOptions,
		"signal" | "apiKey" | "fetch" | "telemetryContext" | "onPayload" | "onResponse" | "onProviderStreamEvent"
	>;
};

export type ToolCallInfo = Pick<ToolExecutionApi, "callId" | "taskId" | "conversationId"> & {
	progress?: ProgressTarget;
	outputWindow?: ToolExecutionApi["outputWindow"];
};
export type ToolProgress = {
	requestId: string;
	attempt: number;
	output?: string;
	reports?: import("./tool-reports.js").ToolReport[];
	offset?: number;
};

export type ProgressTarget = { workflowId: string; runId: string; requestId: string };
export type ModelProgress = { requestId: string; attempt: number; events: AssistantMessageEvent[] };
export type DeferredRequest = {
	model: ModelRequest["model"];
	handle: DeferredHandle;
	options?: ModelRequest["options"];
	wait?: number;
	progress?: ProgressTarget;
};
