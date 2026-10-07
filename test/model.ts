import type { AssistantMessage, Model } from "@earendil-works/pi-ai";

export const model: Model<"test"> = {
	id: "test",
	name: "Test model",
	api: "test",
	provider: "test",
	baseUrl: "http://localhost",
	reasoning: false,
	input: ["text"],
	contextWindow: 100_000,
	maxTokens: 1000,
	cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
};

export function answer(
	content: AssistantMessage["content"],
	stopReason: AssistantMessage["stopReason"] = "stop",
): AssistantMessage {
	return {
		role: "assistant",
		content,
		stopReason,
		api: model.api,
		provider: model.provider,
		model: model.id,
		timestamp: Date.now(),
		usage: {
			input: 1,
			output: 1,
			cacheRead: 0,
			cacheWrite: 0,
			totalTokens: 2,
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
		},
	};
}
