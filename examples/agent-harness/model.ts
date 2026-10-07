import type { Model } from "@earendil-works/pi-ai";

// Catalog metadata only. The worker resolves the actual provider and credentials.
export const model: Model<"openai-codex-responses"> = {
	id: "gpt-6-luna",
	name: "GPT-6 Luna",
	api: "openai-codex-responses",
	provider: "openai-codex",
	baseUrl: "https://chatgpt.com/backend-api",
	reasoning: true,
	input: ["text"],
	contextWindow: 272_000,
	maxTokens: 1024,
	cost: { input: 0.25, output: 2, cacheRead: 0.025, cacheWrite: 0.25 },
};
