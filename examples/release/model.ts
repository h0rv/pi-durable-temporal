import type { Model } from "@earendil-works/pi-ai";

export type AgentRole = "author" | "tests" | "security";
export const roles: AgentRole[] = ["author", "tests", "security"];
export const models: Model<"faux">[] = roles.map((role) => ({
	id: role,
	name: role,
	api: "faux",
	provider: "faux",
	baseUrl: "http://localhost",
	reasoning: false,
	input: ["text"],
	contextWindow: 100_000,
	maxTokens: 1000,
	cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
}));
