import { readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import { type Api, InMemoryCredentialStore, type Model, Type } from "@earendil-works/pi-ai";
import { createModels } from "@earendil-works/pi-ai/models";
import {
	type FauxResponseFactory,
	fauxAssistantMessage,
	fauxProvider,
	fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { openaiProvider } from "@earendil-works/pi-ai/providers/openai";
import { OPENAI_MODELS } from "@earendil-works/pi-ai/providers/openai.models";
import { openaiCodexProvider } from "@earendil-works/pi-ai/providers/openai-codex";
import { OPENAI_CODEX_MODELS } from "@earendil-works/pi-ai/providers/openai-codex.models";
import { Value } from "typebox/value";
import { model as scriptedModel } from "./model.js";

export function exampleModel(env: NodeJS.ProcessEnv = process.env): Model<Api> {
	const provider = env.PI_PROVIDER ?? "faux";
	if (provider === "faux") return scriptedModel;
	const catalog = provider === "openai" ? OPENAI_MODELS : provider === "codex" ? OPENAI_CODEX_MODELS : undefined;
	if (!catalog) throw new Error("PI_PROVIDER must be faux, openai or codex");
	const selected = Object.values(catalog).find((model) => model.id === (env.PI_MODEL ?? "gpt-6-luna"));
	if (!selected) throw new Error("PI_MODEL is not in the installed Pi catalog");
	return selected;
}

export async function exampleModels() {
	const credentials = new InMemoryCredentialStore();
	if (process.env.PI_PROVIDER === "codex") {
		const schema = Type.Object({
			"openai-codex": Type.Object({
				type: Type.Literal("oauth"),
				access: Type.String(),
				refresh: Type.String(),
				expires: Type.Number(),
			}),
		});
		const auth = Value.Parse(
			schema,
			JSON.parse(await readFile(process.env.PI_AUTH_FILE ?? join(homedir(), ".pi/agent/auth.json"), "utf8")),
		);
		await credentials.modify("openai-codex", async () => auth["openai-codex"]);
	}
	if (process.env.PI_PROVIDER === "openai" && !process.env.OPENAI_API_KEY)
		throw new Error("Set OPENAI_API_KEY for PI_PROVIDER=openai");
	const models = createModels({ credentials });
	models.setProvider(openaiProvider());
	models.setProvider(openaiCodexProvider());
	const faux = fauxProvider({ models: [{ id: "demo" }] });
	const respond: FauxResponseFactory = (context) => {
		const start = context.messages.reduce((found, message, index) => (message.role === "user" ? index : found), -1);
		const results = context.messages.slice(start).filter((message) => message.role === "toolResult");
		if (results.some((result) => result.isError)) return fauxAssistantMessage("The tool was denied.");
		const prompt = context.messages[start];
		const add = JSON.stringify(prompt).includes("add 8");
		if (
			context.messages.some(
				(message) => message.role === "system" && message.toolsAdded?.some((tool) => tool.name === "codemode"),
			)
		) {
			if (!results.length)
				return fauxAssistantMessage(fauxToolCall("mcpReadInventory", {}), { stopReason: "toolUse" });
			if (results.length === 1)
				return fauxAssistantMessage(
					fauxToolCall("codemode", {
						code: "const rows = JSON.parse(await tools.read_inventory({})); text(rows.reduce((total, row) => total + row.price * row.quantity, 0));",
					}),
					{ stopReason: "toolUse" },
				);
			return fauxAssistantMessage("The inventory total is 460.");
		}
		if (!results.length)
			return fauxAssistantMessage(fauxToolCall("calculate", { operation: "multiply", a: 7, b: 6 }), {
				stopReason: "toolUse",
			});
		if (add && results.length === 1)
			return fauxAssistantMessage(fauxToolCall("calculate", { operation: "add", a: 42, b: 8 }), {
				stopReason: "toolUse",
			});
		return fauxAssistantMessage(add ? "The final answer is 50." : "The answer is 42.");
	};
	faux.setResponses(Array.from({ length: 1024 }, () => respond));
	models.setProvider(faux.provider);
	return models;
}
