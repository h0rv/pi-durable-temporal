import { Type } from "@earendil-works/pi-ai";
import { createRegistry, defineExtension } from "@earendil-works/pi-durable";
import { createTemporalModels, runTemporalAgent, temporalTool } from "../src/workflow.js";
import { model } from "./model.js";

export async function demo(prompt: string) {
	const registry = createRegistry();
	registry.install(
		defineExtension({
			name: "calculator",
			tools: [
				temporalTool({
					name: "double",
					description: "Double a number",
					parameters: Type.Object({ value: Type.Number() }),
				}),
			],
		}),
	);
	return runTemporalAgent(
		{ type: "input", content: prompt },
		{
			models: createTemporalModels([model]),
			registry,
			agent: { model: { provider: model.provider, modelId: model.id } },
			settings: { compaction: { enabled: false } },
		},
	);
}
