import { Type } from "@earendil-works/pi-ai";
import { createRegistry, defineExtension } from "@earendil-works/pi-durable";
import { condition, defineQuery, defineSignal, setHandler } from "@temporalio/workflow";
import { createTemporalModels, runTemporalTurn, temporalTool } from "../../src/workflow.js";
import { model } from "../model.js";

export async function mcpAgent() {
	const registry = createRegistry();
	registry.install(
		defineExtension({
			name: "mcp",
			tools: [
				temporalTool({ name: "mcpReadInventory", description: "Read the inventory", parameters: Type.Object({}) }),
				temporalTool(
					{
						name: "codemode",
						description: "Run JavaScript using tools.read_inventory({})",
						parameters: Type.Object({ code: Type.String() }),
					},
					{ retry: { maximumAttempts: 3, initialInterval: "100 ms" } },
				),
			],
		}),
	);
	const result = await runTemporalTurn(
		{ type: "input", content: "Calculate the inventory total" },
		{
			models: createTemporalModels([model]),
			registry,
			agent: { model: { provider: model.provider, modelId: model.id } },
			settings: { compaction: { enabled: false } },
		},
	);
	let released = false;
	setHandler(defineQuery<boolean>("ready"), () => true);
	setHandler(defineSignal("release"), () => {
		released = true;
	});
	await condition(() => released);
	return result;
}
