import { Type } from "@earendil-works/pi-ai";
import { createRegistry, defineExtension } from "@earendil-works/pi-durable";
import type { Protocol } from "@temporalio/agent-harness-client";
import { createAgentTrace } from "../src/agent-harness.js";
import { createTemporalModels, runTemporalAgent, temporalTool } from "../src/workflow.js";
import { model } from "./model.js";

export async function traceOnly() {
	const events: Protocol.AgentStreamItem[] = [];
	const trace = createAgentTrace({
		model: model.id,
		publish: async (event) => {
			events.push(event);
		},
	});
	const registry = createRegistry();
	registry.install(trace.extension);
	registry.install(
		defineExtension({
			name: "tools",
			tools: [
				temporalTool({
					name: "double",
					description: "Double a number",
					parameters: Type.Object({ value: Type.Number() }),
				}),
			],
		}),
	);
	const result = await runTemporalAgent(
		{ type: "input", content: "Double 6" },
		{
			models: createTemporalModels([model]),
			registry,
			agent: { model: { provider: model.provider, modelId: model.id } },
		},
	);
	return { result, events };
}
