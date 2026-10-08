import { createRegistry, defineExtension } from "@earendil-works/pi-durable";
import { createBashTool } from "@earendil-works/pi-durable/tools";
import { createTemporalModels, runTemporalAgent, temporalTool } from "../src/workflow.js";
import { model } from "./model.js";
import { reportTool } from "./tool-report-tools.js";

export async function reports(native: boolean, fail: boolean, stream = false, retries = 1) {
	const registry = createRegistry();
	registry.install(
		defineExtension({
			name: "reports",
			tools: [
				native
					? reportTool
					: temporalTool(
							reportTool,
							{ retry: { maximumAttempts: retries, initialInterval: "10 ms" } },
							{ stream },
						),
			],
		}),
	);
	return runTemporalAgent(
		{ type: "input", content: String(fail) },
		{
			models: createTemporalModels([model]),
			registry,
			agent: { model: { provider: model.provider, modelId: model.id } },
			settings: { compaction: { enabled: false } },
		},
	);
}

export async function shellCancellation() {
	const registry = createRegistry();
	registry.install(
		defineExtension({ name: "shell", tools: [temporalTool(createBashTool(), { heartbeatTimeout: "2 seconds" })] }),
	);
	return runTemporalAgent(
		{ type: "input", content: "Run shell" },
		{
			models: createTemporalModels([model]),
			registry,
			agent: { model: { provider: model.provider, modelId: model.id } },
			settings: { compaction: { enabled: false } },
		},
	);
}

export async function largeShell() {
	const registry = createRegistry();
	registry.install(defineExtension({ name: "shell", tools: [temporalTool(createBashTool())] }));
	return runTemporalAgent(
		{ type: "input", content: "Run shell" },
		{
			models: createTemporalModels([model]),
			registry,
			agent: { model: { provider: model.provider, modelId: model.id } },
			settings: { compaction: { enabled: false } },
		},
	);
}
