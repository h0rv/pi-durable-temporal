import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { Type } from "@earendil-works/pi-ai";
import { createRegistry, defineExtension } from "@earendil-works/pi-durable";
import { CancellationScope, condition, defineQuery, defineSignal, setHandler } from "@temporalio/workflow";
import type { ModelProgress, ToolProgress } from "../src/types.js";
import { createTemporalModels, openTemporalHarness, runTemporalAgent, temporalTool } from "../src/workflow.js";
import { model } from "./model.js";

export const proceed = defineSignal("proceed");
export const paused = defineQuery<boolean>("paused");

export async function agent(input: { prompt: string; pause?: boolean; safe?: boolean }) {
	const registry = createRegistry();
	registry.install(
		defineExtension({
			name: "tools",
			tools: [
				temporalTool(
					{
						name: "double",
						description: "Double a number",
						parameters: Type.Object({ value: Type.Number() }),
					},
					input.safe
						? { startToCloseTimeout: "2 seconds", retry: { maximumAttempts: 3, initialInterval: "10 ms" } }
						: { startToCloseTimeout: "10 seconds" },
				),
			],
		}),
	);
	const harness = await openTemporalHarness({
		models: createTemporalModels([model], {
			startToCloseTimeout: "2 seconds",
			retry: { maximumAttempts: 3, initialInterval: "10 ms" },
		}),
		registry,
		settings: { retry: { enabled: false }, compaction: { enabled: false } },
	});
	try {
		const root = await harness.root(BACKGROUND_CONTEXT, {
			agent: { model: { provider: model.provider, modelId: model.id } },
		});
		const submission = await root.submit({ type: "input", content: input.prompt }, BACKGROUND_CONTEXT);
		const settled = await submission.wait(BACKGROUND_CONTEXT);
		if (CancellationScope.current().consideredCancelled) await CancellationScope.current().cancelRequested;
		if (input.pause) {
			let ready = false;
			setHandler(paused, () => true);
			setHandler(proceed, () => {
				ready = true;
			});
			await condition(() => ready);
		}
		return {
			settled,
			context: await root.context(BACKGROUND_CONTEXT),
			usage: await harness.usage(BACKGROUND_CONTEXT),
		};
	} finally {
		await harness.close(BACKGROUND_CONTEXT);
	}
}

export async function oneShot(prompt = "Say hello") {
	return runTemporalAgent(
		{ type: "input", content: prompt },
		{
			registry: createRegistry(),
			models: createTemporalModels([model], {
				startToCloseTimeout: "10 seconds",
				retry: { maximumAttempts: 3, initialInterval: "10 ms" },
			}),
			agent: { model: { provider: model.provider, modelId: model.id } },
			settings: { compaction: { enabled: false } },
		},
	);
}

export async function compatibility() {
	const bytes = crypto.getRandomValues(new Uint8Array(16));
	const reference = new WeakRef({ value: 42 });
	const before = performance.now();
	clearTimeout(undefined);
	await new Promise<void>((resolve) => queueMicrotask(resolve));
	const clone = structuredClone({ bytes, nested: { value: reference.deref()?.value } });
	return { bytes: Array.from(clone.bytes), value: clone.nested.value, clock: performance.now() >= before };
}

export async function modelTransport() {
	const progress: ModelProgress[] = [];
	const models = createTemporalModels([model], undefined, {
		stream: true,
		onProgress: (event) => progress.push(event),
	});
	const selected = models.getModel(model.provider, model.id);
	if (!selected) throw new Error("Missing test model");
	const result = await models
		.streamSimple(selected, { messages: [{ role: "user", content: "Hello", timestamp: 0 }] })
		.result();
	return { result, progress };
}

export async function deferredTransport() {
	const models = createTemporalModels([model]);
	const selected = models.getModel(model.provider, model.id);
	if (!selected) throw new Error("Missing test model");
	const pending = await models.streamSimple(selected, { messages: [] }, { deferred: true }).result();
	if (!pending.deferred) throw new Error("Missing deferred handle");
	const result = await models.fetchDeferred(selected, pending.deferred, { wait: 1000 });
	await models.cancelDeferred(selected, pending.deferred);
	return result;
}

export { failedModel, nativeHooks, nativeSession } from "./native-workflows.js";

export async function toolTransport() {
	const progress: ToolProgress[] = [];
	const registry = createRegistry();
	const tool = {
		name: "progress",
		description: "Report progress",
		parameters: Type.Object({}),
		outputLimits: { maxBytes: 10, retain: "tail" as const },
	};
	registry.install(
		defineExtension({
			name: "progress",
			tools: [
				temporalTool(
					tool,
					{ retry: { maximumAttempts: 2, initialInterval: "10 ms" } },
					{ stream: true, onProgress: (value) => progress.push(value) },
				),
			],
		}),
	);
	const result = await runTemporalAgent(
		{ type: "input", content: "Report progress" },
		{
			models: createTemporalModels([model]),
			registry,
			agent: { model: { provider: model.provider, modelId: model.id } },
			settings: { compaction: { enabled: false } },
		},
	);
	return { result, progress };
}
