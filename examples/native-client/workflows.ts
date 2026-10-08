import type { Context } from "@earendil-works/chord";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import type { Api, Model } from "@earendil-works/pi-ai";
import {
	type AgentChange,
	type ConversationView,
	createRegistry,
	defineExtension,
	type HarnessSettings,
} from "@earendil-works/pi-durable";
import { CodingTools } from "@earendil-works/pi-durable/tools";
import {
	allHandlersFinished,
	CancellationScope,
	condition,
	defineQuery,
	defineSignal,
	defineUpdate,
	setHandler,
} from "@temporalio/workflow";
import type { AgentController } from "../../.local/pi-upstream/packages/coding-agent/src/experimental/services/agent-controller.ts";
import { createAgentController } from "../../.local/pi-upstream/packages/coding-agent/src/experimental/services/agent-controller-provider.ts";
import { createTemporalModels, openTemporalHarness, temporalTool } from "../../src/workflow.js";

type Arguments<T> = T extends (...args: [...infer Args, Context]) => unknown ? Args : never;

const update = <K extends keyof AgentController>(method: K) =>
	defineUpdate<Awaited<ReturnType<AgentController[K]>>, Arguments<AgentController[K]>>(`piController.${method}`);

export const controllerUpdates = {
	prompt: update("prompt"),
	steer: update("steer"),
	followUp: update("followUp"),
	cancelQueued: update("cancelQueued"),
	abort: update("abort"),
	compact: update("compact"),
	waitForPrompt: update("waitForPrompt"),
};
export const transcriptQuery = defineQuery<ConversationView>("piTranscript");
export const configureUpdate = defineUpdate<void, [AgentChange]>("piConfigure");
export const closeSession = defineSignal("piCloseSession");

export async function piNativeSession(input: {
	model: Model<Api>;
	cwd: string;
	agent?: AgentChange;
	settings?: HarnessSettings;
}) {
	const coding = createRegistry();
	coding.install(CodingTools);
	const registry = createRegistry();
	registry.install(
		defineExtension({
			name: CodingTools.name,
			tools: coding
				.snapshot()
				.tools()
				.map(({ tool }) => temporalTool(tool, { heartbeatTimeout: "10 seconds" }, { stream: true })),
		}),
	);
	const harness = await openTemporalHarness({
		models: createTemporalModels([input.model], undefined, { stream: true }),
		registry,
		settings: input.settings,
	});
	const conversation = await harness.root(BACKGROUND_CONTEXT, {
		agent: {
			cwd: input.cwd,
			model: { provider: input.model.provider, modelId: input.model.id },
			...input.agent,
		},
	});
	const controller = createAgentController(harness, conversation);
	const transcript = await conversation.viewState(BACKGROUND_CONTEXT);
	let closed = false;
	const admitting = {
		validator(..._args: unknown[]) {
			if (closed) throw new Error("Session is closing");
		},
	};
	setHandler(transcriptQuery, () => transcript.value);
	setHandler(closeSession, () => {
		closed = true;
	});
	setHandler(configureUpdate, (change) => conversation.configure(change, BACKGROUND_CONTEXT), admitting);
	setHandler(controllerUpdates.prompt, (request) => controller.prompt(request, BACKGROUND_CONTEXT), admitting);
	setHandler(controllerUpdates.steer, (request) => controller.steer(request, BACKGROUND_CONTEXT), admitting);
	setHandler(controllerUpdates.followUp, (request) => controller.followUp(request, BACKGROUND_CONTEXT), admitting);
	setHandler(controllerUpdates.cancelQueued, (entryId) => controller.cancelQueued(entryId, BACKGROUND_CONTEXT));
	setHandler(controllerUpdates.abort, () => controller.abort(BACKGROUND_CONTEXT));
	setHandler(controllerUpdates.compact, (request) => controller.compact(request, BACKGROUND_CONTEXT), admitting);
	setHandler(controllerUpdates.waitForPrompt, (operationId) =>
		controller.waitForPrompt(operationId, BACKGROUND_CONTEXT),
	);
	try {
		await condition(() => closed);
	} finally {
		await CancellationScope.nonCancellable(async () => {
			await controller.abort(BACKGROUND_CONTEXT);
			await condition(allHandlersFinished);
			transcript.dispose();
			await harness.close(BACKGROUND_CONTEXT);
		});
	}
}
