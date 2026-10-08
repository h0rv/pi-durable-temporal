import type { Context } from "@earendil-works/chord";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import type { Api, Model } from "@earendil-works/pi-ai";
import {
	type AgentChange,
	type ConversationView,
	createRegistry,
	defineExtension,
	type HarnessSettings,
	type InboxState,
	type LiveState,
} from "@earendil-works/pi-durable";
import { CodingTools } from "@earendil-works/pi-durable/tools";
import { ApplicationFailure } from "@temporalio/common";
import {
	allHandlersFinished,
	CancellationScope,
	condition,
	continueAsNew,
	defineQuery,
	defineSignal,
	defineUpdate,
	patched,
	setHandler,
	workflowInfo,
} from "@temporalio/workflow";
import type { AgentController } from "../../.local/pi-upstream/packages/coding-agent/src/experimental/services/agent-controller.ts";
import { createAgentController } from "../../.local/pi-upstream/packages/coding-agent/src/experimental/services/agent-controller-provider.ts";
import type { PiCheckpoint } from "../../src/state.js";
import { createTemporalModels, openTemporalSession, temporalTool } from "../../src/workflow.js";
import { type NativeTraceState, nativeTrace } from "./trace.js";

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
export const continueSession = defineSignal("piContinueSession");

export async function piNativeSession(
	input: {
		model: Model<Api>;
		cwd: string;
		agent?: AgentChange;
		settings?: HarnessSettings;
		maxTurnsPerRun?: number;
		retainTraceTurns?: number;
	},
	prior?: { pi: PiCheckpoint; trace: NativeTraceState },
) {
	const automaticRollover = patched("native-session-rollover-v1");
	const maxTurns = input.maxTurnsPerRun ?? 20;
	const retainTurns = input.retainTraceTurns ?? 20;
	if (!Number.isSafeInteger(maxTurns) || maxTurns < 1 || !Number.isSafeInteger(retainTurns) || retainTurns < 0)
		throw ApplicationFailure.nonRetryable("Invalid session retention settings");
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
	const trace = nativeTrace(registry, input.model.id, automaticRollover ? retainTurns : Infinity, prior?.trace);
	const firstTurn = trace.turn;
	const session = await openTemporalSession(
		{
			models: createTemporalModels([input.model], undefined, { stream: true }),
			registry,
			settings: input.settings,
		},
		prior?.pi,
	);
	const harness = session.harness;
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
	let rotating = false;
	let requested = false;
	let next: { pi: PiCheckpoint; trace: NativeTraceState } | undefined;
	const unsubscribeTrace = trace.attach(transcript, () => closed);
	const admitting = {
		validator(..._args: unknown[]) {
			if (closed) throw new Error("Session is closing");
			if (rotating) throw ApplicationFailure.nonRetryable("Session is continuing as new", "SessionRollingOver");
		},
	};
	setHandler(transcriptQuery, () => transcript.value);
	setHandler(closeSession, () => {
		closed = true;
	});
	setHandler(continueSession, () => {
		requested = true;
	});
	setHandler(configureUpdate, (change) => conversation.configure(change, BACKGROUND_CONTEXT), admitting);
	setHandler(controllerUpdates.prompt, (request) => controller.prompt(request, BACKGROUND_CONTEXT), admitting);
	setHandler(controllerUpdates.steer, (request) => controller.steer(request, BACKGROUND_CONTEXT), admitting);
	setHandler(controllerUpdates.followUp, (request) => controller.followUp(request, BACKGROUND_CONTEXT), admitting);
	setHandler(
		controllerUpdates.cancelQueued,
		(entryId) => controller.cancelQueued(entryId, BACKGROUND_CONTEXT),
		admitting,
	);
	setHandler(controllerUpdates.abort, () => controller.abort(BACKGROUND_CONTEXT), admitting);
	setHandler(controllerUpdates.compact, (request) => controller.compact(request, BACKGROUND_CONTEXT), admitting);
	setHandler(
		controllerUpdates.waitForPrompt,
		(operationId) => controller.waitForPrompt(operationId, BACKGROUND_CONTEXT),
		admitting,
	);
	try {
		await condition(() => {
			if (closed) return true;
			const live = transcript.value.docs["pi.live"] as LiveState;
			const inbox = transcript.value.docs["pi.inbox"] as InboxState;
			return (
				(requested ||
					(automaticRollover && (trace.turn - firstTurn >= maxTurns || workflowInfo().continueAsNewSuggested))) &&
				!live.run &&
				!live.compactions?.length &&
				!inbox.items.length
			);
		});
		if (!closed) {
			rotating = true;
			trace.close();
			await condition(allHandlersFinished);
			const pi = await session.checkpoint();
			if (!closed) next = { pi, trace: trace.checkpoint(retainTurns) };
		}
	} finally {
		await CancellationScope.nonCancellable(async () => {
			await controller.abort(BACKGROUND_CONTEXT);
			trace.close();
			await condition(allHandlersFinished);
			unsubscribeTrace();
			transcript.dispose();
			await harness.close(BACKGROUND_CONTEXT);
		});
	}
	if (next && !closed) await continueAsNew<typeof piNativeSession>(input, next);
}
