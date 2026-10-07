import "./workflow-globals.js";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import type {
	Api,
	AssistantMessage,
	DeferredFetchOptions,
	Model,
	Models,
	SimpleStreamOptions,
	Static,
	Tool,
	TSchema,
} from "@earendil-works/pi-ai";
import { createModels, createProvider } from "@earendil-works/pi-ai/models";
import { AssistantMessageEventStream } from "@earendil-works/pi-ai/utils/event-stream";
import {
	type AgentChange,
	Harness,
	type HarnessOptions,
	type InputSubmissionDraft,
	MemoryStorage,
	type Storage,
	type ToolExecutionResult,
	type ToolRegistration,
} from "@earendil-works/pi-durable";
import { ActivityFailure, ApplicationFailure } from "@temporalio/common";
import {
	type ActivityOptions,
	CancellationScope,
	defineSignal,
	patched,
	proxyActivities,
	setHandler,
	uuid4,
	workflowInfo,
} from "@temporalio/workflow";
import type { createModelActivities } from "./index.js";
import { type PiCheckpoint, TemporalStorage } from "./state.js";
import type { ModelProgress, ModelRequest, ProgressTarget, ToolCallInfo, ToolProgress } from "./types.js";

export { type PiCheckpoint, TemporalStorage } from "./state.js";

const DEFAULT_ACTIVITY_OPTIONS: ActivityOptions = {
	startToCloseTimeout: "5 minutes",
	retry: { maximumAttempts: 3 },
};

async function withSignal<T>(signal: AbortSignal | undefined, run: () => Promise<T>): Promise<T> {
	const scope = new CancellationScope();
	const cancel = () => scope.cancel();
	if (signal?.aborted) scope.cancel();
	else signal?.addEventListener("abort", cancel, { once: true });
	try {
		return await scope.run(run);
	} finally {
		signal?.removeEventListener("abort", cancel);
	}
}

export type ModelTransportOptions = { stream?: boolean; onProgress?: (progress: ModelProgress) => void };

/** Run Pi model requests as Temporal activities. */
export function createTemporalModels(
	catalog: readonly Model<Api>[],
	options?: ActivityOptions,
	transport: ModelTransportOptions = {},
): Models {
	const activities = proxyActivities<ReturnType<typeof createModelActivities>>({
		...DEFAULT_ACTIVITY_OPTIONS,
		...options,
		retry: { ...DEFAULT_ACTIVITY_OPTIONS.retry, ...options?.retry },
	});
	const models = createModels({ authContext: { env: async () => undefined, fileExists: async () => false } });
	const pending = new Map<string, { stream: AssistantMessageEventStream; attempt: number }>();
	if (transport.stream)
		setHandler(defineSignal<[ModelProgress]>("pi_model_progress"), (progress) => {
			const request = pending.get(progress.requestId);
			if (!request || progress.attempt < request.attempt) return;
			request.attempt = progress.attempt;
			for (const event of progress.events)
				if (event.type !== "done" && event.type !== "error") request.stream.push(event);
			transport.onProgress?.(progress);
		});
	const dispatch = (
		signal: AbortSignal | undefined,
		run: (target?: ProgressTarget) => Promise<Awaited<ReturnType<typeof activities.piModel>>>,
	) => {
		const events = new AssistantMessageEventStream();
		const target = transport.stream
			? { workflowId: workflowInfo().workflowId, runId: workflowInfo().runId, requestId: uuid4() }
			: undefined;
		if (target) pending.set(target.requestId, { stream: events, attempt: 0 });
		const result = withSignal(signal, () => run(target)).catch((error: unknown) => {
			if (
				error instanceof ActivityFailure &&
				error.cause instanceof ApplicationFailure &&
				error.cause.type === "PiModelError"
			) {
				const message = error.cause.details?.[0];
				if (message && typeof message === "object" && "role" in message && message.role === "assistant")
					return message as AssistantMessage;
			}
			throw error;
		});
		events.result = () => result;
		void result
			.then(
				(message) => {
					if (message.stopReason === "error" || message.stopReason === "aborted")
						events.push({ type: "error", reason: message.stopReason, error: message });
					else if (message.stopReason !== "pending")
						events.push({ type: "done", reason: message.stopReason, message });
					events.end(message);
				},
				() => events.end(),
			)
			.finally(() => {
				if (target) pending.delete(target.requestId);
			});
		return events;
	};
	for (const provider of new Set(catalog.map((model) => model.provider))) {
		const stream = (model: Model<Api>, context: ModelRequest["context"], request?: SimpleStreamOptions) => {
			const {
				temperature,
				maxTokens,
				reasoning,
				thinkingBudgets,
				toolChoice,
				sessionId,
				cacheRetention,
				metadata,
				timeoutMs,
				maxRetries,
				maxRetryDelayMs,
				transport,
				deferred,
				websocketConnectTimeoutMs,
			} = request ?? {};
			return dispatch(request?.signal, (progress) =>
				activities.piModel({
					model: { provider: model.provider, id: model.id },
					context,
					progress,
					options: {
						temperature,
						maxTokens,
						reasoning,
						thinkingBudgets,
						toolChoice,
						sessionId,
						cacheRetention,
						metadata,
						timeoutMs,
						maxRetries,
						maxRetryDelayMs,
						transport,
						deferred,
						websocketConnectTimeoutMs,
					},
				}),
			);
		};
		models.setProvider(
			createProvider({
				id: provider,
				models: catalog.filter((model) => model.provider === provider),
				auth: { apiKey: { name: "Temporal worker", resolve: async () => ({ auth: {} }) } },
				api: {
					stream,
					streamSimple: stream,
					fetchDeferred: (model, handle, request?: DeferredFetchOptions) =>
						dispatch(request?.signal, (progress) =>
							activities.piFetchDeferred({
								model: { provider: model.provider, id: model.id },
								handle,
								wait: request?.wait,
								progress,
							}),
						),
					cancelDeferred: (model, handle, request) =>
						withSignal(request?.signal, () =>
							activities.piCancelDeferred({ model: { provider: model.provider, id: model.id }, handle }),
						),
				},
			}),
		);
	}
	return models;
}

/** Run a tool as the activity with the same name. */
export function temporalTool<P extends TSchema>(
	tool: Tool<P>,
	options?: ActivityOptions,
	transport: { stream?: boolean; onProgress?: (progress: ToolProgress) => void } = {},
): ToolRegistration<P> {
	const pending = new Map<
		string,
		{ api: Parameters<ToolRegistration<P>["execute"]>[1]; attempt: number; output: string }
	>();
	if (transport.stream)
		setHandler(defineSignal<[ToolProgress]>(`pi_tool_progress:${tool.name}`), (progress) => {
			const call = pending.get(progress.requestId);
			if (!call || progress.attempt < call.attempt) return;
			if (progress.attempt === call.attempt)
				call.api.output(
					progress.output.startsWith(call.output) ? progress.output.slice(call.output.length) : progress.output,
				);
			call.attempt = progress.attempt;
			call.output = progress.output;
			transport.onProgress?.(progress);
		});
	const activities = proxyActivities<
		Record<string, (args: Static<P>, call: ToolCallInfo) => Promise<ToolExecutionResult>>
	>({ startToCloseTimeout: "5 minutes", ...options, retry: { maximumAttempts: 1, ...options?.retry } });
	return {
		...tool,
		replay: "safe", // Temporal reuses recorded results during replay.
		execute: async (args, api, context) => {
			const progress = transport.stream
				? { workflowId: workflowInfo().workflowId, runId: workflowInfo().runId, requestId: `tool-${api.taskId}` }
				: undefined;
			if (progress) pending.set(progress.requestId, { api, attempt: 1, output: "" });
			try {
				return await withSignal(context.abortSignal, () =>
					activities[tool.name](args, {
						callId: api.callId,
						taskId: api.taskId,
						conversationId: api.conversationId,
						...(progress ? { progress } : {}),
					}),
				);
			} catch (error) {
				if (error instanceof ActivityFailure && error.cause && patched("pi-tool-errors-v1"))
					throw new Error(error.cause.message, { cause: error });
				throw error;
			} finally {
				if (progress) pending.delete(progress.requestId);
			}
		},
	};
}

/** Open Pi's harness with storage rebuilt by workflow replay. */
export async function openTemporalHarness(
	options: HarnessOptions,
	storage: Storage = new MemoryStorage(),
): Promise<Harness> {
	return Harness.open(storage, options, BACKGROUND_CONTEXT);
}

/** Run one submission and close the harness. */
export async function runTemporalAgent(input: InputSubmissionDraft, options: HarnessOptions & { agent: AgentChange }) {
	const harness = await openTemporalHarness({
		...options,
		settings: { ...options.settings, retry: { enabled: false, ...options.settings?.retry } },
	});
	try {
		const root = await harness.root(BACKGROUND_CONTEXT, { agent: options.agent });
		const submission = await root.submit(input, BACKGROUND_CONTEXT);
		const settled = await Promise.race([
			submission.wait(BACKGROUND_CONTEXT),
			CancellationScope.current().cancelRequested,
		]);
		if (settled.status !== "done")
			throw ApplicationFailure.nonRetryable("Pi submission ended without an answer", "PiSubmissionUnanswered");
		return { context: await root.context(BACKGROUND_CONTEXT), usage: await harness.usage(BACKGROUND_CONTEXT) };
	} finally {
		await CancellationScope.nonCancellable(() => harness.close(BACKGROUND_CONTEXT));
	}
}

/** Open a Pi session whose complete storage state can cross workflow runs. */
export async function openTemporalSession(
	options: HarnessOptions,
	state?: PiCheckpoint,
): Promise<{ harness: Harness; checkpoint(): Promise<PiCheckpoint> }> {
	const storage = await TemporalStorage.restore(state);
	const harness = await openTemporalHarness(options, storage);
	return {
		harness,
		async checkpoint(): Promise<PiCheckpoint> {
			await harness.waitForIdle(BACKGROUND_CONTEXT);
			const inspection = await harness.inspect(BACKGROUND_CONTEXT);
			if (inspection.tasks.length || inspection.submissions.length)
				throw new Error("Checkpoint requires an idle Pi session, including background tasks");
			return storage.checkpoint();
		},
	};
}

/** Run a turn while retaining conversations, documents, identifiers, and usage. */
export async function runTemporalTurn(
	input: InputSubmissionDraft,
	options: HarnessOptions & { agent: AgentChange },
	state?: PiCheckpoint,
): Promise<{ text: string; usage: Awaited<ReturnType<Harness["usage"]>>; state: PiCheckpoint }> {
	const session = await openTemporalSession(
		{ ...options, settings: { ...options.settings, retry: { enabled: false, ...options.settings?.retry } } },
		state,
	);
	try {
		const root = await session.harness.root(BACKGROUND_CONTEXT, { agent: options.agent });
		if (state) await root.configure(options.agent, BACKGROUND_CONTEXT);
		const submission = await root.submit(input, BACKGROUND_CONTEXT);
		const settled = await Promise.race([
			submission.wait(BACKGROUND_CONTEXT),
			CancellationScope.current().cancelRequested,
		]);
		if (settled.status !== "done")
			throw ApplicationFailure.nonRetryable("Pi submission ended without an answer", "PiSubmissionUnanswered");
		const context = await root.context(BACKGROUND_CONTEXT);
		const message = [...context.messages].reverse().find((message) => message.role === "assistant");
		return {
			text:
				message?.content
					.filter((block) => block.type === "text")
					.map((block) => block.text)
					.join("\n") ?? "",
			usage: await session.harness.usage(BACKGROUND_CONTEXT),
			state: await session.checkpoint(),
		};
	} finally {
		await CancellationScope.nonCancellable(() => session.harness.close(BACKGROUND_CONTEXT));
	}
}
