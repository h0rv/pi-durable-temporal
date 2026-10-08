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
import type { ToolActivityResult, ToolReports } from "./tool-reports.js";
import type { ModelProgress, ModelRequest, ProgressTarget, ToolCallInfo, ToolProgress } from "./types.js";

export { type PiCheckpoint, TemporalStorage } from "./state.js";

const DEFAULT_ACTIVITY_OPTIONS: ActivityOptions = {
	startToCloseTimeout: "5 minutes",
	retry: { maximumAttempts: 1 },
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
	const nativeRetry = patched("native-pi-retry-v1");
	const activities = proxyActivities<ReturnType<typeof createModelActivities>>({
		...DEFAULT_ACTIVITY_OPTIONS,
		...(nativeRetry ? { heartbeatTimeout: "10 seconds" } : {}),
		...options,
		retry: { maximumAttempts: nativeRetry ? 1 : 3, ...options?.retry },
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
	const requestOptions = (request: SimpleStreamOptions = {}): ModelRequest["options"] => {
		const {
			signal: _signal,
			apiKey: _apiKey,
			fetch: _fetch,
			telemetryContext: _telemetryContext,
			onPayload: _onPayload,
			onResponse: _onResponse,
			onProviderStreamEvent: _onProviderStreamEvent,
			...options
		} = request;
		return options;
	};
	for (const provider of new Set(catalog.map((model) => model.provider))) {
		const stream = (model: Model<Api>, context: ModelRequest["context"], request?: SimpleStreamOptions) =>
			dispatch(request?.signal, (progress) =>
				activities.piModel({
					model: { provider: model.provider, id: model.id },
					context,
					progress,
					options: requestOptions(request),
				}),
			);
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
								options: requestOptions(request),
								progress,
							}),
						),
					cancelDeferred: (model, handle, request) =>
						withSignal(request?.signal, () =>
							activities.piCancelDeferred({
								model: { provider: model.provider, id: model.id },
								handle,
								options: requestOptions(request),
							}),
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
		{
			api: Parameters<ToolRegistration<P>["execute"]>[1];
			attempt: number;
			output: string;
			reports: number;
			publication: Promise<void>;
		}
	>();
	const applyReports = async (api: Parameters<ToolRegistration<P>["execute"]>[1], reports: ToolReports, seen = 0) => {
		for (const report of reports.piToolReports.slice(seen)) {
			if (report.type === "output")
				api.output(typeof report.chunk === "string" ? report.chunk : Uint8Array.from(report.chunk), report.skipped);
			else if (report.type === "diagnostic") api.diagnostic(report.value);
			else await api.details(report.value, BACKGROUND_CONTEXT);
		}
	};
	if (transport.stream)
		setHandler(defineSignal<[ToolProgress]>(`pi_tool_progress:${tool.name}`), async (progress) => {
			const call = pending.get(progress.requestId);
			if (!call || progress.attempt < call.attempt) return;
			if (progress.reports && (options?.retry?.maximumAttempts ?? 1) !== 1) {
				transport.onProgress?.(progress);
				return;
			}
			if (progress.attempt !== call.attempt) call.reports = 0;
			const previousAttempt = call.attempt;
			call.attempt = progress.attempt;
			if (progress.reports) {
				const reports = progress.reports;
				const offset = progress.offset ?? 0;
				const seen = Math.max(0, call.reports - offset);
				call.reports = Math.max(call.reports, offset + progress.reports.length);
				call.publication = call.publication.then(() =>
					applyReports(call.api, { piToolReports: reports, attempt: progress.attempt }, seen),
				);
				await call.publication;
			} else if (progress.output !== undefined && progress.attempt === previousAttempt) {
				call.api.output(
					progress.output.startsWith(call.output) ? progress.output.slice(call.output.length) : progress.output,
				);
				call.output = progress.output;
			}
			call.attempt = progress.attempt;
			transport.onProgress?.(progress);
		});
	const activities = proxyActivities<
		Record<string, (args: Static<P>, call: ToolCallInfo) => Promise<ToolExecutionResult | ToolActivityResult>>
	>({ startToCloseTimeout: "5 minutes", ...options, retry: { maximumAttempts: 1, ...options?.retry } });
	return {
		...tool,
		replay: "safe", // Temporal reuses recorded results during replay.
		execute: async (args, api, context) => {
			const progress = transport.stream
				? { workflowId: workflowInfo().workflowId, runId: workflowInfo().runId, requestId: `tool-${api.taskId}` }
				: undefined;
			if (progress)
				pending.set(progress.requestId, {
					api,
					attempt: 1,
					output: "",
					reports: 0,
					publication: Promise.resolve(),
				});
			try {
				const result = await withSignal(context.abortSignal, () =>
					activities[tool.name](args, {
						callId: api.callId,
						taskId: api.taskId,
						conversationId: api.conversationId,
						outputWindow: api.outputWindow,
						...(progress ? { progress } : {}),
					}),
				);
				if ("reports" in result) {
					const call = progress && pending.get(progress.requestId);
					if (call) await call.publication;
					await applyReports(
						api,
						result.reports,
						call && call.attempt === result.reports.attempt ? call.reports : 0,
					);
					const { reports: _reports, ...nativeResult } = result;
					return nativeResult;
				}
				return result;
			} catch (error) {
				if (error instanceof ActivityFailure && error.cause instanceof ApplicationFailure) {
					const reports = error.cause.details?.find(
						(detail): detail is ToolReports =>
							!!detail && typeof detail === "object" && "piToolReports" in detail,
					);
					if (reports) {
						const call = progress && pending.get(progress.requestId);
						if (call) await call.publication;
						await applyReports(api, reports, call && call.attempt === reports.attempt ? call.reports : 0);
					}
				}
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
	const harness = await openTemporalHarness(
		patched("native-pi-retry-v1")
			? options
			: { ...options, settings: { ...options.settings, retry: { enabled: false, ...options.settings?.retry } } },
	);
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
		patched("native-pi-retry-v1")
			? options
			: { ...options, settings: { ...options.settings, retry: { enabled: false, ...options.settings?.retry } } },
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
