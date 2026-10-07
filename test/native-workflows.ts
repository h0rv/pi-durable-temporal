import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { Type } from "@earendil-works/pi-ai";
import {
	CompactionTask,
	createRegistry,
	defineDoc,
	defineExtension,
	defineTask,
	GenerationTask,
	hook,
	ToolTask,
	watchEvents,
} from "@earendil-works/pi-durable";
import { proxyActivities } from "@temporalio/workflow";
import { createTemporalModels, openTemporalSession, temporalTool } from "../src/workflow.js";
import { model } from "./model.js";

export async function nativeSession() {
	const calls: string[] = [];
	const Note = defineDoc<{ text: string }>({
		kind: "test.note",
		version: 1,
		scope: "conversation",
		history: "rewindable",
		fork: "asOf",
		initial: () => ({ text: "first" }),
	});
	const activities = proxyActivities<{ probe(): Promise<number> }>({
		startToCloseTimeout: "2 seconds",
		retry: { maximumAttempts: 2, initialInterval: "10 ms" },
	});
	const Probe = defineTask<null, { phase: "run" }, number>({
		name: "test.probe",
		version: 1,
		initial: () => ({ phase: "run" }),
		phases: {
			run: async (_task, runtime, context) => {
				const result = await activities.probe();
				await runtime.commit(() => ({ status: "terminal", outcome: { status: "completed", result } }), context);
			},
		},
		abort: async (_task, runtime, context) => {
			await runtime.commit(() => ({ status: "terminal", outcome: { status: "aborted" } }), context);
		},
	});
	const registry = createRegistry();
	registry.install(
		defineExtension({
			name: "native",
			tasks: [Probe],
			hooks: [
				hook(GenerationTask, {
					beforeRequest: () => {
						calls.push("beforeRequest");
						return undefined;
					},
					afterResponse: () => {
						calls.push("afterResponse");
					},
					onYield: () => {
						calls.push("onYield");
						return undefined;
					},
					afterTools: () => {
						calls.push("afterTools");
					},
				}),
				hook(CompactionTask, {
					beforeCompact: () => {
						calls.push("beforeCompact");
						return { summary: "Remember the first and second messages." };
					},
				}),
			],
		}),
	);
	const options = {
		models: createTemporalModels([model]),
		registry,
		settings: { retry: { enabled: false }, compaction: { enabled: false, keepRecentTokens: 1 } },
	};
	const session = await openTemporalSession(options);
	try {
		const root = await session.harness.root(BACKGROUND_CONTEXT, {
			agent: { model: { provider: model.provider, modelId: model.id } },
			init: async (tx, conversation) => {
				await tx.doc(Note, conversation);
			},
		});
		const marker = await root.commit(
			(tx) => tx.appendEntry(root.id, { kind: "test.marker", data: { label: "fork" } }),
			BACKGROUND_CONTEXT,
		);
		await root.commit(async (tx) => {
			(await tx.doc(Note, root.id)).text = "changed";
		}, BACKGROUND_CONTEXT);
		const fork = await root.fork(marker.id, { ownership: { kind: "ownerless" } }, BACKGROUND_CONTEXT);
		const events: string[] = [];
		const watcher = await watchEvents(session.harness, root.id, BACKGROUND_CONTEXT);
		watcher.start(async (batch) => {
			for (const event of batch) events.push(event.type);
		});
		for (const content of ["first ".repeat(100), "second ".repeat(100)]) {
			const submission = await root.submit({ type: "input", content }, BACKGROUND_CONTEXT);
			await submission.wait(BACKGROUND_CONTEXT);
		}
		const compact = await root.compact(undefined, BACKGROUND_CONTEXT);
		await session.harness.waitForTask(compact, BACKGROUND_CONTEXT);
		const task = await root.commit(
			(tx) => tx.createTask(Probe, null, { ownership: { kind: "conversation" } }),
			BACKGROUND_CONTEXT,
		);
		const outcome = (await session.harness.waitForTask(task, BACKGROUND_CONTEXT)).state;
		await watcher.stop();
		const checkpoint = await session.checkpoint();
		await session.harness.close(BACKGROUND_CONTEXT);
		const restored = await openTemporalSession(options, checkpoint);
		try {
			const rootAgain = await restored.harness.root(BACKGROUND_CONTEXT);
			return {
				calls,
				events,
				outcome,
				rootId: root.id,
				restoredId: rootAgain.id,
				note: await restored.harness.snapshot(Note, root.id, BACKGROUND_CONTEXT),
				forkNote: await restored.harness.snapshot(Note, fork.id, BACKGROUND_CONTEXT),
				context: await rootAgain.context(BACKGROUND_CONTEXT),
			};
		} finally {
			await restored.harness.close(BACKGROUND_CONTEXT);
		}
	} finally {
		await session.harness.close(BACKGROUND_CONTEXT);
	}
}

export async function failedModel() {
	const responses: string[] = [];
	const registry = createRegistry();
	registry.install(
		defineExtension({
			name: "errors",
			hooks: [
				hook(GenerationTask, {
					afterResponse: (message) => {
						responses.push(message.errorMessage ?? "");
					},
				}),
			],
		}),
	);
	const session = await openTemporalSession({
		models: createTemporalModels([model], { retry: { maximumAttempts: 2, initialInterval: "10 ms" } }),
		registry,
		settings: { retry: { enabled: false }, compaction: { enabled: false } },
	});
	try {
		const root = await session.harness.root(BACKGROUND_CONTEXT, {
			agent: { model: { provider: model.provider, modelId: model.id } },
		});
		const submission = await root.submit({ type: "input", content: "Fail" }, BACKGROUND_CONTEXT);
		return { settled: await submission.wait(BACKGROUND_CONTEXT), responses };
	} finally {
		await session.harness.close(BACKGROUND_CONTEXT);
	}
}

export async function nativeHooks(mode: "tools" | "yield" | "terminate" | "mixed") {
	const calls: string[] = [];
	const failures: string[] = [];
	const rounds: number[] = [];
	let yields = 0;
	const registry = createRegistry();
	registry.install(
		defineExtension({
			name: "hooks",
			tools: [
				temporalTool({ name: "echo", description: "Echo text", parameters: Type.Object({ text: Type.String() }) }),
			],
			hooks: [
				hook(ToolTask, {
					beforeTool: (call) => {
						calls.push(`beforeTool:${call.id}`);
						if (call.id === "blocked") return { block: "Approval required" };
						if (call.id === "rewrite") return { arguments: { text: "rewritten" } };
						return undefined;
					},
					afterTool: (call, result) => {
						calls.push(`afterTool:${call.id}`);
						if (result.isError) {
							failures.push(JSON.stringify(result));
							return { content: [{ type: "text", text: "Recovered by hook" }], isError: false };
						}
						if (mode === "terminate" || (mode === "mixed" && call.id === "stop"))
							return { ...result, control: { terminate: true } };
						if (call.id === "rewrite")
							return { ...result, content: [{ type: "text", text: "Replaced by hook" }] };
						return undefined;
					},
				}),
				hook(GenerationTask, {
					beforeRequest: ({ messages }) => {
						calls.push("beforeRequest");
						return mode === "yield"
							? { messages: [...messages, { role: "user", content: "Request only", timestamp: 0 }] }
							: undefined;
					},
					afterResponse: () => {
						calls.push("afterResponse");
					},
					afterTools: (_assistant, results) => {
						calls.push("afterTools");
						rounds.push(results.length);
					},
					onYield: () => {
						calls.push("onYield");
						return mode === "yield" && yields++ === 0 ? { continue: "Check the answer" } : undefined;
					},
				}),
			],
		}),
	);
	const session = await openTemporalSession({
		models: createTemporalModels([model]),
		registry,
		settings: { retry: { enabled: false }, compaction: { enabled: false } },
	});
	try {
		const root = await session.harness.root(BACKGROUND_CONTEXT, {
			agent: { model: { provider: model.provider, modelId: model.id } },
		});
		const submission = await root.submit({ type: "input", content: "Run hooks" }, BACKGROUND_CONTEXT);
		return {
			settled: await submission.wait(BACKGROUND_CONTEXT),
			context: await root.context(BACKGROUND_CONTEXT),
			calls,
			failures,
			rounds,
		};
	} finally {
		await session.harness.close(BACKGROUND_CONTEXT);
	}
}
