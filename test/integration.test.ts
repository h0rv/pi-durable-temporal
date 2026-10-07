import { randomUUID } from "node:crypto";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { Type } from "@earendil-works/pi-ai";
import { createModels, createProvider } from "@earendil-works/pi-ai/models";
import { fauxAssistantMessage, fauxProvider } from "@earendil-works/pi-ai/providers/faux";
import { AssistantMessageEventStream } from "@earendil-works/pi-ai/utils/event-stream";
import { defineTool } from "@earendil-works/pi-durable";
import { NodeExecutionEnv } from "@earendil-works/pi-durable/env/node";
import { Context } from "@temporalio/activity";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { bundleWorkflowCode, Worker } from "@temporalio/worker";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createModelActivities, createToolActivities, type ModelRequest } from "../src/index.js";
import { answer, model } from "./model.js";

let env: TestWorkflowEnvironment;
let bundle: Awaited<ReturnType<typeof bundleWorkflowCode>>;

beforeAll(async () => {
	const path = process.env.TEMPORAL_CLI_PATH;
	env = await TestWorkflowEnvironment.createLocal(
		path ? { server: { executable: { type: "existing-path", path } } } : undefined,
	);
	bundle = await bundleWorkflowCode({ workflowsPath: fileURLToPath(new URL("./workflows.ts", import.meta.url)) });
}, 60_000);

it("retries transient provider errors through the real model activity", async () => {
	const faux = fauxProvider({ provider: "test", api: "test", models: [{ id: "test" }] });
	faux.setResponses([
		fauxAssistantMessage([], { stopReason: "error", errorMessage: "429 Too Many Requests" }),
		fauxAssistantMessage("Hello"),
	]);
	const models = createModels();
	models.setProvider(faux.provider);
	const queue = randomUUID();
	const worker = await Worker.create({
		connection: env.nativeConnection,
		taskQueue: queue,
		workflowBundle: bundle,
		activities: createModelActivities(models),
	});
	await worker.runUntil(async () => {
		const result = await env.client.workflow.execute("oneShot", {
			workflowId: randomUUID(),
			taskQueue: queue,
			workflowExecutionTimeout: "10 seconds",
		});
		expect(result.context.messages.at(-1).content[0].text).toBe("Hello");
		expect(faux.state.callCount).toBe(2);
	});
}, 60_000);

it.each([false, true])(
	"only retries tool activities when opted in (safe=%s)",
	async (safe) => {
		const queue = randomUUID();
		let attempts = 0;
		let toolError: boolean | undefined;
		let toolMessage = "";
		const worker = await Worker.create({
			connection: env.nativeConnection,
			taskQueue: queue,
			workflowBundle: bundle,
			activities: {
				piModel: async (request: ModelRequest) => {
					const result = request.context.messages.find((message) => message.role === "toolResult");
					if (!result)
						return answer(
							[{ type: "toolCall", id: "call-1", name: "double", arguments: { value: 21 } }],
							"toolUse",
						);
					toolError = result.isError;
					toolMessage = JSON.stringify(result);
					return answer([{ type: "text", text: "Finished" }]);
				},
				double: async () => {
					if (++attempts === 1) throw new Error("Temporary tool failure");
					return { content: [{ type: "text", text: "42" }] };
				},
			},
		});
		await worker.runUntil(async () => {
			await env.client.workflow.execute("agent", {
				workflowId: randomUUID(),
				taskQueue: queue,
				workflowExecutionTimeout: "10 seconds",
				args: [{ prompt: "Double 21", safe }],
			});
			expect(attempts).toBe(safe ? 2 : 1);
			expect(Boolean(toolError)).toBe(!safe);
			if (!safe) expect(toolMessage).toContain("Temporary tool failure");
		});
	},
	60_000,
);

it("recovers a waiting workflow on a new worker without repeating completed calls", async () => {
	const queue = randomUUID();
	let calls = 0;
	const activities = {
		piModel: async () => {
			calls++;
			return answer([{ type: "text", text: "Hello" }]);
		},
	};
	const handle = await env.client.workflow.start("agent", {
		workflowId: randomUUID(),
		taskQueue: queue,
		workflowExecutionTimeout: "15 seconds",
		args: [{ prompt: "Say hello", pause: true }],
	});
	const worker = await Worker.create({
		connection: env.nativeConnection,
		taskQueue: queue,
		workflowBundle: bundle,
		activities,
	});
	await worker.runUntil(async () => {
		for (let attempt = 0; attempt < 100; attempt++) {
			try {
				if (await handle.query("paused")) return;
			} catch {
				/* Handler is installed after the first turn. */
			}
			await new Promise((resolve) => setTimeout(resolve, 20));
		}
		throw new Error("Workflow did not reach the checkpoint");
	});
	const replacement = await Worker.create({
		connection: env.nativeConnection,
		taskQueue: queue,
		workflowBundle: bundle,
		activities,
	});
	await replacement.runUntil(async () => {
		await handle.signal("proceed");
		const result = await handle.result();
		expect(result.settled.status).toBe("done");
		expect(calls).toBe(1);
	});
}, 60_000);

it("propagates workflow cancellation to an in-flight model activity", async () => {
	const queue = randomUUID();
	let started: () => void = () => undefined;
	let cancelled = false;
	const activityStarted = new Promise<void>((resolve) => {
		started = resolve;
	});
	const worker = await Worker.create({
		connection: env.nativeConnection,
		taskQueue: queue,
		workflowBundle: bundle,
		activities: {
			piModel: async () => {
				started();
				try {
					await Context.current().cancelled;
				} finally {
					cancelled = true;
				}
			},
		},
	});
	await worker.runUntil(async () => {
		const handle = await env.client.workflow.start("oneShot", {
			workflowId: randomUUID(),
			taskQueue: queue,
			workflowExecutionTimeout: "10 seconds",
		});
		await activityStarted;
		await handle.cancel();
		await expect(handle.result()).rejects.toMatchObject({ cause: { name: "CancelledFailure" } });
	});
	expect(cancelled).toBe(true);
}, 60_000);

it("fails a one-shot workflow when Pi cannot answer", async () => {
	const queue = randomUUID();
	const worker = await Worker.create({
		connection: env.nativeConnection,
		taskQueue: queue,
		workflowBundle: bundle,
		activities: createModelActivities(createModels()),
	});
	await worker.runUntil(async () => {
		await expect(
			env.client.workflow.execute("oneShot", {
				workflowId: randomUUID(),
				taskQueue: queue,
				workflowExecutionTimeout: "10 seconds",
			}),
		).rejects.toMatchObject({ cause: { type: "PiSubmissionUnanswered" } });
	});
}, 60_000);
afterAll(async () => {
	await env?.teardown();
});

it("replays parallel tools whose activities complete in a different order", async () => {
	const queue = randomUUID();
	let tools = 0;
	const worker = await Worker.create({
		connection: env.nativeConnection,
		taskQueue: queue,
		workflowBundle: bundle,
		activities: {
			piModel: async (request: ModelRequest) => {
				const results = request.context.messages.filter((message) => message.role === "toolResult");
				if (!results.length)
					return answer(
						[
							{ type: "toolCall", id: "call-1", name: "double", arguments: { value: 21 } },
							{ type: "toolCall", id: "call-2", name: "double", arguments: { value: 3 } },
						],
						"toolUse",
					);
				expect(results).toHaveLength(2);
				return answer([{ type: "text", text: "42 and 6" }]);
			},
			double: async ({ value }: { value: number }) => {
				tools++;
				if (value === 21) await new Promise((resolve) => setTimeout(resolve, 50));
				return { content: [{ type: "text", text: String(value * 2) }] };
			},
		},
	});
	await worker.runUntil(async () => {
		const handle = await env.client.workflow.start("agent", {
			workflowId: randomUUID(),
			taskQueue: queue,
			workflowExecutionTimeout: "10 seconds",
			args: [{ prompt: "Double 21 and 3" }],
		});
		const result = await handle.result();
		expect(result.settled.status).toBe("done");
		expect(result.context.messages.at(-1).content[0].text).toBe("42 and 6");
		await Worker.runReplayHistory({ workflowBundle: bundle }, await handle.fetchHistory());
		expect(tools).toBe(2);
	});
}, 60_000);

it("runs Pi's model/tool loop with each external call recorded in Temporal", async () => {
	const queue = randomUUID();
	let models = 0;
	let tools = 0;
	const worker = await Worker.create({
		connection: env.nativeConnection,
		taskQueue: queue,
		workflowBundle: bundle,
		activities: {
			piModel: async (request: ModelRequest) => {
				models++;
				const result = request.context.messages.find((message) => message.role === "toolResult");
				return result
					? answer([{ type: "text", text: "The answer is 42." }])
					: answer([{ type: "toolCall", id: "call-1", name: "double", arguments: { value: 21 } }], "toolUse");
			},
			double: async ({ value }: { value: number }) => {
				tools++;
				return { content: [{ type: "text", text: String(value * 2) }] };
			},
		},
	});
	await worker.runUntil(async () => {
		const handle = await env.client.workflow.start("agent", {
			workflowId: randomUUID(),
			taskQueue: queue,
			workflowExecutionTimeout: "10 seconds",
			args: [{ prompt: "Double 21" }],
		});
		const result = await handle.result();
		expect(result.settled.status).toBe("done");
		expect(result.context.messages.at(-1).content[0].text).toBe("The answer is 42.");
		expect(models).toBe(2);
		expect(tools).toBe(1);
		const history = await handle.fetchHistory();
		const names = history.events?.flatMap(
			(event) => event.activityTaskScheduledEventAttributes?.activityType?.name ?? [],
		);
		expect(names).toEqual(["piModel", "double", "piModel"]);
		await Worker.runReplayHistory({ workflowBundle: bundle }, history);
		expect(models).toBe(2);
		expect(tools).toBe(1);
	});
}, 60_000);

it("replays the browser compatibility globals without extra timers", async () => {
	const queue = randomUUID();
	const worker = await Worker.create({ connection: env.nativeConnection, taskQueue: queue, workflowBundle: bundle });
	await worker.runUntil(async () => {
		const handle = await env.client.workflow.start("compatibility", { workflowId: queue, taskQueue: queue });
		const result = await handle.result();
		expect(result.bytes).toHaveLength(16);
		expect(result.value).toBe(42);
		expect(result.clock).toBe(true);
		const history = await handle.fetchHistory();
		expect(history.events?.filter((event) => event.timerStartedEventAttributes)).toHaveLength(0);
		await Worker.runReplayHistory({ workflowBundle: bundle }, history, queue);
	});
}, 30_000);

it("publishes model progress and replays the final result", async () => {
	const faux = fauxProvider({ provider: "test", api: "test", models: [{ id: "test" }] });
	faux.setResponses([fauxAssistantMessage("Hello")]);
	const models = createModels();
	models.setProvider(faux.provider);
	const queue = randomUUID();
	const worker = await Worker.create({
		connection: env.nativeConnection,
		taskQueue: queue,
		workflowBundle: bundle,
		activities: createModelActivities(models),
	});
	await worker.runUntil(async () => {
		const handle = await env.client.workflow.start("modelTransport", {
			workflowId: queue,
			taskQueue: queue,
			workflowExecutionTimeout: "15 seconds",
		});
		const result = await handle.result();
		expect(result.result.content[0].text).toBe("Hello");
		expect(
			result.progress
				.flatMap((value: { events: { type: string }[] }) => value.events)
				.some((event: { type: string }) => event.type === "text_delta"),
		).toBe(true);
		await Worker.runReplayHistory({ workflowBundle: bundle }, await handle.fetchHistory(), queue);
		expect(faux.state.callCount).toBe(1);
	});
}, 30_000);

it("retains native Pi documents, forks, compaction, events and custom tasks in checkpoints", async () => {
	const queue = randomUUID();
	let attempts = 0;
	const worker = await Worker.create({
		connection: env.nativeConnection,
		taskQueue: queue,
		workflowBundle: bundle,
		activities: {
			piModel: async () => answer([{ type: "text", text: "Hello" }]),
			probe: async () => {
				if (++attempts === 1) throw new Error("Injected custom task failure");
				return 42;
			},
		},
	});
	await worker.runUntil(async () => {
		const handle = await env.client.workflow.start("nativeSession", {
			workflowId: queue,
			taskQueue: queue,
			workflowExecutionTimeout: "15 seconds",
		});
		const result = await handle.result();
		expect(result.rootId).toBe(result.restoredId);
		expect(result.note).toEqual({ text: "changed" });
		expect(result.forkNote).toEqual({ text: "first" });
		expect(result.outcome.outcome).toEqual({ status: "completed", result: 42 });
		expect(result.calls.filter((call: string) => call === "beforeRequest")).toHaveLength(2);
		expect(result.calls).toContain("beforeCompact");
		expect(result.events).toContain("message_start");
		expect(JSON.stringify(result.context)).toContain("Remember the first and second messages.");
		await Worker.runReplayHistory({ workflowBundle: bundle }, await handle.fetchHistory(), queue);
		expect(attempts).toBe(2);
	});
}, 30_000);

it("returns exhausted provider errors to Pi's classification and response hooks", async () => {
	const faux = fauxProvider({ provider: "test", api: "test", models: [{ id: "test" }] });
	faux.setResponses(
		Array.from({ length: 2 }, () =>
			fauxAssistantMessage([], { stopReason: "error", errorMessage: "429 Too Many Requests" }),
		),
	);
	const models = createModels();
	models.setProvider(faux.provider);
	const queue = randomUUID();
	const worker = await Worker.create({
		connection: env.nativeConnection,
		taskQueue: queue,
		workflowBundle: bundle,
		activities: createModelActivities(models),
	});
	await worker.runUntil(async () => {
		const handle = await env.client.workflow.start("failedModel", {
			workflowId: queue,
			taskQueue: queue,
			workflowExecutionTimeout: "10 seconds",
		});
		const result = await handle.result();
		expect(result.settled.status).toBe("unanswered");
		expect(result.settled.reason).toBe("model_error");
		expect(result.responses).toEqual(["429 Too Many Requests"]);
		await Worker.runReplayHistory({ workflowBundle: bundle }, await handle.fetchHistory(), queue);
		expect(faux.state.callCount).toBe(2);
	});
}, 30_000);

it("routes deferred fetch and cancellation through worker activities", async () => {
	const handle = { provider: model.provider, modelId: model.id, api: model.api, id: "deferred-1" };
	let fetched = 0;
	let cancelled = 0;
	const pending = () => {
		const events = new AssistantMessageEventStream();
		events.end({ ...answer([], "pending"), deferred: handle });
		return events;
	};
	const models = createModels();
	models.setProvider(
		createProvider({
			id: model.provider,
			models: [model],
			auth: { apiKey: { name: "test", resolve: async () => ({ auth: {} }) } },
			api: {
				stream: pending,
				streamSimple: pending,
				fetchDeferred: (_model, request, options) => {
					expect(request.id).toBe(handle.id);
					expect(options?.wait).toBe(1000);
					fetched++;
					const events = new AssistantMessageEventStream();
					events.push({
						type: "done",
						reason: "stop",
						message: answer([{ type: "text", text: "Deferred answer" }]),
					});
					return events;
				},
				cancelDeferred: async (_model, request) => {
					expect(request.id).toBe(handle.id);
					cancelled++;
				},
			},
		}),
	);
	const queue = randomUUID();
	const worker = await Worker.create({
		connection: env.nativeConnection,
		taskQueue: queue,
		workflowBundle: bundle,
		activities: createModelActivities(models),
	});
	await worker.runUntil(async () => {
		const result = await env.client.workflow.execute("deferredTransport", {
			workflowId: queue,
			taskQueue: queue,
			workflowExecutionTimeout: "10 seconds",
		});
		expect(result.content[0].text).toBe("Deferred answer");
		expect(fetched).toBe(1);
		expect(cancelled).toBe(1);
	});
}, 30_000);

it("streams bounded tool output across activity retries", async () => {
	const tool = defineTool({
		name: "progress",
		description: "Report progress",
		parameters: Type.Object({}),
		outputLimits: { maxBytes: 10, retain: "tail" },
		execute: async (_args, api) => {
			api.output("aaaaaaaaaa");
			await new Promise((resolve) => setTimeout(resolve, 150));
			api.output("bbbbbbbbbb");
			if (Context.current().info.attempt === 1) throw new Error("Injected progress failure");
			return {};
		},
	});
	const queue = randomUUID();
	const worker = await Worker.create({
		connection: env.nativeConnection,
		taskQueue: queue,
		workflowBundle: bundle,
		activities: {
			...createToolActivities([tool], { env: () => new NodeExecutionEnv({ cwd: tmpdir() }) }),
			piModel: async (request: ModelRequest) =>
				request.context.messages.some((message) => message.role === "toolResult")
					? answer([{ type: "text", text: "Done" }])
					: answer([{ type: "toolCall", id: "progress-1", name: "progress", arguments: {} }], "toolUse"),
		},
	});
	await worker.runUntil(async () => {
		const handle = await env.client.workflow.start("toolTransport", {
			workflowId: queue,
			taskQueue: queue,
			workflowExecutionTimeout: "10 seconds",
		});
		const { result, progress } = await handle.result();
		expect(
			result.context.messages.find((message: { role: string }) => message.role === "toolResult").content,
		).toEqual([{ type: "text", text: "bbbbbbbbbb" }]);
		expect(new Set(progress.map((value: { attempt: number }) => value.attempt))).toEqual(new Set([1, 2]));
		expect(progress.at(-1).output).toBe("bbbbbbbbbb");
		await Worker.runReplayHistory({ workflowBundle: bundle }, await handle.fetchHistory(), queue);
	});
}, 30_000);

it("applies native tool blocks and replacements before the next model request", async () => {
	const queue = randomUUID();
	const executed: string[] = [];
	let models = 0;
	const worker = await Worker.create({
		connection: env.nativeConnection,
		taskQueue: queue,
		workflowBundle: bundle,
		activities: {
			piModel: async (request: ModelRequest) => {
				models++;
				const results = request.context.messages.filter((message) => message.role === "toolResult");
				if (!results.length)
					return answer(
						[
							{ type: "toolCall", id: "blocked", name: "echo", arguments: { text: "blocked" } },
							{ type: "toolCall", id: "rewrite", name: "echo", arguments: { text: "original" } },
							{ type: "toolCall", id: "failure", name: "echo", arguments: { text: "fail" } },
						],
						"toolUse",
					);
				const byId = new Map(results.map((result) => [result.toolCallId, result]));
				expect(byId.get("blocked")?.isError).toBe(true);
				expect(JSON.stringify(byId.get("blocked"))).toContain("Approval required");
				expect(byId.get("rewrite")?.content).toEqual([{ type: "text", text: "Replaced by hook" }]);
				expect(byId.get("failure")?.isError).toBe(false);
				expect(byId.get("failure")?.content).toEqual([{ type: "text", text: "Recovered by hook" }]);
				return answer([{ type: "text", text: "Done" }]);
			},
			echo: async ({ text }: { text: string }) => {
				executed.push(text);
				if (text === "fail") throw new Error("Injected hook test failure");
				return { content: [{ type: "text", text }] };
			},
		},
	});
	await worker.runUntil(async () => {
		const handle = await env.client.workflow.start("nativeHooks", {
			workflowId: queue,
			taskQueue: queue,
			workflowExecutionTimeout: "10 seconds",
			args: ["tools"],
		});
		const result = await handle.result();
		expect(result.settled.status).toBe("done");
		expect(executed.sort()).toEqual(["fail", "rewritten"]);
		expect(result.failures).toHaveLength(1);
		expect(result.failures[0]).toContain("Injected hook test failure");
		expect(result.rounds).toEqual([3]);
		expect(result.calls.indexOf("afterTools")).toBeGreaterThan(result.calls.indexOf("afterTool:failure"));
		const history = await handle.fetchHistory();
		expect(
			history.events?.filter((event) => event.activityTaskScheduledEventAttributes?.activityType?.name === "echo"),
		).toHaveLength(2);
		await Worker.runReplayHistory({ workflowBundle: bundle }, history, queue);
		expect(models).toBe(2);
		expect(executed).toHaveLength(2);
	});
}, 30_000);

it("continues on yield and keeps request replacements out of stored context", async () => {
	const queue = randomUUID();
	let models = 0;
	const worker = await Worker.create({
		connection: env.nativeConnection,
		taskQueue: queue,
		workflowBundle: bundle,
		activities: {
			piModel: async (request: ModelRequest) => {
				models++;
				expect(request.context.messages.at(-1)).toMatchObject({ role: "user", content: "Request only" });
				if (models === 2) expect(JSON.stringify(request.context.messages)).toContain("Check the answer");
				return answer([{ type: "text", text: `Answer ${models}` }]);
			},
		},
	});
	await worker.runUntil(async () => {
		const handle = await env.client.workflow.start("nativeHooks", {
			workflowId: queue,
			taskQueue: queue,
			workflowExecutionTimeout: "10 seconds",
			args: ["yield"],
		});
		const result = await handle.result();
		expect(result.settled.status).toBe("done");
		expect(result.context.messages.at(-1).content).toEqual([{ type: "text", text: "Answer 2" }]);
		expect(JSON.stringify(result.context.messages)).not.toContain("Request only");
		expect(JSON.stringify(result.context.messages)).toContain("Check the answer");
		expect(result.calls).toEqual([
			"beforeRequest",
			"afterResponse",
			"onYield",
			"beforeRequest",
			"afterResponse",
			"onYield",
		]);
		await Worker.runReplayHistory({ workflowBundle: bundle }, await handle.fetchHistory(), queue);
		expect(models).toBe(2);
	});
}, 30_000);

it.each(["terminate", "mixed"])(
	"honors tool termination only for a whole round (%s)",
	async (mode) => {
		const queue = randomUUID();
		let models = 0;
		let tools = 0;
		const worker = await Worker.create({
			connection: env.nativeConnection,
			taskQueue: queue,
			workflowBundle: bundle,
			activities: {
				piModel: async () =>
					++models === 1
						? answer(
								[
									{ type: "toolCall", id: "stop", name: "echo", arguments: { text: "stop" } },
									{ type: "toolCall", id: "other", name: "echo", arguments: { text: "other" } },
								],
								"toolUse",
							)
						: answer([{ type: "text", text: "Finished" }]),
				echo: async () => {
					tools++;
					return { content: [] };
				},
			},
		});
		await worker.runUntil(async () => {
			const handle = await env.client.workflow.start("nativeHooks", {
				workflowId: queue,
				taskQueue: queue,
				workflowExecutionTimeout: "10 seconds",
				args: [mode],
			});
			const result = await handle.result();
			expect(result.settled.status).toBe("done");
			expect(result.rounds).toEqual([2]);
			expect(result.context.messages.at(-1).role).toBe(mode === "terminate" ? "toolResult" : "assistant");
			await Worker.runReplayHistory({ workflowBundle: bundle }, await handle.fetchHistory(), queue);
			expect(models).toBe(mode === "terminate" ? 1 : 2);
			expect(tools).toBe(2);
		});
	},
	30_000,
);
