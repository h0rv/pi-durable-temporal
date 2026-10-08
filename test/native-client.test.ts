import { randomUUID } from "node:crypto";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai/providers/faux";
import type { ConversationView } from "@earendil-works/pi-durable";
import { Context, heartbeat } from "@temporalio/activity";
import { type AgentSseFrame, HttpTransport } from "@temporalio/agent-harness-client";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { bundleWorkflowCode, Worker } from "@temporalio/worker";
import { expect, it } from "vitest";
import { createConsole } from "../examples/agent-harness/server.js";
import type { ModelRequest } from "../src/types.js";
import { model } from "./model.js";

it.skipIf(!existsSync(fileURLToPath(new URL("../.local/pi-upstream/package.json", import.meta.url))))(
	"serves the upstream controller and transcript from one Temporal session",
	async () => {
		const path = process.env.TEMPORAL_CLI_PATH;
		const env = await TestWorkflowEnvironment.createLocal(
			path ? { server: { executable: { type: "existing-path", path } } } : undefined,
		);
		const taskQueue = randomUUID();
		const requests: ModelRequest[] = [];
		let block = false;
		let started = () => {};
		let cancelled = () => {};
		const modelCancelled = new Promise<void>((resolve) => {
			cancelled = resolve;
		});
		const modelStarted = new Promise<void>((resolve) => {
			started = resolve;
		});
		const console = createConsole(env.client, ".", taskQueue);
		await new Promise<void>((resolve) => console.listen(0, "127.0.0.1", resolve));
		const address = console.address();
		if (!address || typeof address === "string") throw new Error("No console port");
		const baseUrl = `http://127.0.0.1:${address.port}/api/`;
		const transport = new HttpTransport({ baseUrl });
		try {
			const workflowOptions = {
				workflowsPath: fileURLToPath(new URL("../examples/native-client/workflows.ts", import.meta.url)),
				webpackConfigHook(config: import("webpack").Configuration) {
					config.resolve ??= {};
					config.resolve.modules = [fileURLToPath(new URL("../node_modules", import.meta.url)), "node_modules"];
					return config;
				},
			};
			const bundle = await bundleWorkflowCode(workflowOptions);
			const worker = await Worker.create({
				connection: env.nativeConnection,
				taskQueue,
				workflowBundle: bundle,
				activities: {
					async piModel(request: ModelRequest) {
						requests.push(request);
						const last = request.context.messages.at(-1);
						if (last?.role === "user" && last.content === "tool prompt")
							return fauxAssistantMessage(fauxToolCall("write", { path: "test.txt", content: "hello" }), {
								stopReason: "toolUse",
							});
						if (block) {
							started();
							const timer = setInterval(() => heartbeat(), 20);
							try {
								await Context.current().cancelled;
							} finally {
								clearInterval(timer);
								cancelled();
							}
						}
						return fauxAssistantMessage(`answer ${requests.length}`);
					},
					async write() {
						return { content: [{ type: "text", text: "Wrote test.txt" }] };
					},
				},
			});
			await worker.runUntil(async () => {
				const id = randomUUID();
				const first = await env.client.workflow.start("piNativeSession", {
					workflowId: id,
					taskQueue,
					args: [
						{
							model,
							cwd: "/workspace",
							maxTurnsPerRun: 2,
							retainTraceTurns: 2,
							settings: { compaction: { enabled: false, keepRecentTokens: 1 } },
						},
					],
				});
				const runIds = [(await first.describe()).runId];
				const second = env.client.workflow.getHandle(id);
				const prompt = await first.executeUpdate<
					{ accepted: true; operationId: string },
					[{ message: string; images: null }]
				>("piController.prompt", {
					args: [{ message: "first prompt", images: null }],
				});
				expect(prompt.accepted).toBe(true);
				expect(await second.executeUpdate("piController.waitForPrompt", { args: [prompt.operationId] })).toEqual({
					status: "done",
					text: "answer 1",
					reason: null,
				});
				expect(await second.query<ConversationView>("piTranscript")).toEqual(await first.query("piTranscript"));
				const followUp = await second.executeUpdate<
					{ accepted: true; entryId: string },
					[{ message: string; images: null }]
				>("piController.followUp", {
					args: [{ message: "second prompt", images: null }],
				});
				expect(await first.executeUpdate("piController.waitForPrompt", { args: [followUp.entryId] })).toEqual({
					status: "done",
					text: "answer 2",
					reason: null,
				});
				expect(requests[1].context.messages.some((message) => message.role === "assistant")).toBe(true);
				await expect.poll(async () => (await first.describe()).runId, { timeout: 10000 }).not.toBe(runIds[0]);
				runIds.push((await first.describe()).runId);
				expect(
					await first.executeUpdate("piController.waitForPrompt", { args: [prompt.operationId] }),
				).toMatchObject({ status: "done", text: "answer 1" });
				expect(
					(await first.query<ConversationView>("piTranscript")).entries.some((entry) =>
						entry.model?.some((m) => m.role === "assistant"),
					),
				).toBe(true);
				const tool = await first.executeUpdate<{ operationId: string }, [{ message: string; images: null }]>(
					"piController.prompt",
					{ args: [{ message: "tool prompt", images: null }] },
				);
				expect(await first.executeUpdate("piController.waitForPrompt", { args: [tool.operationId] })).toMatchObject(
					{ status: "done" },
				);
				await expect
					.poll(async () =>
						(await (await fetch(`${baseUrl}sessions`)).json()).some(
							(s: { workflow_id: string }) => s.workflow_id === id,
						),
					)
					.toBe(true);
				const frames: AgentSseFrame[] = [];
				for await (const frame of transport.attach(id, 0, AbortSignal.timeout(10_000))) frames.push(frame);
				expect(frames.filter((frame) => frame.event === "turn_end")).toHaveLength(2);
				expect(frames.filter((frame) => frame.event === "model_interaction_ended")).toHaveLength(3);
				expect(frames.filter((frame) => frame.event === "tool_end")).toHaveLength(1);
				expect(frames.filter((frame) => frame.event === "message_handler_end")).toHaveLength(2);
				expect(frames.filter((frame) => frame.event === "message_handler_error")).toHaveLength(0);
				expect(frames.find((frame) => frame.event === "state_snapshot")?.data).toMatchObject({
					state_id: "pi.usage",
				});
				expect(await transport.agentStatus(id)).toMatchObject({ turn_active: false });
				expect((await fetch(`${baseUrl}agent-interface/${id}`)).status).toBe(200);
				expect(
					(
						await fetch(`${baseUrl}messages`, {
							method: "POST",
							headers: { "Content-Type": "application/json" },
							body: JSON.stringify({ session_id: id, message: { type: "ask", payload: { text: "blocked" } } }),
						})
					).status,
				).toBe(405);
				await first.executeUpdate("piConfigure", { args: [{ thinkingLevel: "off" }] });
				expect((await second.query<ConversationView>("piTranscript")).docs["pi.agent"].thinkingLevel).toBe("off");
				expect(await first.executeUpdate("piController.cancelQueued", { args: ["bad-id"] })).toEqual({
					outcome: "not_found",
				});
				const compact = await first.executeUpdate<{ accepted: boolean }, [{ customInstructions: null }]>(
					"piController.compact",
					{
						args: [{ customInstructions: null }],
					},
				);
				expect(compact.accepted).toBe(true);
				await expect
					.poll(
						async () =>
							(await first.query<ConversationView>("piTranscript")).entries.some(
								(entry) => entry.kind === "pi.compaction",
							),
						{ timeout: 10000 },
					)
					.toBe(true);
				block = true;
				const blocked = await first.executeUpdate<
					{ accepted: true; operationId: string },
					[{ message: string; images: null }]
				>("piController.prompt", {
					args: [{ message: "keep running", images: null }],
				});
				await modelStarted;
				const queued = await second.executeUpdate<
					{ accepted: true; entryId: string },
					[{ message: string; images: null }]
				>("piController.followUp", {
					args: [{ message: "queued", images: null }],
				});
				expect(await first.executeUpdate("piController.cancelQueued", { args: [queued.entryId] })).toEqual({
					outcome: "cancelled",
				});
				const steering = await second.executeUpdate<
					{ accepted: true; entryId: string },
					[{ message: string; images: null }]
				>("piController.steer", {
					args: [{ message: "change direction", images: null }],
				});
				expect(steering.accepted).toBe(true);
				await second.executeUpdate("piController.abort");
				await modelCancelled;
				expect(
					await first.executeUpdate("piController.waitForPrompt", { args: [blocked.operationId] }),
				).toMatchObject({ status: "unanswered" });
				await expect.poll(async () => (await first.describe()).runId, { timeout: 10000 }).not.toBe(runIds[1]);
				runIds.push((await first.describe()).runId);
				const aborted: AgentSseFrame[] = [];
				for await (const frame of transport.attach(id, 0, AbortSignal.timeout(10_000))) aborted.push(frame);
				expect(aborted.filter((frame) => frame.event === "message_handler_error").length).toBeGreaterThan(0);

				await first.signal("piCloseSession");
				await expect(
					second.executeUpdate("piController.prompt", { args: [{ message: "after close", images: null }] }),
				).rejects.toThrow();
				await first.result();
				for (const runId of runIds) {
					const history = await env.client.workflow.getHandle(id, runId).fetchHistory();
					await Worker.runReplayHistory({ workflowBundle: bundle }, history);
				}
				block = false;
				const empty = await env.client.workflow.start("piNativeSession", {
					workflowId: randomUUID(),
					taskQueue,
					args: [{ model, cwd: "/workspace", maxTurnsPerRun: 1, retainTraceTurns: 0 }],
				});
				const emptyRun = (await empty.describe()).runId;
				await empty.executeUpdate("piController.prompt", {
					args: [{ message: "no retained trace", images: null }],
				});
				await expect.poll(async () => (await empty.describe()).runId, { timeout: 10000 }).not.toBe(emptyRun);
				expect(await empty.query("trace_snapshot")).toMatchObject({ log: [] });
				const emptyFrames: AgentSseFrame[] = [];
				for await (const frame of transport.attach(empty.workflowId, 0, AbortSignal.timeout(10_000)))
					emptyFrames.push(frame);
				expect(emptyFrames).toEqual([]);
				await empty.signal("piCloseSession");
				await empty.result();
			});
		} finally {
			console.closeAllConnections();
			await new Promise<void>((resolve) => console.close(() => resolve()));
			await env.teardown();
		}
	},
	120_000,
);
