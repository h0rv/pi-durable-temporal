import { randomUUID } from "node:crypto";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import type { ConversationView } from "@earendil-works/pi-durable";
import { Context, heartbeat } from "@temporalio/activity";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { bundleWorkflowCode, Worker } from "@temporalio/worker";
import { expect, it } from "vitest";
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
				},
			});
			await worker.runUntil(async () => {
				const id = randomUUID();
				const first = await env.client.workflow.start("piNativeSession", {
					workflowId: id,
					taskQueue,
					args: [{ model, cwd: "/workspace", settings: { compaction: { enabled: false, keepRecentTokens: 1 } } }],
				});
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

				await first.signal("piCloseSession");
				await expect(
					second.executeUpdate("piController.prompt", { args: [{ message: "after close", images: null }] }),
				).rejects.toThrow();
				await first.result();
				const history = await first.fetchHistory();
				await Worker.runReplayHistory({ workflowBundle: bundle }, history);
			});
		} finally {
			await env.teardown();
		}
	},
	120_000,
);
