import { randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai/providers/faux";
import { type ConversationId, MemoryStorage, type TaskId } from "@earendil-works/pi-durable";
import { MockActivityEnvironment, TestWorkflowEnvironment } from "@temporalio/testing";
import { bundleWorkflowCode, Worker } from "@temporalio/worker";
import { expect, it } from "vitest";
import { createWorkspaceActivities } from "../examples/agent-harness/activities.js";
import { approveTool, ask, close, status } from "../examples/agent-harness/protocol.js";
import { model } from "../examples/model.js";
import type { ModelRequest } from "../src/index.js";

it("uses native coding tools in the configured worker directory after human approval", async () => {
	const path = process.env.TEMPORAL_CLI_PATH;
	const env = await TestWorkflowEnvironment.createLocal(
		path ? { server: { executable: { type: "existing-path", path } } } : undefined,
	);
	const directory = await mkdtemp(join(tmpdir(), "pi-workspace-"));
	const workflowId = randomUUID();
	try {
		await writeFile(join(directory, "note.txt"), "before\n");
		const bundle = await bundleWorkflowCode({
			workflowsPath: fileURLToPath(new URL("../examples/agent-harness/session.ts", import.meta.url)),
		});
		const names: string[] = [];
		const worker = await Worker.create({
			connection: env.nativeConnection,
			taskQueue: workflowId,
			workflowBundle: bundle,
			activities: {
				...createWorkspaceActivities(directory),
				async piModel(request: ModelRequest) {
					const results = request.context.messages.filter((message) => message.role === "toolResult");
					const tools = request.context.messages.flatMap((message) =>
						message.role === "system" ? (message.toolsAdded ?? []) : [],
					);
					expect(tools.map((tool) => tool.name).sort()).toEqual(["bash", "edit", "read", "write"]);
					expect(results.every((result) => !result.isError)).toBe(true);
					const calls: { name: string; args: Parameters<typeof fauxToolCall>[1] }[] = [
						{ name: "read", args: { path: "note.txt" } },
						{ name: "write", args: { path: "copy.txt", content: "before\n" } },
						{ name: "edit", args: { path: "copy.txt", edits: [{ oldText: "before", newText: "after" }] } },
						{ name: "bash", args: { command: "cat copy.txt" } },
					];
					const call = calls[results.length];
					if (!call) {
						expect(results.at(-1)?.content).toEqual([{ type: "text", text: "after\n" }]);
						return fauxAssistantMessage("Updated copy.txt and checked its contents.");
					}
					names.push(call.name);
					return fauxAssistantMessage(fauxToolCall(call.name, call.args), { stopReason: "toolUse" });
				},
			},
		});
		await worker.runUntil(async () => {
			const handle = await env.client.workflow.start("piSession", {
				workflowId,
				taskQueue: workflowId,
				args: [{ task: "workspace", approvalMode: "manual", model }],
			});
			await handle.executeUpdate(ask, {
				args: [{ text: "Copy note.txt, change before to after and check the copy." }],
			});
			const approvals: string[] = [];
			const deadline = Date.now() + 20_000;
			while (true) {
				const snapshot = await handle.query(status);
				for (const approval of snapshot.pending_approvals) {
					approvals.push(approval.tool_name);
					if (approval.tool_name === "write") {
						await expect(readFile(join(directory, "copy.txt"), "utf8")).rejects.toMatchObject({ code: "ENOENT" });
					}
					await handle.executeUpdate(approveTool, {
						args: [{ tool_id: approval.tool_id, approved: true, remember: false }],
					});
				}
				if (!snapshot.turn_active && snapshot.current_turn === 1) break;
				if (Date.now() > deadline) throw new Error("Workspace turn did not complete");
				await new Promise((resolve) => setTimeout(resolve, 50));
			}
			await handle.signal(close);
			await handle.result();
			expect(names).toEqual(["read", "write", "edit", "bash"]);
			expect(approvals).toEqual(names);
			expect(await readFile(join(directory, "copy.txt"), "utf8")).toBe("after\n");
			const history = await env.client.workflow.getHandle(`${workflowId}/turn-1`).fetchHistory();
			await Worker.runReplayHistory({ workflowBundle: bundle }, history, `${workflowId}/turn-1`);
		});
	} finally {
		await env.teardown();
		await rm(directory, { recursive: true, force: true });
	}
}, 60_000);

it("requires an explicit workspace directory", async () => {
	const activity = createWorkspaceActivities().read;
	const environment = new MockActivityEnvironment();
	const storage = new MemoryStorage();
	await expect(
		environment.run(
			activity,
			{ path: "README.md" },
			{
				callId: "call",
				taskId: await storage.mintId<TaskId>(),
				conversationId: await storage.mintId<ConversationId>(),
			},
		),
	).rejects.toMatchObject({ type: "PiWorkspaceNotConfigured", nonRetryable: true });
});
