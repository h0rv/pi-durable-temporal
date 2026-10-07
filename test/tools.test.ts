import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type ConversationId, MemoryStorage, type TaskId, type ToolExecutionResult } from "@earendil-works/pi-durable";
import { NodeExecutionEnv } from "@earendil-works/pi-durable/env/node";
import { createBashTool, createEditTool, createReadTool, createWriteTool } from "@earendil-works/pi-durable/tools";
import { MockActivityEnvironment } from "@temporalio/testing";
import { expect, it } from "vitest";
import { createToolActivities } from "../src/tools.js";

it("runs Pi's read, write, edit and bash tools on the worker", async () => {
	const directory = await mkdtemp(join(tmpdir(), "pi-tools-"));
	const storage = new MemoryStorage();
	const call = {
		callId: "call",
		taskId: await storage.mintId<TaskId>(),
		conversationId: await storage.mintId<ConversationId>(),
	};
	const activities = createToolActivities([createReadTool(), createWriteTool(), createEditTool(), createBashTool()], {
		env: () => new NodeExecutionEnv({ cwd: directory }),
	});
	const environment = new MockActivityEnvironment();
	const execute = (name: string, args: unknown) =>
		environment.run<[unknown, typeof call], ToolExecutionResult, (typeof activities)[string]>(
			activities[name],
			args,
			call,
		);
	try {
		await execute("write", { path: "note.txt", content: "before\n" });
		const read = await execute("read", { path: "note.txt" });
		expect(read.content).toEqual([{ type: "text", text: "before\n" }]);
		await execute("edit", { path: "note.txt", edits: [{ oldText: "before", newText: "after" }] });
		expect(await readFile(join(directory, "note.txt"), "utf8")).toBe("after\n");
		const bash = await execute("bash", { command: "cat note.txt" });
		expect(bash.content).toEqual([{ type: "text", text: "after\n" }]);
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});
