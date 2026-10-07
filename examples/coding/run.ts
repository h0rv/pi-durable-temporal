import assert from "node:assert/strict";
import { type ChildProcess, spawn } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { setTimeout } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { HttpTransport } from "@temporalio/agent-harness-client";
import { Client, Connection } from "@temporalio/client";
import { bundleWorkflowCode, Worker } from "@temporalio/worker";
import { type PendingApproval, status } from "../agent-harness/protocol.js";
import { createConsole } from "../agent-harness/server.js";
import { installUi } from "../agent-harness/ui.js";
import { exampleModel } from "../providers.js";
import { stop, waitFor } from "../release/process.js";
import { localDataConverter } from "../storage.js";

const provider = process.env.PI_PROVIDER ?? (process.env.OPENAI_API_KEY ? "openai" : "codex");
if (provider === "faux") throw new Error("The coding example needs PI_PROVIDER=openai or codex");
const model = exampleModel({ ...process.env, PI_PROVIDER: provider });
const id = `pi-coding-${randomUUID()}`;
const taskQueue = id;
const port = Number(process.env.PI_CONSOLE_PORT ?? 8001);
const root = resolve(".local/coding");
await mkdir(root, { recursive: true });
const record = resolve(".local/coding-demo.cast");
const events: Array<[number, "o", string]> = [];
const started = Date.now();
const header = {
	version: 2,
	width: 110,
	height: 30,
	timestamp: Math.floor(started / 1000),
	title: "Pi Durable on Temporal",
};
async function log(message: string) {
	console.log(message);
	events.push([(Date.now() - started) / 1000, "o", `${message}\r\n`]);
	await writeFile(record, `${[JSON.stringify(header), ...events.map((event) => JSON.stringify(event))].join("\n")}\n`);
}
const connection = await Connection.connect({ address: "localhost:7233" });
const dataConverter = localDataConverter();
const client = new Client({ connection, dataConverter });
const server = createConsole(client, await installUi(), taskQueue, model);
await new Promise<void>((resolve) => server.listen(port, "127.0.0.1", resolve));
const transport = new HttpTransport({ baseUrl: `http://localhost:${port}/api/` });
let worker: ChildProcess;
const launch = (block: boolean) =>
	spawn(process.execPath, ["--import", "tsx", fileURLToPath(new URL("../agent-harness/worker.ts", import.meta.url))], {
		stdio: ["ignore", "pipe", "pipe", "ipc"],
		env: {
			...process.env,
			TEMPORAL_API_KEY: "",
			TEMPORAL_TLS: "false",
			TEMPORAL_ADDRESS: "localhost:7233",
			TEMPORAL_NAMESPACE: "default",
			TEMPORAL_TASK_QUEUE: taskQueue,
			PI_PROVIDER: provider,
			PI_RECORD_MODEL_CALLS: "true",
			PI_CODING_DIRECTORY: root,
			PI_PAUSE_AFTER_PUBLISH: String(block),
		},
	});
async function shutdown() {
	if (worker) await stop(worker);
	server.closeAllConnections();
	server.close();
	await connection.close();
}
process.once("SIGINT", () => void shutdown());
process.once("SIGTERM", () => void shutdown());
try {
	worker = launch(true);
	await waitFor(worker, "ready", 60_000);
	await log(`Pi Durable coding agent on Temporal. Provider: ${provider}/${model.id}`);
	await transport.createSession({
		agent_workflow_type: "piSession",
		session_id: id,
		data: { task: "coding", approvalMode: "manual" },
	});
	// The console's provider setting is independent of the worker's credentials.
	const handle = client.workflow.getHandle(id);
	await transport.submitMessage(id, {
		type: "ask",
		payload: {
			text: "Fix total(items) so it sums price * quantity for each item. Read the existing code, write the correction, run tests, and request publication of the passing source hash. Use readCandidate, writeCandidate, runTests and publishCandidate in that order.",
		},
	});
	await log(`Trace and approval: http://localhost:${port}/?s=${id}`);
	let request: PendingApproval | undefined;
	for (let attempt = 0; attempt < 360; attempt++) {
		const snapshot = await handle.query(status);
		request = snapshot.pending_approvals.find((tool) => tool.tool_name === "publishCandidate");
		if (request) break;
		if (!snapshot.turn_active && !snapshot.pending_turns.length)
			throw new Error("Agent ended without requesting publication");
		await setTimeout(500);
	}
	if (!request) throw new Error("Publication approval was not requested within three minutes");

	const directory = join(root, createHash("sha256").update(id).digest("hex"));
	const modelCalls = async () =>
		(await readFile(join(directory, "model-calls"), "utf8"))
			.trim()
			.split("\n")
			.map((line) => JSON.parse(line) as { workflowId: string; activityId: string; attempt: number })
			.filter((call) => call.workflowId === `${id}/turn-1`);
	await log(`Generated code:\n${await readFile(join(directory, "candidate.mjs"), "utf8")}`);
	await log("Tests passed after an injected test runner outage. Publication is waiting for human approval.");
	const checkpoint = waitFor(worker, "published", process.argv.includes("--approve") ? 20_000 : 0);
	if (process.argv.includes("--approve")) {
		await transport.approveTool(id, request.tool_id, {
			approved: true,
			reason: "Simulated human approval for the recorded local demo",
		});
		await log("Sent a simulated human approval through the console API.");
	} else await log("Approve publication in the pending tool card to continue.");
	await checkpoint;
	const before = await modelCalls();
	await stop(worker);
	await log("Killed the worker after the file was published, before activity completion.");
	worker = launch(false);
	await waitFor(worker, "ready", 60_000);
	await log("Started a replacement worker on the same Temporal task queue.");
	const frames = [];
	for await (const frame of transport.attach(id, 0, AbortSignal.timeout(90_000))) frames.push(frame);
	assert(frames.some((frame) => frame.event === "turn_end"));
	assert(!frames.some((frame) => frame.event === "message_handler_error"));
	const after = await modelCalls();
	assert.equal(after.length, before.length + 1, "Only the final response should call the model after recovery");
	assert.deepEqual(after.slice(0, before.length), before);
	assert.equal(new Set(after.map((call) => call.activityId)).size, after.length, "Model activities must not repeat");
	assert.equal((await readFile(join(directory, "publications"), "utf8")).trim().split("\n").length, 1);
	const history = await handle.fetchHistory();
	const bundle = await bundleWorkflowCode({
		workflowsPath: fileURLToPath(new URL("../agent-harness/session.ts", import.meta.url)),
	});
	await Worker.runReplayHistory({ workflowBundle: bundle, dataConverter }, history, id);
	await log(
		`Completed. ${before.length} recorded model calls were reused. One final model call ran after recovery. The file was published once.`,
	);
	await log(`Recording: ${record}`);
	await log("The console and replacement worker remain running. Press Ctrl+C to stop them.");
} catch (error) {
	await shutdown();
	throw error;
}
