import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { setTimeout } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { Client, Connection } from "@temporalio/client";
import { bundleWorkflowCode, Worker } from "@temporalio/worker";
import { localDataConverter } from "../storage.js";
import { startWorker, stop, waitFor } from "./process.js";

const address = "localhost:7233";
const directory = await mkdtemp(resolve(".local/release-"));
const queue = `pi-release-${randomUUID()}`;
const bundle = await bundleWorkflowCode({ workflowsPath: fileURLToPath(new URL("./workflows.ts", import.meta.url)) });
const bundlePath = `${directory}/workflow.cjs`;
await writeFile(bundlePath, bundle.code);
const connection = await Connection.connect({ address });
const dataConverter = localDataConverter(`${directory}/payloads`);
const client = new Client({ connection, dataConverter });
let worker = startWorker(address, queue, directory, bundlePath, "publish");
try {
	await waitFor(worker, "ready");
	const handle = await client.workflow.start("releaseChange", {
		workflowId: queue,
		taskQueue: queue,
		workflowExecutionTimeout: "60 seconds",
	});
	for (let i = 0; (await handle.query("stage")) !== "awaitingApproval"; i++) {
		if (i > 100) throw new Error("Review did not finish");
		await setTimeout(100);
	}
	console.log("Three Pi agents finished. The test activity retried once. The workflow is waiting for approval.");
	await stop(worker);
	worker = startWorker(address, queue, directory, bundlePath, "publish");
	await waitFor(worker, "ready");
	assert.equal(await handle.query("stage"), "awaitingApproval");
	console.log("A new worker recovered the approval wait. Sending a simulated approval.");
	const checkpoint = waitFor(worker, "published");
	await handle.signal("approve", { approved: true });
	await checkpoint;
	await stop(worker);
	console.log("Killed the worker after the file was published, before activity completion.");
	worker = startWorker(address, queue, directory, bundlePath);
	await waitFor(worker, "ready");
	const result = await handle.result();
	assert.equal(result.status, "published");
	assert.equal((await readFile(`${directory}/published-events`, "utf8")).trim(), "published");
	assert.equal((await readFile(`${directory}/model-calls`, "utf8")).trim().split("\n").length, 6);
	const history = await handle.fetchHistory();
	await Worker.runReplayHistory({ workflowBundle: bundle, dataConverter }, history, queue);
	console.log(`Recovered without repeating the six model calls or publishing again. Files: ${directory}`);
	console.log(
		`History: http://localhost:8233/namespaces/default/workflows/${encodeURIComponent(queue)}/${(await handle.describe()).runId}/history`,
	);
} finally {
	await stop(worker);
	await connection.close();
}
