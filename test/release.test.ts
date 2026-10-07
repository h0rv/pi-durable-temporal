import type { ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { access, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { Client } from "@temporalio/client";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { bundleWorkflowCode, Worker } from "@temporalio/worker";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReleaseActivities } from "../examples/release/activities.js";
import { startWorker, stop, waitFor } from "../examples/release/process.js";
import { localDataConverter } from "../examples/storage.js";

let env: TestWorkflowEnvironment;
let root: string;
let bundle: Awaited<ReturnType<typeof bundleWorkflowCode>>;
let bundlePath: string;
beforeAll(async () => {
	const path = process.env.TEMPORAL_CLI_PATH;
	env = await TestWorkflowEnvironment.createLocal(
		path ? { server: { executable: { type: "existing-path", path } } } : undefined,
	);
	root = await mkdtemp(join(tmpdir(), "pi-release-"));
	bundle = await bundleWorkflowCode({
		workflowsPath: fileURLToPath(new URL("../examples/release/workflows.ts", import.meta.url)),
	});
	bundlePath = join(root, "workflows.cjs");
	await writeFile(bundlePath, bundle.code);
}, 60_000);
afterAll(async () => {
	await env?.teardown();
	if (root) await rm(root, { recursive: true, force: true });
});

async function waitForApproval(handle: ReturnType<Client["workflow"]["getHandle"]>) {
	for (let i = 0; i < 100; i++) {
		if ((await handle.query("stage")) === "awaitingApproval") return;
		await setTimeout(50);
	}
	throw new Error("Approval wait not reached");
}
it("recovers approval and publication across process crashes without repeating agent work", async () => {
	const directory = await mkdtemp(join(root, "recover-"));
	const queue = randomUUID();
	const dataConverter = localDataConverter(join(directory, "payloads"));
	const client = new Client({ connection: env.client.connection, dataConverter });
	let worker: ChildProcess = startWorker(env.address, queue, directory, bundlePath, "publish");
	try {
		await waitFor(worker, "ready");
		const handle = await client.workflow.start("releaseChange", {
			workflowId: queue,
			taskQueue: queue,
			workflowExecutionTimeout: "40 seconds",
		});
		await waitForApproval(handle);
		await expect(access(join(directory, "published.mjs"))).rejects.toThrow();
		await stop(worker);
		worker = startWorker(env.address, queue, directory, bundlePath, "publish");
		await waitFor(worker, "ready");
		expect(await handle.query("stage")).toBe("awaitingApproval");
		const checkpoint = waitFor(worker, "published");
		await handle.signal("approve", { approved: true });
		await checkpoint;
		await stop(worker);
		worker = startWorker(env.address, queue, directory, bundlePath);
		await waitFor(worker, "ready");
		expect((await handle.result()).status).toBe("published");
		expect((await readFile(join(directory, "model-calls"), "utf8")).trim().split("\n")).toHaveLength(6);
		expect((await readFile(join(directory, "tests-attempts"), "utf8")).trim().split("\n")).toHaveLength(2);
		expect((await readFile(join(directory, "publish-attempts"), "utf8")).trim().split("\n")).toHaveLength(2);
		expect(await readFile(join(directory, "published-events"), "utf8")).toBe("published\n");
		const history = await handle.fetchHistory();
		await Worker.runReplayHistory({ workflowBundle: bundle, dataConverter }, history, queue);
		const children =
			history.events?.flatMap(
				(event) => event.childWorkflowExecutionStartedEventAttributes?.workflowExecution ?? [],
			) ?? [];
		expect(children).toHaveLength(3);
		for (const child of children)
			await Worker.runReplayHistory(
				{ workflowBundle: bundle, dataConverter },
				await client.workflow.getHandle(child.workflowId ?? "", child.runId ?? undefined).fetchHistory(),
				child.workflowId ?? undefined,
			);
	} finally {
		await stop(worker);
	}
}, 60_000);

it.each(["deny", "changed"])(
	"does not publish when approval is %s",
	async (scenario) => {
		const directory = await mkdtemp(join(root, "reject-"));
		const queue = randomUUID();
		const worker = await Worker.create({
			connection: env.nativeConnection,
			taskQueue: queue,
			workflowBundle: bundle,
			activities: createReleaseActivities(directory),
		});
		await worker.runUntil(async () => {
			const handle = await env.client.workflow.start("releaseChange", {
				workflowId: queue,
				taskQueue: queue,
				workflowExecutionTimeout: "20 seconds",
			});
			await waitForApproval(handle);
			if (scenario === "changed") await writeFile(join(directory, "candidate.mjs"), "changed");
			await handle.signal("approve", { approved: scenario !== "deny" });
			if (scenario === "deny") expect((await handle.result()).status).toBe("rejected");
			else await expect(handle.result()).rejects.toThrow();
			await expect(access(join(directory, "published.mjs"))).rejects.toThrow();
		});
	},
	30_000,
);
