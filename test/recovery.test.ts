import { type ChildProcess, spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Client } from "@temporalio/client";
import proto from "@temporalio/proto";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { bundleWorkflowCode, Worker } from "@temporalio/worker";
import { afterAll, beforeAll, expect, it } from "vitest";
import { localDataConverter } from "../examples/storage.js";
import type { ModelRequest } from "../src/index.js";
import { answer } from "./model.js";

let env: TestWorkflowEnvironment;
const { temporal } = proto;
let bundle: Awaited<ReturnType<typeof bundleWorkflowCode>>;
let root: string;
let bundlePath: string;
beforeAll(async () => {
	const path = process.env.TEMPORAL_CLI_PATH;
	env = await TestWorkflowEnvironment.createLocal(
		path ? { server: { executable: { type: "existing-path", path } } } : undefined,
	);
	root = await mkdtemp(join(tmpdir(), "pi-temporal-recovery-"));
	bundle = await bundleWorkflowCode({ workflowsPath: fileURLToPath(new URL("./workflows.ts", import.meta.url)) });
	bundlePath = join(root, "workflow.cjs");
	await writeFile(bundlePath, bundle.code);
}, 60_000);
afterAll(async () => {
	await env?.teardown();
	if (root) await rm(root, { recursive: true, force: true });
});

function startWorker(queue: string, directory: string, block = "") {
	const child = spawn(
		process.execPath,
		[
			"--import",
			"tsx",
			fileURLToPath(new URL("./crash-worker.ts", import.meta.url)),
			env.address,
			queue,
			directory,
			bundlePath,
			block,
		],
		{ stdio: ["ignore", "pipe", "pipe", "ipc"] },
	);
	return child;
}

function waitFor(child: ChildProcess, phase: string) {
	return new Promise<void>((resolve, reject) => {
		let output = "";
		const capture = (chunk: Buffer) => {
			output += chunk.toString();
		};
		child.stdout?.on("data", capture);
		child.stderr?.on("data", capture);
		const cleanup = () => {
			clearTimeout(timer);
			child.off("message", message);
			child.off("exit", exited);
			child.off("error", reject);
		};
		const message = (value: unknown) => {
			if (typeof value === "object" && value !== null && "phase" in value && value.phase === phase) {
				cleanup();
				resolve();
			}
		};
		const exited = () => {
			cleanup();
			reject(new Error(`Worker exited before ${phase}: ${output}`));
		};
		const timer = setTimeout(() => {
			cleanup();
			reject(new Error(`Worker did not reach ${phase}: ${output}`));
		}, 15_000);
		child.on("message", message);
		child.once("exit", exited);
		child.once("error", reject);
	});
}

async function stop(child: ChildProcess, signal: NodeJS.Signals) {
	if (child.exitCode !== null || child.signalCode !== null) return;
	await new Promise<void>((resolve) => {
		child.once("exit", () => resolve());
		child.kill(signal);
	});
}

it.each(["before-tool", "after-tool", "model"])(
	"survives SIGKILL at %s with externalized history and an idempotent tool",
	async (phase) => {
		const directory = await mkdtemp(join(root, "crash-"));
		const queue = randomUUID();
		const dataConverter = localDataConverter(join(directory, "payloads"));
		const client = new Client({ connection: env.client.connection, dataConverter });
		const first = startWorker(queue, directory, phase);
		let replacement: ChildProcess | undefined;
		try {
			await waitFor(first, "ready");
			const checkpoint = waitFor(first, phase);
			const handle = await client.workflow.start("agent", {
				workflowId: randomUUID(),
				taskQueue: queue,
				workflowExecutionTimeout: "30 seconds",
				args: [{ prompt: "Double 21", safe: true }],
			});
			await checkpoint;
			await stop(first, "SIGKILL");
			replacement = startWorker(queue, directory);
			await waitFor(replacement, "ready");
			const result = await handle.result();
			expect(result.settled.status).toBe("done");
			expect(result.context.messages.at(-1).content[0].text).toBe("The answer is 42.");
			expect(await readFile(join(directory, "effect"), "utf8")).toBe("42");
			const modelAttempts = (await readFile(join(directory, "model-attempts"), "utf8")).trim().split("\n");
			const toolAttempts = (await readFile(join(directory, "tool-attempts"), "utf8")).trim().split("\n");
			expect(modelAttempts).toHaveLength(phase === "model" ? 3 : 2);
			expect(toolAttempts).toHaveLength(phase === "model" ? 1 : 2);
			await Worker.runReplayHistory({ workflowBundle: bundle, dataConverter }, await handle.fetchHistory());
			expect((await readFile(join(directory, "model-attempts"), "utf8")).trim().split("\n")).toEqual(modelAttempts);
			expect((await readFile(join(directory, "tool-attempts"), "utf8")).trim().split("\n")).toEqual(toolAttempts);
		} finally {
			await stop(first, "SIGKILL");
			if (replacement) await stop(replacement, "SIGKILL");
		}
	},
	60_000,
);

it("passes input, model context, response and final result larger than 2 MiB using storage references", async () => {
	const directory = await mkdtemp(join(root, "large-"));
	const dataConverter = localDataConverter(directory);
	const client = new Client({ connection: env.client.connection, dataConverter });
	const queue = randomUUID();
	const input = "large-input-".repeat(300_000);
	const output = "large-output-".repeat(300_000);
	let calls = 0;
	const worker = await Worker.create({
		connection: env.nativeConnection,
		taskQueue: queue,
		workflowBundle: bundle,
		dataConverter,
		activities: {
			piModel: async (request: ModelRequest) => {
				calls++;
				expect(request.context.messages[0].content).toBe(input);
				return answer([{ type: "text", text: output }]);
			},
		},
	});
	await worker.runUntil(async () => {
		const handle = await client.workflow.start("oneShot", {
			workflowId: randomUUID(),
			taskQueue: queue,
			workflowExecutionTimeout: "30 seconds",
			args: [input],
		});
		const result = await handle.result();
		expect(result.context.messages.at(-1).content[0].text).toBe(output);
		const history = await handle.fetchHistory();
		const raw = temporal.api.history.v1.History.encode(history).finish();
		expect(raw.byteLength).toBeLessThan(30_000);
		await Worker.runReplayHistory({ workflowBundle: bundle, dataConverter }, history);
		expect(calls).toBe(1);
	});
}, 60_000);

it("preserves a waiting Pi workflow across a local Temporal server restart", async () => {
	const directory = await mkdtemp(join(root, "server-"));
	const path = process.env.TEMPORAL_CLI_PATH;
	const server = {
		dbFilename: join(directory, "temporal.sqlite"),
		...(path ? { executable: { type: "existing-path" as const, path } } : {}),
	};
	let restarted: TestWorkflowEnvironment | undefined;
	let stopped = false;
	const original = await TestWorkflowEnvironment.createLocal({ server });
	const queue = randomUUID();
	const workflowId = randomUUID();
	let calls = 0;
	const activities = {
		piModel: async () => {
			calls++;
			return answer([{ type: "text", text: "Hello" }]);
		},
	};
	try {
		const worker = await Worker.create({
			connection: original.nativeConnection,
			taskQueue: queue,
			workflowBundle: bundle,
			activities,
		});
		await worker.runUntil(async () => {
			const handle = await original.client.workflow.start("agent", {
				workflowId,
				taskQueue: queue,
				workflowExecutionTimeout: "30 seconds",
				args: [{ prompt: "Say hello", pause: true }],
			});
			for (let attempt = 0; attempt < 100; attempt++) {
				try {
					if (await handle.query("paused")) return;
				} catch {
					/* Wait for the completed turn. */
				}
				await new Promise((resolve) => setTimeout(resolve, 20));
			}
			throw new Error("Workflow did not reach its checkpoint");
		});
		await original.teardown();
		stopped = true;
		restarted = await TestWorkflowEnvironment.createLocal({ server });
		const replacement = await Worker.create({
			connection: restarted.nativeConnection,
			taskQueue: queue,
			workflowBundle: bundle,
			activities,
		});
		const handle = restarted.client.workflow.getHandle(workflowId);
		await replacement.runUntil(async () => {
			await handle.signal("proceed");
			const result = await handle.result();
			expect(result.settled.status).toBe("done");
			expect(calls).toBe(1);
		});
	} finally {
		if (!stopped) await original.teardown();
		await restarted?.teardown();
	}
}, 60_000);
