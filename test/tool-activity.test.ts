import { randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai/providers/faux";
import { NodeExecutionEnv } from "@earendil-works/pi-durable/env/node";
import { createBashTool } from "@earendil-works/pi-durable/tools";
import { Client } from "@temporalio/client";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { bundleWorkflowCode, Worker } from "@temporalio/worker";
import { afterAll, beforeAll, expect, it } from "vitest";
import { localDataConverter } from "../examples/storage.js";
import type { ModelRequest } from "../src/index.js";
import { createToolActivities } from "../src/tools.js";
import { reportTool } from "./tool-report-tools.js";

let env: TestWorkflowEnvironment;
let bundle: Awaited<ReturnType<typeof bundleWorkflowCode>>;
beforeAll(async () => {
	const path = process.env.TEMPORAL_CLI_PATH;
	env = await TestWorkflowEnvironment.createLocal(
		path ? { server: { executable: { type: "existing-path", path } } } : undefined,
	);
	bundle = await bundleWorkflowCode({
		workflowsPath: fileURLToPath(new URL("./tool-report-workflows.ts", import.meta.url)),
	});
}, 60_000);
afterAll(async () => env?.teardown());

it.each([false, true])(
	"preserves native byte decoding, truncation, diagnostics and details when fail=%s",
	async (fail) => {
		const queue = randomUUID();
		const worker = await Worker.create({
			connection: env.nativeConnection,
			taskQueue: queue,
			workflowBundle: bundle,
			activities: {
				...createToolActivities([reportTool], { env: () => new NodeExecutionEnv({ cwd: tmpdir() }) }),
				async piModel(request: ModelRequest) {
					return request.context.messages.some((message) => message.role === "toolResult")
						? fauxAssistantMessage("Done")
						: fauxAssistantMessage(fauxToolCall("report", { fail }), { stopReason: "toolUse" });
				},
			},
		});
		await worker.runUntil(async () => {
			const execute = async (native: boolean, stream = false) => {
				const result = await env.client.workflow.execute("reports", {
					workflowId: randomUUID(),
					taskQueue: queue,
					args: [native, fail, stream],
				});
				const message = result.context.messages.find((message: { role: string }) => message.role === "toolResult");
				return { content: message.content, details: message.details, isError: message.isError };
			};
			const native = await execute(true);
			expect(JSON.stringify(native)).not.toContain("�");
			expect(native.content.some((block: { text: string }) => block.text.includes("reported warning"))).toBe(true);
			expect(await execute(false)).toEqual(native);
			expect(await execute(false, true)).toEqual(native);
		});
	},
	30_000,
);

it("returns only the final attempt's native reports after an activity retry", async () => {
	const queue = randomUUID();
	let attempts = 0;
	const tool = {
		...reportTool,
		execute: async (...args: Parameters<typeof reportTool.execute>) => {
			attempts++;
			return reportTool.execute({ ...args[0], fail: attempts === 1 }, args[1], args[2]);
		},
	};
	const worker = await Worker.create({
		connection: env.nativeConnection,
		taskQueue: queue,
		workflowBundle: bundle,
		activities: {
			...createToolActivities([tool], { env: () => new NodeExecutionEnv({ cwd: tmpdir() }) }),
			async piModel(request: ModelRequest) {
				return request.context.messages.some((message) => message.role === "toolResult")
					? fauxAssistantMessage("Done")
					: fauxAssistantMessage(fauxToolCall("report", { fail: false }), { stopReason: "toolUse" });
			},
		},
	});
	await worker.runUntil(async () => {
		const result = await env.client.workflow.execute("reports", {
			workflowId: queue,
			taskQueue: queue,
			args: [false, false, true, 2],
		});
		const message = result.context.messages.find((message: { role: string }) => message.role === "toolResult");
		expect(attempts).toBe(2);
		expect(message.content[0].text).toBe("three\n");
		expect(message.content.at(-1).text.match(/reported warning/g)).toHaveLength(1);
		expect(message.content.at(-1).text).toContain("2 lines, 12 bytes dropped");
		expect(message.content.at(-1).text).not.toContain("reported failure");
	});
}, 30_000);

it("stops an activity's shell process on cancellation before worker shutdown", async () => {
	const directory = await mkdtemp(join(tmpdir(), "pi-cancel-shell-"));
	const queue = randomUUID();
	let cleanup = false;
	const worker = await Worker.create({
		connection: env.nativeConnection,
		taskQueue: queue,
		workflowBundle: bundle,
		maxHeartbeatThrottleInterval: "100 ms",
		activities: {
			...createToolActivities([createBashTool()], {
				env: () => {
					const execution = new NodeExecutionEnv({ cwd: directory });
					const original = execution.cleanup.bind(execution);
					execution.cleanup = async (context) => {
						await original(context);
						cleanup = true;
					};
					return execution;
				},
			}),
			async piModel() {
				return fauxAssistantMessage(fauxToolCall("bash", { command: "echo $$ > pid; while :; do sleep 1; done" }), {
					stopReason: "toolUse",
				});
			},
		},
	});
	try {
		await worker.runUntil(async () => {
			const handle = await env.client.workflow.start("shellCancellation", { workflowId: queue, taskQueue: queue });
			let pid = 0;
			await expect
				.poll(
					async () => {
						try {
							pid = Number(await readFile(join(directory, "pid"), "utf8"));
							return pid > 0;
						} catch {
							return false;
						}
					},
					{ timeout: 10_000 },
				)
				.toBe(true);
			process.kill(pid, 0);
			await handle.cancel();
			await expect(handle.result()).rejects.toThrow();
			await expect.poll(() => cleanup, { timeout: 10_000 }).toBe(true);
			expect(() => process.kill(pid, 0)).toThrow();
		});
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
}, 30_000);

it("keeps native shell output bounded with external payload storage", async () => {
	const directory = await mkdtemp(join(tmpdir(), "pi-large-shell-"));
	const queue = randomUUID();
	const dataConverter = localDataConverter(join(directory, "payloads"));
	const client = new Client({ connection: env.client.connection, dataConverter });
	let window: unknown;
	const native = createToolActivities([createBashTool()], {
		env: (call) => {
			window = call.outputWindow;
			return new NodeExecutionEnv({ cwd: directory });
		},
	});
	const worker = await Worker.create({
		connection: env.nativeConnection,
		dataConverter,
		taskQueue: queue,
		workflowBundle: bundle,
		activities: {
			...native,
			async piModel(request: ModelRequest) {
				return request.context.messages.some((message) => message.role === "toolResult")
					? fauxAssistantMessage("Done")
					: fauxAssistantMessage(
							fauxToolCall("bash", {
								command: `node -e 'process.stdout.write("🦀line\\n".repeat(400000)+"LAST-LINE\\n")'`,
							}),
							{ stopReason: "toolUse" },
						);
			},
		},
	});
	try {
		await worker.runUntil(async () => {
			const handle = await client.workflow.start("largeShell", { workflowId: queue, taskQueue: queue });
			const result = await handle.result();
			const message = result.context.messages.find((message: { role: string }) => message.role === "toolResult");
			expect(window).toMatchObject({ maxBytes: 50 * 1024, maxLines: 2000 });
			expect(message.content[0].text).toContain("LAST-LINE");
			expect(message.content[0].text).not.toContain("�");
			expect(new TextEncoder().encode(message.content[0].text).byteLength).toBeLessThanOrEqual(50 * 1024);
			expect(message.content.some((block: { text: string }) => block.text.includes("Output truncated"))).toBe(true);
			const history = await handle.fetchHistory();
			const completed = history.events?.find((event) => event.workflowExecutionCompletedEventAttributes);
			const payload = completed?.workflowExecutionCompletedEventAttributes?.result?.payloads?.[0];
			expect(new TextDecoder().decode(payload?.metadata?.messageType)).toBe(
				"temporal.api.sdk.v1.ExternalStorageReference",
			);
		});
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
}, 30_000);
