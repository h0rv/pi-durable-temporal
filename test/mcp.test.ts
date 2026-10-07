import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Context } from "@temporalio/activity";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { bundleWorkflowCode, Worker } from "@temporalio/worker";
import { expect, it } from "vitest";
import { createMcpActivities, prepareMcpDirectory } from "../examples/mcp/activities.js";
import { exampleModels } from "../examples/providers.js";
import { createModelActivities } from "../src/index.js";

it("retries code mode after MCP reads and reuses completed results after worker replacement", async () => {
	const directory = await mkdtemp(join(tmpdir(), "pi-mcp-"));
	const path = process.env.TEMPORAL_CLI_PATH;
	const env = await TestWorkflowEnvironment.createLocal(
		path ? { server: { executable: { type: "existing-path", path } } } : undefined,
	);
	try {
		const bundle = await bundleWorkflowCode({
			workflowsPath: fileURLToPath(new URL("../examples/mcp/workflows.ts", import.meta.url)),
		});
		const mcp = createMcpActivities(await prepareMcpDirectory(directory));
		let attempts = 0;
		let reads = 0;
		const activities = {
			...createModelActivities(await exampleModels()),
			async mcpReadInventory() {
				reads++;
				return mcp.mcpReadInventory();
			},
			async codemode(args: { code: string }) {
				attempts++;
				const result = await mcp.codemode(args);
				if (Context.current().info.attempt === 1) throw new Error("Injected failure after MCP read");
				return result;
			},
		};
		const queue = randomUUID();
		const worker = () =>
			Worker.create({ connection: env.nativeConnection, taskQueue: queue, workflowBundle: bundle, activities });
		const handle = await env.client.workflow.start("mcpAgent", {
			workflowId: queue,
			taskQueue: queue,
			workflowExecutionTimeout: "30 seconds",
		});
		await (await worker()).runUntil(async () => {
			for (let count = 0; count < 150; count++) {
				try {
					if (await handle.query("ready")) return;
				} catch {}
				await new Promise((resolve) => setTimeout(resolve, 50));
			}
			throw new Error("Code mode did not finish");
		});
		expect(attempts).toBe(2);
		expect(reads).toBe(1);
		await (await worker()).runUntil(async () => {
			await handle.signal("release");
			const result = await handle.result();
			expect(result.text).toContain("460");
			expect(attempts).toBe(2);
			expect(reads).toBe(1);
			await Worker.runReplayHistory({ workflowBundle: bundle }, await handle.fetchHistory(), queue);
		});
	} finally {
		await env.teardown();
		await rm(directory, { recursive: true, force: true });
	}
}, 60_000);
