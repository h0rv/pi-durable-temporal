import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai/providers/faux";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { Worker } from "@temporalio/worker";
import { expect, it } from "vitest";
import type { ModelRequest } from "../src/types.js";
import type { traceOnly } from "./trace-workflows.js";

it("traces tools without adding an approval policy", async () => {
	const path = process.env.TEMPORAL_CLI_PATH;
	const env = await TestWorkflowEnvironment.createLocal(
		path ? { server: { executable: { type: "existing-path", path } } } : undefined,
	);
	let effects = 0;
	try {
		const taskQueue = randomUUID();
		const worker = await Worker.create({
			connection: env.nativeConnection,
			taskQueue,
			workflowsPath: fileURLToPath(new URL("./trace-workflows.ts", import.meta.url)),
			activities: {
				async piModel(request: ModelRequest) {
					return request.context.messages.some((message) => message.role === "toolResult")
						? fauxAssistantMessage("12")
						: fauxAssistantMessage(fauxToolCall("double", { value: 6 }), { stopReason: "toolUse" });
				},
				async double({ value }: { value: number }) {
					effects++;
					return { content: [{ type: "text", text: String(value * 2) }] };
				},
			},
		});
		await worker.runUntil(async () => {
			const result = await env.client.workflow.execute<typeof traceOnly>("traceOnly", {
				workflowId: randomUUID(),
				taskQueue,
				workflowExecutionTimeout: "10 seconds",
			});
			expect(effects).toBe(1);
			expect(result.events.some((event) => event.type === "tool_end")).toBe(true);
			expect(result.events.some((event) => event.type === "tool_approval_requested")).toBe(false);
		});
	} finally {
		await env.teardown();
	}
}, 60_000);
