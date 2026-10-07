import { appendFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { createRegistry } from "@earendil-works/pi-durable";
import { NodeExecutionEnv } from "@earendil-works/pi-durable/env/node";
import { CodingTools } from "@earendil-works/pi-durable/tools";
import { Context } from "@temporalio/activity";
import type { Protocol } from "@temporalio/agent-harness-client";
import { ApplicationFailure } from "@temporalio/common";
import { createModelActivities, createToolActivities } from "../../src/index.js";
import { codingActivities, codingDirectory } from "../coding/activities.js";
import { createMcpActivities } from "../mcp/activities.js";
import { exampleModels } from "../providers.js";
import type { ApprovalRequest } from "./protocol.js";
export function createWorkspaceActivities(directory?: string) {
	const registry = createRegistry();
	registry.install(CodingTools);
	const cwd = directory ? resolve(directory) : undefined;
	return createToolActivities(
		registry
			.snapshot()
			.tools()
			.map(({ tool }) => tool),
		{
			env: () => {
				if (!cwd)
					throw ApplicationFailure.nonRetryable(
						"Set PI_WORKSPACE_DIRECTORY on the worker",
						"PiWorkspaceNotConfigured",
					);
				return new NodeExecutionEnv({ cwd });
			},
		},
	);
}

export async function createAgentActivities() {
	const models = await exampleModels();

	const modelActivities = createModelActivities(models);
	return {
		...modelActivities,
		...createMcpActivities(process.env.PI_MCP_DIRECTORY),
		...codingActivities(),
		...createWorkspaceActivities(process.env.PI_WORKSPACE_DIRECTORY),
		async piModel(request: Parameters<typeof modelActivities.piModel>[0]) {
			if (process.env.PI_RECORD_MODEL_CALLS === "true") {
				const { workflowExecution, activityId, attempt } = Context.current().info;
				await appendFile(
					join(await codingDirectory(), "model-calls"),
					`${JSON.stringify({ ...workflowExecution, activityId, attempt })}\n`,
				);
			}
			return modelActivities.piModel(request);
		},
		async evaluateApproval(
			request: ApprovalRequest,
		): Promise<{ verdict: Protocol.AutoApprovalVerdict; reason: string; details: Record<string, unknown> }> {
			const { a, b, operation } = request.tool_input;
			const safe =
				request.tool_name === "calculate" &&
				(operation === "add" || operation === "multiply") &&
				typeof a === "number" &&
				typeof b === "number" &&
				Number.isFinite(a) &&
				Number.isFinite(b) &&
				Math.abs(a) <= 100 &&
				Math.abs(b) <= 100;
			return {
				verdict: safe ? "approve" : "escalate",
				reason: safe ? "Pure arithmetic with operands between -100 and 100" : "This call needs a human decision",
				details: { rule: "bounded-calculator", maximumOperand: 100 },
			};
		},
		async calculate({ operation, a, b }: { operation: "add" | "multiply"; a: number; b: number }) {
			const value = operation === "add" ? a + b : a * b;
			return { content: [{ type: "text" as const, text: String(value) }], details: { value } };
		},
	};
}
