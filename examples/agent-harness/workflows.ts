import { Type } from "@earendil-works/pi-ai";
import { createRegistry, defineExtension } from "@earendil-works/pi-durable";
import { CodingTools } from "@earendil-works/pi-durable/tools";
import type { Protocol } from "@temporalio/agent-harness-client";
import { getExternalWorkflowHandle, patched, proxyActivities } from "@temporalio/workflow";
import { createAgentTrace } from "../../src/agent-harness.js";
import {
	createTemporalModels,
	type PiCheckpoint,
	runTemporalAgent,
	runTemporalTurn,
	temporalTool,
} from "../../src/workflow.js";
import { model as scriptedModel } from "../model.js";
import type { createAgentActivities } from "./activities.js";
import { model } from "./model.js";
import type { SessionOptions } from "./protocol.js";

export async function piAgent(input: {
	prompt: string;
	parentWorkflowId: string;
	options?: SessionOptions;
	allowedTools?: string[];
	state?: PiCheckpoint;
}) {
	const selectedModel = patched("configurable-model-v1") ? (input.options?.model ?? scriptedModel) : model;
	const parent = getExternalWorkflowHandle(input.parentWorkflowId);
	const publish = (event: Protocol.AgentStreamItem) => parent.signal("pi_trace", event);
	const approvalActivity = proxyActivities<Awaited<ReturnType<typeof createAgentActivities>>>({
		startToCloseTimeout: "10 seconds",
		retry: { maximumAttempts: 1 },
	});
	const registry = createRegistry();
	if (input.options?.task === "workspace") {
		const coding = createRegistry();
		coding.install(CodingTools);
		registry.install(
			defineExtension({
				name: CodingTools.name,
				tools: coding
					.snapshot()
					.tools()
					.map(({ tool }) => temporalTool(tool)),
			}),
		);
	}
	if (input.options?.task === "mcp")
		registry.install(
			defineExtension({
				name: "mcp",
				tools: [
					temporalTool(
						{
							name: "mcpReadInventory",
							description: "Read the sample inventory through MCP",
							parameters: Type.Object({}),
						},
						{ retry: { maximumAttempts: 3, initialInterval: "100 ms" } },
					),
					temporalTool(
						{
							name: "codemode",
							description:
								"Run JavaScript in Pi's code mode sandbox. Call tools.read_inventory({}) to read inventory JSON through MCP. Use text() to report results. Only this read-only tool is available.",
							parameters: Type.Object({ code: Type.String({ maxLength: 16000 }) }),
						},
						{ retry: { maximumAttempts: 3, initialInterval: "100 ms" } },
					),
				],
			}),
		);
	if (input.options?.task !== "workspace")
		registry.install(
			defineExtension({
				name: "calculator",
				tools: [
					temporalTool(
						{
							name: "calculate",
							description: "Add or multiply two numbers. Use this for all arithmetic.",
							parameters: Type.Object({
								operation: Type.Union([Type.Literal("add"), Type.Literal("multiply")]),
								a: Type.Number(),
								b: Type.Number(),
							}),
						},
						{ startToCloseTimeout: "30 seconds", retry: { maximumAttempts: 3 } },
					),
				],
			}),
		);
	if (input.options?.task === "coding")
		registry.install(
			defineExtension({
				name: "coding",
				tools: [
					temporalTool({
						name: "readCandidate",
						description: "Read the current source and its hash",
						parameters: Type.Object({}),
					}),
					temporalTool({
						name: "writeCandidate",
						description:
							"Write a candidate. Export function total(items) using a single return items.reduce((sum,item) => expression,0). The expression may only use addition and multiplication on sum, item.price and item.quantity.",
						parameters: Type.Object({ source: Type.String({ maxLength: 8000 }) }),
					}),
					temporalTool(
						{
							name: "runTests",
							description:
								"Check the candidate without executing JavaScript. Return passing tests and source hash.",
							parameters: Type.Object({}),
						},
						{ startToCloseTimeout: "10 seconds", retry: { maximumAttempts: 3, initialInterval: "100 ms" } },
					),
					temporalTool(
						{
							name: "publishCandidate",
							description: "Publish the tested source hash. Requires human approval.",
							parameters: Type.Object({ hash: Type.String({ pattern: "^[a-f0-9]{64}$" }) }),
						},
						{ startToCloseTimeout: "3 seconds", retry: { maximumAttempts: 3, initialInterval: "100 ms" } },
					),
				],
			}),
		);
	registry.install(
		createAgentTrace({
			model: selectedModel.id,
			publish,
			approvalMode: input.options?.approvalMode,
			allowedTools: input.allowedTools,
			evaluate: approvalActivity.evaluateApproval,
			evaluator: "bounded-calculator",
		}).extension,
	);
	const persistent = patched("persistent-pi-state-v1");
	const run = persistent ? runTemporalTurn : runTemporalAgent;
	const result = await run(
		{ type: "input", content: input.prompt },
		{
			models: createTemporalModels([selectedModel]),
			registry,
			agent: {
				model: { provider: selectedModel.provider, modelId: selectedModel.id },
				thinkingLevel: "off",
				instructions:
					input.options?.task === "workspace"
						? "You are a concise coding assistant. Use read, write, edit and bash to work in the worker workspace. Inspect relevant files before changing them. Run checks appropriate to the change and report their results."
						: input.options?.task === "mcp"
							? "Read the inventory with mcpReadInventory, then use codemode to calculate price times quantity for every record and report the total. Give a short final reply."
							: input.options?.task === "coding"
								? "You are fixing a small JavaScript module. Read the candidate, fix the bug, run its tests, then publish the tested hash. Use each tool serially. Do not publish until tests pass. The supported source grammar is described by writeCandidate. Give a short final reply."
								: "Use the calculate tool for arithmetic. Follow the user's steps and give a concise answer.",
			},
			settings: { compaction: { enabled: false } },
		},
		input.state,
	);
	if ("state" in result) return result;
	const last = [...result.context.messages].reverse().find((message) => message.role === "assistant");
	const text =
		last?.content
			.filter((block) => block.type === "text")
			.map((block) => block.text)
			.join("\n") ?? "";
	return { text, usage: result.usage, state: undefined };
}
