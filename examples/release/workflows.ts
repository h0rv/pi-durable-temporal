import { Type } from "@earendil-works/pi-ai";
import { createRegistry, defineExtension } from "@earendil-works/pi-durable";
import { ApplicationFailure } from "@temporalio/common";
import {
	condition,
	defineQuery,
	defineSignal,
	executeChild,
	proxyActivities,
	setHandler,
	workflowInfo,
} from "@temporalio/workflow";
import { Value } from "typebox/value";
import { createTemporalModels, runTemporalAgent, temporalTool } from "../../src/workflow.js";
import type { createReleaseActivities } from "./activities.js";
import { type AgentRole, models } from "./model.js";

export const approve = defineSignal<[{ approved: boolean }]>("approve");
export const stage = defineQuery<string>("stage");
const reviewSchema = Type.Object({ approved: Type.Boolean(), summary: Type.String(), hash: Type.String() });

export async function changeAgent(input: { role: AgentRole }) {
	const registry = createRegistry();
	const tool =
		input.role === "author"
			? temporalTool({
					name: "writeCandidate",
					description: "Write the proposed source file",
					parameters: Type.Object({ source: Type.String() }),
				})
			: temporalTool(
					{
						name: "runChecks",
						description: "Run the requested review",
						parameters: Type.Object({ kind: Type.Union([Type.Literal("tests"), Type.Literal("security")]) }),
					},
					{ startToCloseTimeout: "10 seconds", retry: { maximumAttempts: 3, initialInterval: "100 ms" } },
				);
	registry.install(defineExtension({ name: "release-tools", tools: [tool] }));
	const result = await runTemporalAgent(
		{ type: "input", content: `Review the invoice total change as ${input.role}.` },
		{
			models: createTemporalModels(models),
			registry,
			agent: {
				model: { provider: "faux", modelId: input.role },
				instructions: "Use your tool. Return JSON with approved and summary.",
			},
			settings: { compaction: { enabled: false } },
		},
	);
	const last = [...result.context.messages].reverse().find((message) => message.role === "assistant");
	const text =
		last?.content
			.filter((block) => block.type === "text")
			.map((block) => block.text)
			.join("") ?? "";
	return Value.Parse(reviewSchema, JSON.parse(text));
}

export async function releaseChange() {
	let currentStage = "writing";
	let approval: boolean | undefined;
	setHandler(stage, () => currentStage);
	setHandler(approve, (decision) => {
		if (currentStage === "awaitingApproval" && approval === undefined) approval = decision.approved;
	});
	const runAgent = (role: AgentRole) =>
		executeChild(changeAgent, {
			workflowId: `${workflowInfo().workflowId}/${role}`,
			args: [{ role }],
		});
	const draft = await runAgent("author");
	currentStage = "reviewing";
	const reviews = await Promise.all([runAgent("tests"), runAgent("security")]);
	if (reviews.some((review) => !review.approved || review.hash !== draft.hash)) {
		currentStage = "rejected";
		throw ApplicationFailure.nonRetryable("A reviewer rejected the change", "ReviewRejected");
	}
	currentStage = "awaitingApproval";
	await condition(() => approval !== undefined);
	if (!approval) {
		currentStage = "rejected";
		return { status: "rejected" as const, reviews };
	}
	currentStage = "publishing";
	const activities = proxyActivities<ReturnType<typeof createReleaseActivities>>({
		startToCloseTimeout: "2 seconds",
		retry: { maximumAttempts: 3, initialInterval: "100 ms" },
	});
	const receipt = await activities.publish(draft.hash);
	currentStage = "done";
	return { status: "published" as const, reviews, receipt };
}
