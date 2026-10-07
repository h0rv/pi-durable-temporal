import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { Client, Connection } from "@temporalio/client";
import { bundleWorkflowCode, Worker } from "@temporalio/worker";
import { localDataConverter } from "../storage.js";

const session = `pi-durable-e2e-${randomUUID()}`;
const consoleUrl = "http://localhost:8000";
const headers = { "Content-Type": "application/json" };
const created = await fetch(`${consoleUrl}/api/sessions`, {
	method: "POST",
	headers,
	body: JSON.stringify({ agent_workflow_type: "piSession", session_id: session, data: { approvalMode: "auto" } }),
	signal: AbortSignal.timeout(15_000),
});
assert(created.ok, `Create session: ${created.status} ${await created.text()}`);
const response = await fetch(`${consoleUrl}/api/chat`, {
	method: "POST",
	headers,
	body: JSON.stringify({
		session_id: session,
		message: {
			type: "ask",
			payload: {
				text: "Use the calculate tool to multiply 7 by 6, then add 8 to the result. Tell me the final answer.",
			},
		},
	}),
	signal: AbortSignal.timeout(90_000),
});
assert(response.ok, `Chat request: ${response.status}`);
const trace = await response.text();
assert(!/^event: (stream_error|message_handler_error)$/m.test(trace), trace);
assert.equal(trace.match(/^event: tool_end$/gm)?.length, 2, trace);
assert.match(trace, /"output":\s*\{"text":\s*"[^"]*\b50\b[^"]*"\}/);
assert.match(trace, /^event: turn_end$/m);

const archived = await fetch(`${consoleUrl}/api/attach?session_id=${session}&from_offset=0`, {
	signal: AbortSignal.timeout(15_000),
});
assert(archived.ok, `Read archived trace: ${archived.status}`);
assert((await archived.text()).endsWith(trace), "The console must retain every turn event after it finishes");

const connection = await Connection.connect({ address: "localhost:7233" });
try {
	const dataConverter = localDataConverter();
	const client = new Client({ connection, dataConverter });
	const parent = client.workflow.getHandle(session);
	const parentHistory = await parent.fetchHistory();
	const child = parentHistory.events?.find((event) => event.childWorkflowExecutionStartedEventAttributes)
		?.childWorkflowExecutionStartedEventAttributes?.workflowExecution;
	assert(child?.workflowId && child.runId, "The session must start a Pi child workflow");
	const history = await client.workflow.getHandle(child.workflowId, child.runId).fetchHistory();
	const modelInput = history.events?.find(
		(event) => event.activityTaskScheduledEventAttributes?.activityType?.name === "piModel",
	)?.activityTaskScheduledEventAttributes?.input?.payloads?.[0];
	assert(
		Buffer.from(modelInput?.metadata?.messageType ?? [])
			.toString()
			.endsWith("ExternalStorageReference"),
		"Model requests must be stored as external references",
	);
	const finalResult: unknown = await client.workflow.getHandle(child.workflowId, child.runId).result();
	assert(
		typeof finalResult === "object" && finalResult !== null && !("context" in finalResult),
		"Do not return the full transcript from the child workflow",
	);
	const names = history.events?.flatMap(
		(event) => event.activityTaskScheduledEventAttributes?.activityType?.name ?? [],
	);
	assert.deepEqual(names, [
		"piModel",
		"evaluateApproval",
		"calculate",
		"piModel",
		"evaluateApproval",
		"calculate",
		"piModel",
	]);
	const bundle = await bundleWorkflowCode({
		workflowsPath: fileURLToPath(new URL("./workflows.ts", import.meta.url)),
	});
	await Worker.runReplayHistory({ workflowBundle: bundle, dataConverter }, history);
	console.log(`Trace: ${consoleUrl}/?s=${session}`);
	console.log(
		`History: http://localhost:8233/namespaces/default/workflows/${encodeURIComponent(child.workflowId)}/${child.runId}/history`,
	);
	console.log("Passed: real provider, two tools, archived trace and deterministic history replay.");
} finally {
	await connection.close();
}
