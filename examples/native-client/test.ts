import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { Client, Connection } from "@temporalio/client";
import {
	activateBuiltinClientServices,
	openClientRuntime,
} from "../../.local/pi-upstream/packages/coding-agent/src/experimental/client-runtime.ts";
import { connectionSettings } from "../connection.js";

const first = await openClientRuntime({ command: "client" }, { directory: resolve(".local/pi-sockets") });
const second = await openClientRuntime({ command: "client" }, { directory: resolve(".local/pi-sockets") });
const settings = await connectionSettings();
const connection = await Connection.connect(settings.connection);
const temporal = new Client({ connection, namespace: settings.namespace, dataConverter: settings.dataConverter });
try {
	const a = await activateBuiltinClientServices(first.servers[0]);
	const b = await activateBuiltinClientServices(second.servers[0]);
	const session = await a.management.create({}, BACKGROUND_CONTEXT);
	const workflow = temporal.workflow.getHandle(session.sessionId);
	const firstRun = (await workflow.describe()).runId;
	for (const peer of [a, b]) {
		await peer.plugins.prepareSession({ sessionId: session.sessionId, packagePaths: null }, BACKGROUND_CONTEXT);
		await peer.management.attach(session.sessionId, BACKGROUND_CONTEXT);
	}
	const accepted = await a.agent.prompt(
		{
			message:
				'Use write to create native-client.txt containing exactly "Hello from Pi on Temporal" with no punctuation or newline. Then use read to read it back.',
			images: null,
		},
		BACKGROUND_CONTEXT,
	);
	assert(accepted.accepted);
	const result = await b.agent.waitForPrompt(accepted.operationId, BACKGROUND_CONTEXT);
	assert.equal(result.status, "done");
	await workflow.signal("piContinueSession");
	for (let attempt = 0; (await workflow.describe()).runId === firstRun; attempt++) {
		assert(attempt < 100, "Session did not continue as new");
		await new Promise((done) => setTimeout(done, 100));
	}
	assert.deepEqual(await a.agent.waitForPrompt(accepted.operationId, BACKGROUND_CONTEXT), result);
	const history = await workflow.fetchHistory();
	const started = history.events?.find((event) => event.workflowExecutionStartedEventAttributes);
	assert(
		started?.workflowExecutionStartedEventAttributes?.input?.payloads?.every(
			(payload) =>
				Buffer.from(payload.metadata?.messageType ?? []).toString() ===
				"temporal.api.sdk.v1.ExternalStorageReference",
		),
		"Rollover inputs must use external payload references",
	);
	for (let attempt = 0; attempt < 100; attempt++) {
		if (JSON.stringify(a.transcript.state.value) === JSON.stringify(b.transcript.state.value)) break;
		await new Promise((done) => setTimeout(done, 100));
	}
	assert.deepEqual(a.transcript.state.value, b.transcript.state.value);
	assert(a.transcript.state.value?.entries.length);
	assert.equal(
		(
			await readFile(resolve(process.env.PI_WORKSPACE_DIRECTORY ?? process.cwd(), "native-client.txt"), "utf8")
		).trim(),
		"Hello from Pi on Temporal",
	);
	console.log(
		`Session: ${session.sessionId}\n${result.text}\nTwo upstream clients retained the same Pi transcript across Continue-as-New.`,
	);
} finally {
	await first.dispose();
	await second.dispose();
	await connection.close();
}
