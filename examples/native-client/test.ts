import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import {
	activateBuiltinClientServices,
	openClientRuntime,
} from "../../.local/pi-upstream/packages/coding-agent/src/experimental/client-runtime.ts";

const first = await openClientRuntime({ command: "client" }, { directory: resolve(".local/pi-sockets") });
const second = await openClientRuntime({ command: "client" }, { directory: resolve(".local/pi-sockets") });
try {
	const a = await activateBuiltinClientServices(first.servers[0]);
	const b = await activateBuiltinClientServices(second.servers[0]);
	const session = await a.management.create({}, BACKGROUND_CONTEXT);
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
	console.log(`Session: ${session.sessionId}\n${result.text}\nTwo upstream clients received the same Pi transcript.`);
} finally {
	await first.dispose();
	await second.dispose();
}
