import { randomUUID } from "node:crypto";
import { setTimeout } from "node:timers/promises";
import { HttpTransport } from "@temporalio/agent-harness-client";
import { Client, Connection } from "@temporalio/client";
import { connectionSettings } from "../connection.js";
import { status } from "./protocol.js";

const settings = await connectionSettings();
const connection = await Connection.connect(settings.connection);
try {
	const client = new Client({ connection, namespace: settings.namespace, dataConverter: settings.dataConverter });
	const transport = new HttpTransport({ baseUrl: "http://localhost:8000/api/" });
	const id = `pi-human-approval-${randomUUID()}`;
	await transport.createSession({
		agent_workflow_type: "piSession",
		session_id: id,
		data: { approvalMode: "manual" },
	});
	await transport.submitMessage(id, {
		type: "ask",
		payload: { text: "Use calculate to multiply 7 by 6. Give the result." },
	});
	console.log(`Human approval: http://localhost:8000/?s=${id}`);
	for (let attempt = 0; attempt < 120; attempt++) {
		const snapshot = await client.workflow.getHandle(id).query(status);
		if (snapshot.pending_approvals.length) {
			console.log("The tool is waiting. Open the session and approve or deny its pending tool card.");
			break;
		}
		if (attempt === 119) throw new Error("The agent did not request approval within 60 seconds");
		await setTimeout(500);
	}
} finally {
	await connection.close();
}
