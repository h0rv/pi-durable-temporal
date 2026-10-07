import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { createModels } from "@earendil-works/pi-ai/models";
import { fauxAssistantMessage, fauxProvider, fauxToolCall } from "@earendil-works/pi-ai/providers/faux";
import { Client, Connection } from "@temporalio/client";
import { NativeConnection, Worker } from "@temporalio/worker";
import { createModelActivities } from "../src/index.js";
import { localDataConverter } from "./storage.js";

// This demo uses a scripted model and needs no API key.
const faux = fauxProvider({ models: [{ id: "demo" }] });
faux.setResponses([
	fauxAssistantMessage(fauxToolCall("double", { value: 21 }), { stopReason: "toolUse" }),
	fauxAssistantMessage("The answer is 42."),
]);
const models = createModels();
models.setProvider(faux.provider);
const connection = await NativeConnection.connect({ address: "localhost:7233" });
const clientConnection = await Connection.connect({ address: "localhost:7233" });
try {
	const dataConverter = localDataConverter();
	const client = new Client({ connection: clientConnection, dataConverter });
	const worker = await Worker.create({
		connection,
		dataConverter,
		taskQueue: "pi-durable-demo",
		workflowsPath: fileURLToPath(new URL("./workflows.ts", import.meta.url)),
		activities: {
			...createModelActivities(models),
			async double({ value }: { value: number }) {
				return { content: [{ type: "text", text: String(value * 2) }] };
			},
		},
	});
	await worker.runUntil(async () => {
		const handle = await client.workflow.start("demo", {
			workflowId: `pi-demo-${randomUUID()}`,
			taskQueue: "pi-durable-demo",
			args: ["Double 21"],
			workflowExecutionTimeout: "1 minute",
		});
		console.log(JSON.stringify(await handle.result(), null, 2));
		console.log(
			`http://localhost:8233/namespaces/default/workflows/${handle.workflowId}/${handle.firstExecutionRunId}/history`,
		);
	});
} finally {
	await clientConnection.close();
	await connection.close();
}
