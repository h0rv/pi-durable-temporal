import { fileURLToPath } from "node:url";
import { NativeConnection, Worker } from "@temporalio/worker";
import { connectionSettings } from "../connection.js";
import { createAgentActivities } from "./activities.js";

const settings = await connectionSettings();
const connection = await NativeConnection.connect(settings.connection);
try {
	const worker = await Worker.create({
		connection,
		namespace: settings.namespace,
		dataConverter: settings.dataConverter,
		taskQueue: settings.taskQueue,
		workflowsPath: fileURLToPath(new URL("./session.ts", import.meta.url)),
		activities: await createAgentActivities(),
	});
	process.send?.({ phase: "ready" });
	await worker.run();
} finally {
	await connection.close();
}
