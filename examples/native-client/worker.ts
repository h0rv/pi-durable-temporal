import { resolve } from "node:path";
import { NativeConnection, Worker } from "@temporalio/worker";
import { createModelActivities } from "../../src/index.js";
import { createWorkspaceActivities } from "../agent-harness/activities.js";
import { connectionSettings } from "../connection.js";
import { exampleModels } from "../providers.js";

const settings = await connectionSettings();
const connection = await NativeConnection.connect(settings.connection);
try {
	const worker = await Worker.create({
		connection,
		namespace: settings.namespace,
		dataConverter: settings.dataConverter,
		taskQueue: process.env.PI_CLIENT_TASK_QUEUE ?? "pi-native-client",
		workflowsPath: resolve("examples/native-client/workflows.ts"),
		bundlerOptions: {
			webpackConfigHook(config) {
				config.resolve ??= {};
				config.resolve.modules = [resolve("node_modules"), "node_modules"];
				return config;
			},
		},
		activities: {
			...createModelActivities(await exampleModels()),
			...createWorkspaceActivities(process.env.PI_WORKSPACE_DIRECTORY ?? process.cwd()),
		},
	});
	await worker.run();
} finally {
	await connection.close();
}
