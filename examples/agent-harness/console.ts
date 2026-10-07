import { Client, Connection } from "@temporalio/client";
import { connectionSettings } from "../connection.js";
import { createConsole } from "./server.js";
import { installUi } from "./ui.js";

const settings = await connectionSettings();
const connection = await Connection.connect(settings.connection);
const client = new Client({ connection, namespace: settings.namespace, dataConverter: settings.dataConverter });
const server = createConsole(client, await installUi(), settings.taskQueue);
server.listen(Number(process.env.PI_CONSOLE_PORT ?? 8000), "127.0.0.1", () =>
	console.log(`Agent Harness console: http://localhost:${process.env.PI_CONSOLE_PORT ?? 8000}`),
);
async function shutdown() {
	server.closeAllConnections();
	server.close();
	await connection.close();
}
process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);
