import { type ChildProcess, spawn } from "node:child_process";
import { mkdir } from "node:fs/promises";
import { connect } from "node:net";
import { setTimeout } from "node:timers/promises";
import { fileURLToPath } from "node:url";

const children: ChildProcess[] = [];
const env = {
	...process.env,
	TEMPORAL_ADDRESS: "localhost:7233",
	TEMPORAL_NAMESPACE: "default",
	TEMPORAL_API_KEY: "",
	TEMPORAL_TLS: "false",
};
const listening = () =>
	new Promise<boolean>((resolve) => {
		const socket = connect(7233, "127.0.0.1");
		socket.once("connect", () => {
			socket.destroy();
			resolve(true);
		});
		socket.once("error", () => resolve(false));
	});
const launch = (command: string, args: string[]) => {
	const child = spawn(command, args, { env, stdio: "inherit" });
	children.push(child);
	child.once("error", (error) => {
		console.error(error.message);
		shutdown();
	});
	child.once("exit", (code) => {
		if (code) shutdown();
	});
	return child;
};
function shutdown() {
	for (const child of children) child.kill("SIGTERM");
}
process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);
await mkdir(".local", { recursive: true });
if (!(await listening())) {
	launch("temporal", ["server", "start-dev", "--ip", "127.0.0.1", "--db-filename", ".local/temporal.sqlite"]);
	for (let i = 0; !(await listening()); i++) {
		if (i >= 60) {
			shutdown();
			throw new Error("Temporal did not start");
		}
		await setTimeout(500);
	}
}
launch(process.execPath, ["--import", "tsx", fileURLToPath(new URL("./agent-harness/worker.ts", import.meta.url))]);
launch(process.execPath, ["--import", "tsx", fileURLToPath(new URL("./agent-harness/console.ts", import.meta.url))]);
console.log(
	`Open http://localhost:${process.env.PI_CONSOLE_PORT ?? 8000}. The default calculator needs no model credentials.`,
);
