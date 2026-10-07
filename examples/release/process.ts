import { type ChildProcess, spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

export function startWorker(address: string, queue: string, directory: string, bundlePath: string, block = "") {
	return spawn(
		process.execPath,
		[
			"--import",
			"tsx",
			fileURLToPath(new URL("./worker.ts", import.meta.url)),
			address,
			queue,
			directory,
			bundlePath,
			block,
		],
		{ stdio: ["ignore", "pipe", "pipe", "ipc"] },
	);
}
export function waitFor(child: ChildProcess, phase: string, timeoutMs = 20_000) {
	return new Promise<void>((resolve, reject) => {
		let output = "";
		const capture = (chunk: Buffer) => {
			output += chunk.toString();
		};
		const cleanup = () => {
			clearTimeout(timer);
			child.off("message", message);
			child.off("exit", exited);
			child.off("error", failed);
			child.stdout?.off("data", capture);
			child.stderr?.off("data", capture);
		};
		const failed = (error: Error) => {
			cleanup();
			reject(error);
		};
		const message = (value: unknown) => {
			if (typeof value === "object" && value !== null && "phase" in value && value.phase === phase) {
				cleanup();
				resolve();
			}
		};
		const exited = () => failed(new Error(`Worker exited before ${phase}: ${output}`));
		const timer =
			timeoutMs === 0
				? undefined
				: setTimeout(() => failed(new Error(`Worker did not reach ${phase}: ${output}`)), timeoutMs);
		child.stdout?.on("data", capture);
		child.stderr?.on("data", capture);
		child.on("message", message);
		child.once("exit", exited);
		child.once("error", failed);
	});
}
export async function stop(child: ChildProcess) {
	if (child.exitCode !== null || child.signalCode !== null) return;
	await new Promise<void>((resolve) => {
		child.once("exit", () => resolve());
		child.kill("SIGKILL");
	});
}
