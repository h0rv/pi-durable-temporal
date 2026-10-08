import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { resolve } from "node:path";

const [command, ...args] = process.argv.slice(2);
const root = resolve(import.meta.dirname, "..");
const checkout = resolve(root, ".local/pi-upstream");
const revision = "7c10bd4337495ee613f2224843ecdf349b80d1df";
const run = (program, argv, cwd = root) =>
	new Promise((done, reject) => {
		const child = spawn(program, argv, { cwd, stdio: "inherit" });
		child.on("error", reject);
		child.on("exit", (code) => (code === 0 ? done() : reject(new Error(`${program} exited with ${code}`))));
	});

if (command === "setup") {
	if (!existsSync(checkout)) {
		await run("git", [
			"clone",
			"--depth",
			"1",
			"--branch",
			"v1.0.4",
			"https://github.com/earendil-works/pi.git",
			checkout,
		]);
	}
	await run("git", ["checkout", "--detach", revision], checkout);
	await run("npm", ["ci", "--ignore-scripts"], checkout);
	await run("npm", ["run", "hydrate:model-data"], checkout);
} else {
	if (!existsSync(resolve(checkout, "node_modules"))) throw new Error("Run npm run example:client:setup first");
	if (!["client", "server", "worker", "test"].includes(command)) throw new Error("Unknown native client command");
	await run(process.execPath, [
		"--import",
		resolve(checkout, "packages/coding-agent/src/experimental/source-resolver.ts"),
		"--import",
		"tsx",
		resolve(root, `examples/native-client/${command}.ts`),
		...args,
	]);
}
