import { mkdir } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { NativeConnection, Worker } from "@temporalio/worker";
import { localDataConverter } from "../storage.js";
import { createReleaseActivities } from "./activities.js";

const [address = "localhost:7233", taskQueue = "pi-release", directory = ".local/release", bundlePath, block] =
	process.argv.slice(2);
const root = resolve(directory);
await mkdir(root, { recursive: true });
const connection = await NativeConnection.connect({ address });
try {
	const worker = await Worker.create({
		connection,
		taskQueue,
		dataConverter: localDataConverter(`${root}/payloads`),
		...(bundlePath
			? { workflowBundle: { codePath: bundlePath } }
			: { workflowsPath: fileURLToPath(new URL("./workflows.ts", import.meta.url)) }),
		activities: createReleaseActivities(
			root,
			block === "publish"
				? async () => {
						process.send?.({ phase: "published" });
						await new Promise<void>(() => undefined);
					}
				: undefined,
		),
	});
	process.send?.({ phase: "ready" });
	await worker.run();
} finally {
	await connection.close();
}
