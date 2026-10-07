import { appendFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { NativeConnection, Worker } from "@temporalio/worker";
import { localDataConverter } from "../examples/storage.js";
import type { ModelRequest } from "../src/index.js";
import { answer } from "./model.js";

const [address, queue, directory, bundlePath, block] = process.argv.slice(2);
if (!address || !queue || !directory || !bundlePath) throw new Error("Missing crash-test worker arguments");

async function checkpoint(phase: string) {
	if (block !== phase) return;
	process.send?.({ phase });
	await new Promise<void>(() => undefined); // Parent kills this process at the checkpoint.
}

const connection = await NativeConnection.connect({ address });
try {
	const worker = await Worker.create({
		connection,
		taskQueue: queue,
		workflowBundle: { codePath: bundlePath },
		dataConverter: localDataConverter(join(directory, "payloads")),
		activities: {
			async piModel(request: ModelRequest) {
				await appendFile(join(directory, "model-attempts"), "attempt\n");
				if (request.context.messages.some((message) => message.role === "toolResult")) {
					await checkpoint("model");
					return answer([{ type: "text", text: "The answer is 42." }]);
				}
				return answer([{ type: "toolCall", id: "call-1", name: "double", arguments: { value: 21 } }], "toolUse");
			},
			async double({ value }: { value: number }) {
				await appendFile(join(directory, "tool-attempts"), "attempt\n");
				await checkpoint("before-tool");
				// Record the effect once across activity attempts.
				try {
					await writeFile(join(directory, "effect"), String(value * 2), { flag: "wx" });
				} catch (error) {
					if (!(error instanceof Error && "code" in error && error.code === "EEXIST")) throw error;
				}
				await checkpoint("after-tool");
				return { content: [{ type: "text", text: String(value * 2) }] };
			},
		},
	});
	process.send?.({ phase: "ready" });
	await worker.run();
} finally {
	await connection.close();
}
