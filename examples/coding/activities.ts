import { createHash, randomUUID } from "node:crypto";
import { appendFile, link, mkdir, readFile, rename, unlink, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { Context } from "@temporalio/activity";
import { ApplicationFailure } from "@temporalio/common";
import { checkSource } from "./checks.js";

const initial = "export function total(items) { return items.reduce((sum, item) => sum + item.price, 0); }\n";
const hash = (source: string) => createHash("sha256").update(source).digest("hex");
const content = (value: unknown) => ({ content: [{ type: "text" as const, text: JSON.stringify(value) }] });
async function once(path: string, value: string) {
	const temporary = `${path}.${randomUUID()}.tmp`;
	await writeFile(temporary, value, { flag: "wx" });
	try {
		await link(temporary, path);
		return true;
	} catch (error) {
		if (error instanceof Error && "code" in error && error.code === "EEXIST") {
			if ((await readFile(path, "utf8")) !== value) throw new Error("Existing artifact has different content");
			return false;
		}
		throw error;
	} finally {
		await unlink(temporary);
	}
}
export async function codingDirectory() {
	const workflow = Context.current().info.workflowExecution?.workflowId;
	if (!workflow) throw new Error("Coding tools require a workflow");
	const directory = join(
		resolve(process.env.PI_CODING_DIRECTORY ?? ".local/coding"),
		hash(workflow.split("/turn-")[0]),
	);
	await mkdir(directory, { recursive: true });
	return directory;
}
export function codingActivities() {
	return {
		async readCandidate() {
			const directory = await codingDirectory();
			const source = await readFile(join(directory, "candidate.mjs"), "utf8").catch((error) => {
				if (error.code === "ENOENT") return initial;
				throw error;
			});
			return content({ source, hash: hash(source) });
		},
		async writeCandidate({ source }: { source: string }) {
			if (source.length > 8000) throw ApplicationFailure.nonRetryable("Source is too large");
			const directory = await codingDirectory();
			const temporary = join(directory, `${randomUUID()}.tmp`);
			await writeFile(temporary, source);
			await rename(temporary, join(directory, "candidate.mjs"));
			return content({ hash: hash(source) });
		},
		async runTests() {
			const directory = await codingDirectory();
			if (await once(join(directory, "test-runner-failure"), "injected"))
				throw new Error("Injected test runner outage");
			const source = await readFile(join(directory, "candidate.mjs"), "utf8");
			const result = checkSource(source);
			await once(join(directory, `${hash(source)}.passed`), "passed");
			return content({ ...result, hash: hash(source) });
		},
		async publishCandidate({ hash: expected }: { hash: string }) {
			const directory = await codingDirectory();
			const source = await readFile(join(directory, "candidate.mjs"), "utf8");
			if (hash(source) !== expected) throw ApplicationFailure.nonRetryable("Candidate changed after approval");
			await readFile(join(directory, `${expected}.passed`));
			checkSource(source);
			if (await once(join(directory, "published.mjs"), source)) {
				await appendFile(join(directory, "publications"), `${expected}\n`);
				if (process.env.PI_PAUSE_AFTER_PUBLISH === "true") {
					process.send?.({ phase: "published", directory });
					await new Promise<void>(() => undefined);
				}
			}
			return content({ hash: expected, path: join(directory, "published.mjs") });
		},
	};
}
