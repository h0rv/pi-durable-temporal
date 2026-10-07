import { execFile } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { appendFile, link, mkdir, readFile, unlink, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { promisify } from "node:util";
import { createModels } from "@earendil-works/pi-ai/models";
import { fauxAssistantMessage, fauxProvider, fauxToolCall } from "@earendil-works/pi-ai/providers/faux";
import { ApplicationFailure } from "@temporalio/common";
import { createModelActivities } from "../../src/index.js";
import { roles } from "./model.js";

const run = promisify(execFile);
const source =
	"export function total(items) { return items.reduce((sum, item) => sum + item.price * item.quantity, 0); }\n";

async function createOnce(path: string, content: string): Promise<boolean> {
	const temporary = `${path}.${randomUUID()}.tmp`;
	await writeFile(temporary, content, { flag: "wx" });
	try {
		await link(temporary, path);
		return true;
	} catch (error) {
		if (error instanceof Error && "code" in error && error.code === "EEXIST") return false;
		throw error;
	} finally {
		await unlink(temporary);
	}
}

export function createReleaseActivities(directory: string, afterPublish?: () => Promise<void>) {
	const faux = fauxProvider({ models: roles.map((id) => ({ id })) });
	faux.setResponses(
		Array.from({ length: 32 }, () => async (context, _options, _state, model) => {
			await appendFile(join(directory, "model-calls"), `${model.id}\n`);
			const result = context.messages.find((message) => message.role === "toolResult");
			if (result) {
				return fauxAssistantMessage(
					JSON.stringify({
						approved: !result.isError,
						summary: `${model.id} finished.`,
						hash: result.isError
							? ""
							: JSON.parse(
									result.content
										.filter((block) => block.type === "text")
										.map((block) => block.text)
										.join(""),
								),
					}),
				);
			}
			return fauxAssistantMessage(
				model.id === "author"
					? fauxToolCall("writeCandidate", { source })
					: fauxToolCall("runChecks", { kind: model.id }),
				{ stopReason: "toolUse" },
			);
		}),
	);
	const models = createModels();
	models.setProvider(faux.provider);
	return {
		...createModelActivities(models),
		async writeCandidate({ source: candidate }: { source: string }) {
			await mkdir(directory, { recursive: true });
			await createOnce(join(directory, "candidate.mjs"), candidate);
			await appendFile(join(directory, "drafts"), "written\n");
			return {
				content: [
					{ type: "text" as const, text: JSON.stringify(createHash("sha256").update(candidate).digest("hex")) },
				],
			};
		},
		async runChecks({ kind }: { kind: "tests" | "security" }) {
			await appendFile(join(directory, `${kind}-attempts`), "attempt\n");
			if (kind === "tests" && (await createOnce(join(directory, "test-failure"), "injected")))
				throw new Error("Injected test runner failure");
			if (kind === "tests") {
				const url = pathToFileURL(join(directory, "candidate.mjs")).href;
				await run(
					process.execPath,
					[
						"--input-type=module",
						"--eval",
						`import assert from 'node:assert/strict'; import { total } from ${JSON.stringify(url)}; assert.equal(total([{price:7,quantity:6},{price:8,quantity:1}]),50); assert.equal(total([]),0);`,
					],
					{ timeout: 5000 },
				);
			} else {
				const candidate = await readFile(join(directory, "candidate.mjs"), "utf8");
				if (/\b(eval|exec|fetch)\s*\(/.test(candidate))
					throw new Error("Unexpected external operation in the candidate");
			}
			return {
				content: [
					{
						type: "text" as const,
						text: JSON.stringify(
							createHash("sha256")
								.update(await readFile(join(directory, "candidate.mjs")))
								.digest("hex"),
						),
					},
				],
			};
		},
		async publish(expectedHash: string) {
			await appendFile(join(directory, "publish-attempts"), "attempt\n");
			const candidate = await readFile(join(directory, "candidate.mjs"), "utf8");
			if (createHash("sha256").update(candidate).digest("hex") !== expectedHash)
				throw ApplicationFailure.nonRetryable("Candidate changed after review", "CandidateChanged");
			const path = join(directory, "published.mjs");
			if (await createOnce(path, candidate)) {
				await appendFile(join(directory, "published-events"), "published\n");
				await afterPublish?.();
			}
			return { path };
		},
	};
}
