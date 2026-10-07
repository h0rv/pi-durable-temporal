import { createHash, randomUUID } from "node:crypto";
import { link, mkdir, readFile, unlink, writeFile } from "node:fs/promises";
import { join } from "node:path";
import {
	ExternalStorage,
	type Payload,
	type StorageDriver,
	StorageDriverClaim,
	type StorageDriverRetrieveContext,
	type StorageDriverStoreContext,
} from "@temporalio/common";
import proto from "@temporalio/proto";

const { temporal } = proto;

/** Store immutable payloads and check their content hashes on retrieval. */
export class FileStorageDriver implements StorageDriver {
	readonly name = "local-file";
	readonly type = "local.filedriver";
	private readonly directory: string;
	constructor(directory: string) {
		this.directory = directory;
	}

	async store(context: StorageDriverStoreContext, payloads: Payload[]): Promise<StorageDriverClaim[]> {
		await mkdir(this.directory, { recursive: true, mode: 0o700 });
		return Promise.all(
			payloads.map(async (payload) => {
				context.abortSignal?.throwIfAborted();
				const raw = temporal.api.common.v1.Payload.encode(payload).finish();
				const digest = createHash("sha256").update(raw).digest("hex");
				const key = `${digest}.bin`;
				const temporary = join(this.directory, `${key}.${randomUUID()}.tmp`);
				try {
					await writeFile(temporary, raw, { flag: "wx", mode: 0o600 });
					// Readers must see a complete file. Existing files stay unchanged.
					try {
						await link(temporary, join(this.directory, key));
					} catch (error) {
						if (!(error instanceof Error && "code" in error && error.code === "EEXIST")) throw error;
					}
				} finally {
					await unlink(temporary).catch(() => undefined);
				}
				context.abortSignal?.throwIfAborted();
				return new StorageDriverClaim({ key, hash_algorithm: "sha256", hash_value: digest });
			}),
		);
	}

	async retrieve(context: StorageDriverRetrieveContext, claims: StorageDriverClaim[]): Promise<Payload[]> {
		return Promise.all(
			claims.map(async ({ claimData }) => {
				context.abortSignal?.throwIfAborted();
				const { key, hash_value: digest } = claimData;
				if (!digest || !/^[a-f0-9]{64}$/.test(digest) || key !== `${digest}.bin`)
					throw new Error("Invalid storage claim");
				const raw = await readFile(join(this.directory, key));
				if (createHash("sha256").update(raw).digest("hex") !== digest)
					throw new Error("Stored payload failed its integrity check");
				context.abortSignal?.throwIfAborted();
				return temporal.api.common.v1.Payload.decode(raw);
			}),
		);
	}
}

export function localDataConverter(directory = "/tmp/temporal-large-payloads") {
	return {
		externalStorage: new ExternalStorage({ drivers: [new FileStorageDriver(directory)], payloadSizeThreshold: 0 }),
	};
}
