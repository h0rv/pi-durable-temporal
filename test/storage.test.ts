import { mkdtemp, readdir, rm, unlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { StorageDriverClaim } from "@temporalio/common";
import { afterEach, expect, it } from "vitest";
import { FileStorageDriver } from "../examples/storage.js";

const directories: string[] = [];
async function fixture() {
	const directory = await mkdtemp(join(tmpdir(), "pi-temporal-storage-"));
	directories.push(directory);
	return { directory, driver: new FileStorageDriver(directory) };
}
afterEach(async () => {
	await Promise.all(directories.splice(0).map((directory) => rm(directory, { recursive: true, force: true })));
});
const payload = { metadata: { encoding: Buffer.from("json/plain") }, data: Buffer.from('"original session"') };

it("deduplicates concurrent writes and lets a fresh driver recover the original immutable payload", async () => {
	const { directory, driver } = await fixture();
	const claims = await Promise.all(Array.from({ length: 10 }, () => driver.store({}, [payload])));
	expect(await readdir(directory)).toHaveLength(1);
	expect(new Set(claims.map(([claim]) => claim.claimData.key)).size).toBe(1);
	const [restored] = await new FileStorageDriver(directory).retrieve({}, claims[0]);
	expect(Buffer.from(restored.data ?? []).toString()).toBe('"original session"');
});

it("rejects corrupted blobs instead of silently replaying altered conversation state", async () => {
	const { directory, driver } = await fixture();
	const claims = await driver.store({}, [payload]);
	await writeFile(join(directory, claims[0].claimData.key), "different session");
	await expect(driver.retrieve({}, claims)).rejects.toThrow("integrity check");
});

it("fails when a referenced payload is missing", async () => {
	const { directory, driver } = await fixture();
	const claims = await driver.store({}, [payload]);
	await unlink(join(directory, claims[0].claimData.key));
	await expect(driver.retrieve({}, claims)).rejects.toMatchObject({ code: "ENOENT" });
});

it("rejects a claim that attempts to escape the storage directory", async () => {
	const { driver } = await fixture();
	await expect(
		driver.retrieve({}, [new StorageDriverClaim({ key: "../secret", hash_value: "a".repeat(64) })]),
	).rejects.toThrow("Invalid storage claim");
});
