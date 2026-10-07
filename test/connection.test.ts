import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { connectionSettings } from "../examples/connection.js";

it("configures a remote API key connection with TLS and shared external storage", async () => {
	const settings = await connectionSettings({
		TEMPORAL_ADDRESS: "agents.example.com:7233",
		TEMPORAL_NAMESPACE: "agents.account",
		TEMPORAL_API_KEY: "test-key",
		TEMPORAL_PAYLOAD_BUCKET: "agents-payloads",
		AWS_REGION: "us-east-1",
	});
	expect(settings.connection).toEqual({ address: "agents.example.com:7233", apiKey: "test-key", tls: true });
	expect(settings.namespace).toBe("agents.account");
	expect(settings.dataConverter.externalStorage).toBeDefined();
});
it("loads mutual TLS certificates for an existing service", async () => {
	const directory = await mkdtemp(join(tmpdir(), "pi-cert-"));
	try {
		const cert = join(directory, "cert");
		const key = join(directory, "key");
		await writeFile(cert, "certificate");
		await writeFile(key, "private-key");
		const settings = await connectionSettings({
			TEMPORAL_ADDRESS: "agents.example.com:7233",
			TEMPORAL_PAYLOAD_BUCKET: "agents-payloads",
			TEMPORAL_TLS_CERT: cert,
			TEMPORAL_TLS_KEY: key,
		});
		expect(settings.connection.tls).toEqual({
			clientCertPair: { crt: Buffer.from("certificate"), key: Buffer.from("private-key") },
		});
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});
it.each([
	[{ TEMPORAL_ADDRESS: "agents.example.com:7233" }, "shared TEMPORAL_PAYLOAD_BUCKET"],
	[{ TEMPORAL_TLS_CERT: "cert" }, "both TEMPORAL_TLS_CERT"],
	[{ TEMPORAL_API_KEY: "test-key" }, "Unset TEMPORAL_API_KEY"],
] as const)("rejects incomplete connection settings %s", async (env, message) => {
	await expect(connectionSettings(env)).rejects.toThrow(message);
});
it("connects to a private self-hosted service without TLS when explicitly configured", async () => {
	const settings = await connectionSettings({
		TEMPORAL_ADDRESS: "temporal.internal:7233",
		TEMPORAL_PAYLOAD_BUCKET: "agents-payloads",
		TEMPORAL_TLS: "false",
	});
	expect(settings.connection.tls).toBe(false);
});
