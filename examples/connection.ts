import { readFile } from "node:fs/promises";
import { S3Client } from "@aws-sdk/client-s3";
import type { ConnectionOptions } from "@temporalio/client";
import { type DataConverter, ExternalStorage } from "@temporalio/common";
import { S3StorageDriver } from "@temporalio/external-storage-s3";
import { AwsSdkS3StorageDriverClient } from "@temporalio/external-storage-s3-aws-sdk";
import { localDataConverter } from "./storage.js";

export async function connectionSettings(env: NodeJS.ProcessEnv = process.env) {
	const address = env.TEMPORAL_ADDRESS ?? "localhost:7233";
	const namespace = env.TEMPORAL_NAMESPACE ?? "default";
	const apiKey = env.TEMPORAL_API_KEY || undefined;
	const cert = env.TEMPORAL_TLS_CERT;
	const key = env.TEMPORAL_TLS_KEY;
	if (Boolean(cert) !== Boolean(key)) throw new Error("Set both TEMPORAL_TLS_CERT and TEMPORAL_TLS_KEY");
	const local = /^(localhost|127\.0\.0\.1):/.test(address);
	if (local && apiKey) throw new Error("Unset TEMPORAL_API_KEY when connecting to the local server");
	const tls =
		cert && key
			? { clientCertPair: { crt: await readFile(cert), key: await readFile(key) } }
			: env.TEMPORAL_TLS === "false"
				? false
				: env.TEMPORAL_TLS === "true" || Boolean(apiKey) || !local;
	const connection = { address, apiKey, tls } satisfies ConnectionOptions;
	const bucket = env.TEMPORAL_PAYLOAD_BUCKET;
	if (!local && !bucket) throw new Error("Remote workers need a shared TEMPORAL_PAYLOAD_BUCKET");
	const dataConverter: DataConverter = bucket
		? {
				externalStorage: new ExternalStorage({
					drivers: [
						new S3StorageDriver({
							client: new AwsSdkS3StorageDriverClient(new S3Client({ region: env.AWS_REGION })),
							bucket,
						}),
					],
					payloadSizeThreshold: 0,
				}),
			}
		: localDataConverter(env.TEMPORAL_PAYLOAD_DIRECTORY);
	return { connection, namespace, dataConverter, taskQueue: env.TEMPORAL_TASK_QUEUE ?? "pi-durable" };
}
