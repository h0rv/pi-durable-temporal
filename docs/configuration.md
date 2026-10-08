# Configuration

## Store session data outside Temporal

Configure Temporal's [External Storage](https://docs.temporal.io/develop/typescript/best-practices/data-handling/external-storage) on every client and worker. The SDK stores payloads outside Temporal and writes references into history. It retrieves the data before Pi uses it. Storage operations run outside the workflow.

```ts
import { ExternalStorage } from "@temporalio/common";

const dataConverter = {
	externalStorage: new ExternalStorage({
		drivers: [driver],
		payloadSizeThreshold: 0,
	}),
};
// Pass dataConverter to both new Client({ ... }) and Worker.create({ ... }).
```

Use Temporal's S3 or GCS driver in production. [The local example driver](../examples/storage.ts) stores files by their content hash. It writes each file atomically and checks the hash when reading. The TypeScript examples store all payloads outside Temporal.

External Storage is in public preview. Retain stored data for the workflow lifetime and history retention period. Every client and worker needs access to the same storage. Each reference must identify a fixed version of the data. A pointer to the latest session would let replay read different data.

External Storage reduces history size. Pi still holds the context in worker memory. Use Pi compaction to limit that context. Keep workflow runs bounded and use Continue-as-New to limit history event counts. See [state](state.md) for checkpoints and archive growth.

Temporal SDK 1.24 needs a shared `protobufjs` instance for external storage. The package declares 8.8.0 as a peer dependency so npm resolves a shared copy.

## Connect to an existing Temporal service

Your workers run in your infrastructure and poll the remote task queue. The workflow code stays the same. The browser connects to the console server. Only the server and workers receive Temporal and model credentials.

The examples share [connection settings](../examples/connection.ts). For Temporal Cloud, set these values on both the worker and console:

```sh
export TEMPORAL_ADDRESS='<namespace>.<account>.tmprl.cloud:7233'
export TEMPORAL_NAMESPACE='<namespace>.<account>'
export TEMPORAL_API_KEY='<api-key>'
export TEMPORAL_PAYLOAD_BUCKET='<shared-bucket>'
export AWS_REGION='us-east-1'
npm run example:worker
# In another terminal with the same settings:
npm run example:console
```

Use the endpoint from your Temporal service. For mutual TLS, set `TEMPORAL_TLS_CERT` and `TEMPORAL_TLS_KEY` to certificate and key paths instead of setting an API key. For a self-hosted service without TLS, explicitly set `TEMPORAL_TLS=false`. Set `TEMPORAL_TASK_QUEUE` to use another queue. The AWS SDK reads its normal credentials or workload role.

The examples refuse to use local payload files for a remote connection. Temporal's S3 driver stores immutable payloads and checks their hashes. Every worker and client must use the same bucket. Retain those objects while their workflow histories can be replayed. Tests check settings for API keys and mutual TLS. A local worker connected to Temporal Cloud passed model and tool execution, worker restart, Continue-as-New and history replay. That test used local payload files. Shared S3 storage has not been tested.

The example console binds to localhost. Add your application authentication and deploy it behind your own proxy before giving other users access. The package itself does not require the console or S3.

## Limits

- By default, responses arrive after each model activity finishes. Optional progress signals add events to workflow history. Use them for a live client when you need partial output.
- Configure External Storage as shown above. Apply your own redaction and encryption where needed.
- The Agent Harness example continues as new after 20 completed turns. Custom workflows must manage their own lifecycle.
- Hooks and extensions run inside the workflow and must be deterministic. Put external work in activities.
- Pi's browser globals use Temporal's deterministic time and random values. Chord's references stay alive until their tracker is collected, so replay does not depend on garbage collection.
- Pi's default auth loader emits a Webpack warning about a dynamic import. The workflow uses an empty auth context and does not call that loader.
