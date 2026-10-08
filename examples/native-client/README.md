# Pi client on Temporal

Uses Pi 1.0.4's upstream experimental client TUI without changes. Its `AgentController` calls run in a Temporal workflow. `Transcript` publishes Pi's conversation view. Model requests and coding tools run as activities.

The client is source-only upstream. From a checkout of this repository, setup checks out a pinned Pi revision and installs its dependencies:

```sh
npm ci --ignore-scripts
npm run example:client:setup
```

With Temporal running on localhost:7233, start these in separate terminals:

```sh
PI_PROVIDER=codex npm run example:client:worker
PI_PROVIDER=codex npm run example:client:server
npm run example:client
```

`codex` uses your existing Pi login. For OpenAI, use `PI_PROVIDER=openai` and `OPENAI_API_KEY` on the worker and server. Set `PI_WORKSPACE_DIRECTORY` on the worker, server and client to use another workspace. The worker runs tools there and the client uses it for file links.

The TUI shows the session ID. Another terminal can attach to it:

```sh
npm run example:client -- SESSION_ID
```

The workflow ID is the session ID. Disconnecting leaves the session running. Escape aborts its work through Pi's controller. Session removal closes the workflow.

To view model calls, tools and Pi's usage state in the community Agent Harness console:

```sh
npm run example:console
```

Open `http://localhost:8000/?s=SESSION_ID`. Native sessions are read-only in this console. Send prompts through the Pi client.

Sessions continue as new after 20 turns, or when Temporal recommends it. The session ID stays the same. Rollover waits for Pi to finish its work and drain its queue. Set `PI_SESSION_TURNS_PER_RUN` on the server to change the turn limit.

Sessions started before this change keep their previous behavior for replay compatibility. After updating the worker, send Temporal's `piContinueSession` signal once to move an existing session to the new behavior. It waits for the same idle boundary.

The console retains the last 20 completed turns. Set `PI_TRACE_RETAIN_TURNS` on the server to change that limit, or `0` to discard completed traces. Older trace events are removed from the current stream. Earlier Temporal runs remain available until the namespace's history retention expires.

Pi's complete checkpoint is preserved through the existing session adapter. Pi has no public API for pruning its stored conversation, task or submission records. Compaction reduces model context; it does not shrink that archive. Checkpoints still grow. Use external payload storage on every client and worker, and retain those payloads while any workflow history references them.

This example exposes one configured model and no session plugin loading or reload. It uses a local Unix socket and polls the native transcript every 100 ms. A single busy turn can still reach Temporal's history limits before an idle rollover. The client does not automatically repeat prompts after a disconnect.

Use the existing [Temporal connection settings](../../docs/configuration.md) for the worker and server. The client talks to the local bridge, which can connect to remote Temporal. A local worker connected to Temporal Cloud has been tested. Workers on multiple machines with shared S3 payload storage have not.

To start a Cloud session, set these values in both the worker and server terminals:

```sh
export TEMPORAL_ADDRESS='<namespace>.<account>.tmprl.cloud:7233'
export TEMPORAL_NAMESPACE='<namespace>.<account>'
export TEMPORAL_API_KEY='<api-key>'
export TEMPORAL_PAYLOAD_BUCKET='<shared-bucket>'
export AWS_REGION='us-east-1'
export PI_PROVIDER=codex
```

Run `npm run example:client:worker` and `npm run example:client:server` in those terminals. Then run `npm run example:client` to create a session, or `npm run example:client -- SESSION_ID` to attach to a running session. The client needs no Temporal credentials. Completed sessions cannot be attached to.

```sh
npm run example:client:check
npx vitest run test/native-client.test.ts
PI_WORKSPACE_DIRECTORY=/path/to/workspace npm run example:client:test
```

The last command needs the worker and server running. It uses two upstream clients to submit a task, read the result and compare their transcripts across Continue-as-New. It checks the file written by the agent.
