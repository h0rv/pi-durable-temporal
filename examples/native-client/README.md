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

This example exposes one configured model and no session plugin loading or reload. It uses a local Unix socket and polls the native transcript every 100 ms. It has no Continue-as-New, so long sessions need a workflow history limit. The client does not automatically repeat prompts after a disconnect.

Use the existing [Temporal connection settings](../../docs/configuration.md) for the worker and server. The client talks to the local bridge, which can connect to remote Temporal. A remote deployment has not been tested.

```sh
npm run example:client:check
npx vitest run test/native-client.test.ts
PI_WORKSPACE_DIRECTORY=/path/to/workspace npm run example:client:test
```

The last command needs the worker and server running. It uses two upstream clients to submit a task, read the result and compare their transcripts. It checks the file written by the agent.
