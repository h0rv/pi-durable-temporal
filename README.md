# pi-durable-temporal

> Experimental. Supports Pi 1.0.4 and Temporal SDK 1.24.0.

A Temporal integration for [Pi Durable](https://github.com/earendil-works/pi/tree/main/packages/durable). Pi's agent loop runs in a workflow. Model calls and tools run as activities.

## Try it

```sh
brew install temporal
git clone https://github.com/h0rv/pi-durable-temporal.git
cd pi-durable-temporal
npm ci --ignore-scripts
npm run dev
```

Open http://localhost:8000. The calculator needs no API key. The UI is Temporal's community Agent Harness console.

## Install

Install the release archive:

```sh
npm install \
  https://github.com/h0rv/pi-durable-temporal/releases/download/v0.1.0/h0rv-pi-durable-temporal-0.1.0.tgz \
  @earendil-works/pi-durable@1.0.4 @earendil-works/pi-ai@1.0.4 \
  @earendil-works/chord@1.0.4 protobufjs@8.8.0 typebox@1.3.27
```

For GitHub Packages, see [registry setup](docs/install.md).

## Use it

Import workflow functions from `@h0rv/pi-durable-temporal/workflow`. Register `createModelActivities(models)` on your worker with your Pi providers. Wrap tools with `temporalTool` to run them as activities.

See [the workflow and worker example](docs/api.md). Use [checkpoints](docs/state.md) to retain Pi state between turns or workflow runs.

## Examples

- [Coding agent](docs/examples.md#coding-agent). Fix a module, approve publication, replace the worker.
- [Multiple agents](docs/examples.md#multiple-agents). Write, review, test, and publish a file.
- [MCP and code mode](examples/mcp/README.md). Run Pi's sandbox with the reference MCP filesystem server.
- [Approvals and state](examples/agent-harness/README.md). Human decisions, automatic evaluations, and a visible plan.

## Pi TUI

Start the worker with a workspace and model credentials:

```sh
PI_WORKSPACE_DIRECTORY=/absolute/path PI_PROVIDER=codex npm run dev
```

In another terminal, use Pi's normal TUI:

```sh
pi --extension ./src/pi-extension.ts --temporal
```

Pi opens an approval dialog for tool calls. Always approve remembers the exact call for the session. `/temporal open` opens the agent console. `/temporal workflow` opens Temporal Web. See [TUI setup](docs/tui.md) for reconnecting and client limits.

## Limits

Hooks must follow Temporal's replay rules. Interrupted activities can repeat. Tools must prevent duplicate side effects.

Use [external payload storage](docs/configuration.md#store-session-data-outside-temporal) to keep session data out of Temporal history. Remote deployments have not been tested.

See [the docs](docs/README.md) for configuration and support limits.

## Development

```sh
npm run check
npm test
npm run test:package
```

MIT.
