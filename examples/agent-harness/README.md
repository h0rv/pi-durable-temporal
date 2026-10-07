# Pi in the Temporal Agent Harness console

Pi's loop runs in a child workflow. Model calls and tools run as activities. Hooks publish trace events to the session. The example server exposes the community console's HTTP API. It downloads the console UI at version 0.6.0 and checks the archive hash.

Install Node 22.19 or newer and the official Temporal CLI. Run `npm ci --ignore-scripts` at the repository root. The default worker uses Pi's scripted provider. It needs no model credentials. Set `PI_PROVIDER=openai` and `OPENAI_API_KEY` for model calls. Set `PI_PROVIDER=codex` to read a Pi login from `~/.pi/agent/auth.json`. Set `PI_MODEL` to select a model. Use the same provider settings on the worker and console.

Start the server:

```sh
mkdir -p .local
temporal server start-dev --ip 127.0.0.1 --db-filename .local/temporal.sqlite
```

In two additional terminals, run:

```sh
npm run example:worker
```

```sh
npm run example:console
```

Open [the console](http://localhost:8000). Create a Pi Durable session. Leave `approvalMode` as `manual` for human decisions, or set it to `auto` for the example calculator evaluator. Ask it to multiply 7 by 6 with the calculate tool.

## Human approval

Run `npm run example:approval`. It creates a manual session and prints its link. The tool waits for your decision. Open the pending tool card and approve or deny it. You can approve that tool for the rest of the session with "Approve and stop asking". The decision and the wait survive worker replacement.

The server exposes `POST /api/approve` with the same fields as Temporal's published TypeScript client:

```json
{
  "session_id": "your-session",
  "tool_id": "the-pending-tool-id",
  "approved": true,
  "reason": "Reviewed the arguments",
  "remember": false
}
```

A duplicate decision returns HTTP 409. An unknown pending tool returns 404. Denial blocks the tool activity and returns a tool error to Pi. Closing the session denies pending tools and discards queued turns. Human decisions appear as `tool_approval_resolved` events. The automatic decisions pane only shows evaluator results.

## Automatic approval

Run `npm run example:e2e`. It creates an automatic session and asks Pi to multiply 7 by 6, then add 8. The evaluator allows pure calculator calls with operands between -100 and 100. Other arguments require a human decision. An evaluator failure also requires a human decision.

Each evaluation has `auto_approval_evaluation_started` and `auto_approval_evaluation_ended` events. The console shows its rule and reason. This example evaluator is ordinary TypeScript code executed as an activity. It does not use a second model.

The end-to-end script checks the answer of 50, both tool calls and the retained trace. It verifies external payload references and replays the child workflow history. It prints links to the trace and Temporal history.

## Observable state

Both examples publish a state named `plan`. It starts with one snapshot. Each change has a new version and a state patch. The plan shows the current turn and phase. It also shows completed tool calls and pending approvals. Select the state pane or move the replay cursor to inspect its earlier values.

## Storage and deployment

Every client and worker uses Temporal's native External Storage. Local examples share `/tmp/temporal-large-payloads`. Each child returns final text, usage and a Pi checkpoint. Temporal stores references to these payloads. Use the shared S3 settings in the [configuration docs](../../docs/configuration.md) for a remote Temporal service.

The console supports text turns, tool approvals and model/tool traces. It does not implement callback tools, file mounts or the general policy editor. Pi context is retained between turns. The coding example adds private workspace tools and requires human approval for publication. The session continues as new after 20 completed turns. See [state](../../docs/state.md) for archive and trace retention.

The UI belongs to the [Temporal community Agent Harness](https://github.com/temporal-community/temporal-agent-harness). Its MIT license is stored with the downloaded assets. It is separate from Temporal Web's integration catalog at `/namespaces/default/agents`. It runs locally and does not require Temporal Cloud.
