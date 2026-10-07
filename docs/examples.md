# Examples

Run `npm run dev` for the calculator and Agent Harness console at http://localhost:8000. It starts a local Temporal server if needed. The default provider is scripted and needs no credentials.

## Coding agent

With the local server running:

```sh
PI_PROVIDER=openai OPENAI_API_KEY='your-key' npm run example:coding
```

For an existing Pi login, use `PI_PROVIDER=codex`. Set `PI_MODEL` to choose a model from Pi's provider catalog.

The agent fixes an invoice total function and checks its source. Open the printed link on port 8001 to approve publication. The script kills the worker after publication and starts a replacement. It checks that completed model calls are reused and the file is published once.

The checks accept a small JavaScript grammar without executing generated code. This example is not a general coding sandbox.

Use `npm run example:coding -- --approve` to simulate the human decision through the approval API. The [recording](coding-demo.cast) used that option. Download the [recording player](coding-demo.html) and open it in a browser.

## Multiple agents

Run `npm run example:release`. One Pi agent writes a file and two review it. The test runner fails once. The parent waits for a simulated approval. The script replaces workers during approval and after publication.

The reviews and approval refer to the source hash. Publication fails if the source changed. The example uses scripted models. See [the workflow](../examples/release/workflows.ts).

## Approvals and state

Run `npm run example:e2e` for automatic calculator approvals or `npm run example:approval` for a human decision. Both publish a `plan` state to the console.

See [the Agent Harness example](../examples/agent-harness/README.md) for the protocol and UI details.
