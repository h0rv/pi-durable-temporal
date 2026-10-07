# API

## Workflow

```ts
import { Type } from "@earendil-works/pi-ai";
import { createRegistry, defineExtension } from "@earendil-works/pi-durable";
import { createTemporalModels, runTemporalAgent, temporalTool } from "@h0rv/pi-durable-temporal/workflow";
import { model } from "./model.js";

export async function agent(prompt: string) {
	const registry = createRegistry();
	registry.install(defineExtension({
		name: "calculator",
		tools: [temporalTool({
			name: "double",
			description: "Double a number",
			parameters: Type.Object({ value: Type.Number() }),
		})],
	}));
	return runTemporalAgent({ type: "input", content: prompt }, {
		models: createTemporalModels([model]), registry,
		agent: { model: { provider: model.provider, modelId: model.id } },
	});
}
```

`model` is a Pi model descriptor. Configure its provider and credentials on the worker. Register `createModelActivities(models)` and an activity named `double`. The tool activity receives its arguments and `{ callId, taskId, conversationId }`. It returns a Pi `ToolExecutionResult`.

```ts
import { createModelActivities } from "@h0rv/pi-durable-temporal";

const activities = {
	...createModelActivities(models),
	async double({ value }: { value: number }) {
		return { content: [{ type: "text", text: String(value * 2) }] };
	},
};
```

Pass `activities` to your Temporal worker. Keep providers and credentials on the worker. The workflow sends the transcript and generation options to the model activity. It does not send credentials.

## API

| Function | Runs in | Purpose |
| --- | --- | --- |
| `createModelActivities(models)` | Worker | Complete model requests with your Pi providers. |
| `createTemporalModels(catalog, options?, transport?)` | Workflow | Route Pi model requests to the `piModel` activity. |
| `temporalTool(tool, options?, transport?)` | Workflow | Route a tool to the activity with the same name. |
| `openTemporalHarness(options, storage?)` | Workflow | Open Pi's harness for custom orchestration. |
| `runTemporalAgent(input, options)` | Workflow | Run one submission, return context and usage, and close. |
| `runTemporalTurn(input, options, state?)` | Workflow | Return an answer and a Pi checkpoint for the next turn. |
| `openTemporalSession(options, state?)` | Workflow | Open a harness with a checkpoint method. |
| `createToolActivities(tools, { env })` | Worker | Run Pi tools that use an execution environment. |

See [state](state.md) for checkpoint use and retention.

`createToolActivities` supports environment operations such as filesystem access and shell commands. It does not expose live Pi session operations to an activity. Tools that call `api.commit`, create tasks or access conversations must run in the workflow and delegate their external work to activities.

The optional `@h0rv/pi-durable-temporal/agent-harness` module exports `createAgentTrace` for model/tool events and approvals. It also exports `createObservableState` for state snapshots and patches. You supply the event publisher and approval evaluator. See [the console example](../examples/agent-harness/workflows.ts).

Model activities default to a five-minute timeout and three attempts. Retryable Pi provider errors become Temporal activity failures. Pi's separate retry loop is disabled by default in `runTemporalAgent` and `runTemporalTurn`.

Tool activities default to one attempt. Set a retry policy in `ActivityOptions` for tools that can safely repeat. Replay reuses completed activity results. An interrupted activity can run again, so its side effects must tolerate repeated attempts. Use the workflow ID and activity ID as a stable key to record which effects have already completed.

Workflow cancellation cancels pending activities. Cancelling a Pi tool also cancels its activity. `runTemporalAgent` fails if Pi ends the submission without an answer. With `openTemporalHarness`, you can inspect the submission status yourself.

## Execution

Pi's generation tasks and tool tasks run inside the workflow. They are not separate Temporal workflows. A model call is one activity. A tool wrapped with `temporalTool` is another activity.

The console example has a parent workflow for the session and a child workflow for each submitted input. The parent queues inputs and waits for approval decisions. The child runs Pi until it answers, then returns a checkpoint. Custom applications can keep one harness open in one workflow instead.

An activity is a recorded execution boundary, not an atomic transaction with an external service. Temporal can retry an interrupted activity. Use idempotency for writes, or choose a single attempt and let Pi report the failure to the agent.
