# State

## Within a workflow run

Pi keeps its conversations and tasks in memory. Temporal records activity results and workflow events. After worker replacement, Temporal replays those events and Pi rebuilds its state. Completed model calls and tool activities are reused during replay.

You do not need another Pi extension for this. Hooks and workflow tools must be deterministic. Put network calls and file access in activities.

## Between workflow runs

Use `runTemporalTurn` to return a Pi checkpoint with the answer. Pass that checkpoint into the next turn:

```ts
const options = { models, registry, agent };
const first = await runTemporalTurn({ type: "input", content: "Remember 42" }, options);
const second = await runTemporalTurn({ type: "input", content: "What number?" }, options, first.state);
```

For custom orchestration, use `openTemporalSession(options, state?)`. It returns a harness and a `checkpoint()` method. The method waits for foreground work to finish. It rejects a session that still has unfinished tasks or submissions.

The checkpoint contains Pi's committed storage writes and identifier allocations in their original order. It includes conversations, documents and task records. It is a full archive, so it grows with use. It does not include your tool's filesystem or other external data.

Configure [external payload storage](configuration.md#store-session-data-outside-temporal) on every Temporal client and worker. Temporal then stores immutable references in workflow history instead of the checkpoint contents. Retain the referenced data while the history can be replayed.

## Long sessions

The Agent Harness example retains the checkpoint between turns. After 20 completed turns, it continues as a new workflow run. It carries queued turns and approval decisions into that run, along with the trace. A pending tool approval finishes before that boundary.

Continue-as-New limits the number of events in each Temporal run. It does not shrink the Pi checkpoint or the retained trace. Pi compaction limits the context sent to the model. You still need a retention policy for archived state and traces.

`runTemporalAgent` remains available for a single submission. It returns the context and usage, without a checkpoint.
