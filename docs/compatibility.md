# Compatibility

The adapter uses Pi's `Harness` directly. It does not replace Pi's task scheduler, tools or hook system. Install extensions in the registry you pass to `openTemporalHarness` or `openTemporalSession`.

## Workflow code

Pi's conversation APIs remain available through the returned harness. This includes submissions, steering, forks, documents and custom tasks. Pi's generation and tool hooks also run there. All of that code must obey Temporal's replay rules. Use activities for external work.

A tool that needs `api.commit`, conversations or child tasks should stay in the workflow. Wrap only its external operations in activities. `createToolActivities` is for tools that use an execution environment. It cannot transfer a live Pi session into an activity.

Pi turns a thrown tool error into a tool result for the model. It also supports blocked tools, rewritten results and termination requests. The adapter returns activity results to that same Pi tool path. Exhausted model retries return the original provider error response to Pi for classification.

Temporal model activities use one attempt by default. Pi's retry policy, response hooks and usage accounting remain active. If you configure additional Temporal activity attempts, Pi sees only the final attempt's response and usage.

## Progress

Pass `{ stream: true }` as the third argument to `createTemporalModels` or `temporalTool` to receive progress signals. `onProgress` receives the activity attempt number. Partial output is provisional. A retried activity starts reporting again, and the final activity result is authoritative.

With one activity attempt, streamed tool reports update Pi as they arrive. With additional Temporal attempts, `onProgress` receives provisional reports and Pi receives only the final attempt's reports. Without streaming, tool details are applied when the activity finishes.

Progress signals add events and payloads to workflow history. Default model calls return a completed response without those signals. Deferred model requests use `piFetchDeferred` and `piCancelDeferred` activities.

## Clients

The example console supports multiple browser connections to a session. Turns are queued and the trace can be read after reconnecting.

Pi 1.0.4's stock TUI has no supported remote execution backend hook. This package does not change its rendering, input handling or commands.

## Deployment

Local recovery and replay are tested. Remote connection settings are tested, but a Temporal Cloud deployment has not been run.

Model credentials and MCP connections stay on the worker. Files accessed by tools need shared persistent storage when activities can move between workers. `NodeExecutionEnv` is a local process environment, not a security sandbox.

The model adapter handles chat generation. Image generation and classifier APIs need their own worker activities. Provider callbacks such as `onPayload` and `onResponse` cannot cross a Temporal payload boundary. Configure those on the worker.

Use one model transport per workflow and share it between conversations. Use one progress-enabled tool adapter per tool name. Each adapter owns its signal handler.
