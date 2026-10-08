# Pi TUI

Run from the repository directory. Start a worker with a workspace and model credentials:

```sh
PI_WORKSPACE_DIRECTORY=/absolute/path PI_PROVIDER=codex npm run dev
```

`PI_PROVIDER=openai` uses `OPENAI_API_KEY`. `codex` uses Pi's saved credentials.

In another terminal:

```sh
pi --extension ./src/pi-extension.ts --temporal
```

The extension submits prompts to Temporal. The worker runs Pi Durable with its native read, write, edit and bash tools. The normal TUI displays the remote transcript. Local model calls do not run while connected.

New workflows use Pi's session ID as their workflow ID. The console uses that same session ID. Use Pi's `/new` command to start a new session. Another client can attach with `--temporal-session SESSION_ID`.

## Approvals

Pi opens its native selector when a tool needs approval. It shows the tool arguments and offers Approve, Deny and Later. Escape leaves the request pending. Use `/temporal` to reopen it.

`/temporal approve` and `/temporal deny` also work. Tab completes actions and pending tool IDs.

The workflow retains approval requests until someone answers. Another TUI or the browser console can answer them. Disconnecting leaves the workflow running.

## Reconnect

`/temporal status` shows the trace URL and workflow ID. Attach to that workflow after restarting Pi:

```sh
PI_TEMPORAL_URL=http://localhost:8000 \
pi --extension ./src/pi-extension.ts --temporal-session WORKFLOW_ID
```

`/temporal reconnect` reconnects the current client. `/temporal disconnect` returns input to the local agent.

Pi defers saving a new session until it has a native user or assistant message. Remote transcript entries alone do not create that file. Reattach by workflow ID. Existing Pi conversation files can retain the extension's connection and cursor.

## Limits

The client accepts text. Images and local prompt templates are not forwarded. Escape controls Pi's local agent; it does not cancel a Temporal turn. The trace reports completed model responses, rather than streaming individual tokens.

The worker workspace is the directory set in `PI_WORKSPACE_DIRECTORY`. It can differ from the terminal's directory. Native shell tools run on the worker host. Use shared persistent files when workers run on different machines.
