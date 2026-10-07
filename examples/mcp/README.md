# MCP and code mode

This example uses Pi's MCP client and code mode sandbox with the reference MCP filesystem server. The model first reads an inventory through MCP. It then writes JavaScript that calls MCP from code mode and computes the inventory value.

From the repository root:

```sh
PI_MCP_INJECT_FAILURE=true npm run dev
```

In another terminal:

```sh
npm run example:mcp
```

Open the printed trace link. The default model is scripted. The MCP server and QuickJS sandbox run locally. Use `PI_PROVIDER=codex` or `PI_PROVIDER=openai` on the worker for model-generated code. Set `PI_MCP_DIRECTORY` to choose the sample directory.

The failure flag throws after the first code mode execution. Temporal retries that activity. Only an inventory read is exposed to the sandbox, so the repeated MCP call has no side effect.

Code mode runs as one activity. Its nested MCP calls are not separate Temporal activities. If that activity is interrupted before its result is recorded, the script and its nested calls can repeat. Add idempotency before exposing write operations. Each completed outer activity result is reused during workflow replay.

Run `npm test -- test/mcp.test.ts` to check retry, worker replacement and history replay. This uses the pinned reference server package and needs no external account.

The VM is recreated for each attempt. This example does not persist code mode's `store()` values between calls. Pi conversation checkpoints do not save a running VM.
