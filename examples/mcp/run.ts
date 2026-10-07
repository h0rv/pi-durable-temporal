import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { HttpTransport } from "@temporalio/agent-harness-client";

const url = `http://localhost:${process.env.PI_CONSOLE_PORT ?? 8000}`;
const transport = new HttpTransport({ baseUrl: `${url}/api/` });
const session = `pi-mcp-${randomUUID()}`;
await transport.createSession({ agent_workflow_type: "piSession", session_id: session, data: { task: "mcp" } });
const receipt = await transport.submitMessage(session, {
	type: "ask",
	payload: { text: "Read the inventory through MCP. Use codemode to compute its total value." },
});
console.log(`Trace: ${url}/?s=${session}`);
const tools = new Set<string>();
let answer = "";
for await (const frame of transport.attach(session, receipt.accepted_offset, AbortSignal.timeout(120_000))) {
	if (frame.event === "tool_end" && typeof frame.data.tool_name === "string") tools.add(frame.data.tool_name);
	if (frame.event === "message_handler_error") throw new Error(JSON.stringify(frame.data));
	if (frame.event === "reply_delta") {
		const data = frame.data;
		if (typeof data.text === "string") answer += data.text;
	}
}
assert(tools.has("mcpReadInventory"));
assert(tools.has("codemode"));
assert.match(answer, /460/);
console.log(answer);
