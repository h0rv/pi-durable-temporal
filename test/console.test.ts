import { randomUUID } from "node:crypto";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Type } from "@earendil-works/pi-ai";
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai/providers/faux";
import type { JsonValue } from "@temporalio/agent-harness-client";
import { type AgentSseFrame, applyOps, HttpTransport } from "@temporalio/agent-harness-client";
import { Client } from "@temporalio/client";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { bundleWorkflowCode, Worker } from "@temporalio/worker";
import { Value } from "typebox/value";
import { afterAll, beforeAll, expect, it } from "vitest";
import { status } from "../examples/agent-harness/protocol.js";
import { createConsole } from "../examples/agent-harness/server.js";
import { localDataConverter } from "../examples/storage.js";
import type { ModelRequest } from "../src/index.js";

let env: TestWorkflowEnvironment;
let root: string;
let bundle: Awaited<ReturnType<typeof bundleWorkflowCode>>;
beforeAll(async () => {
	const path = process.env.TEMPORAL_CLI_PATH;
	env = await TestWorkflowEnvironment.createLocal(
		path ? { server: { executable: { type: "existing-path", path } } } : undefined,
	);
	root = await mkdtemp(join(tmpdir(), "pi-console-"));
	await writeFile(join(root, "index.html"), "console");
	bundle = await bundleWorkflowCode({
		workflowsPath: fileURLToPath(new URL("../examples/agent-harness/session.ts", import.meta.url)),
	});
}, 60_000);
afterAll(async () => {
	await env?.teardown();
	if (root) await rm(root, { recursive: true, force: true });
});

it("uses the published client to queue turns and recover traces after worker and HTTP server replacement", async () => {
	const dataConverter = localDataConverter(join(root, "payloads"));
	const client = new Client({ connection: env.client.connection, dataConverter });
	const queue = randomUUID();
	let calls = 0;
	let release: () => void = () => undefined;
	const gate = new Promise<void>((resolve) => {
		release = resolve;
	});
	const activities = {
		async piModel(request: ModelRequest) {
			calls++;
			if (
				request.context.messages
					.slice(
						request.context.messages.reduce(
							(found, message, index) => (message.role === "user" ? index : found),
							-1,
						),
					)
					.some((message) => message.role === "toolResult")
			)
				return fauxAssistantMessage("42");
			await gate;
			return fauxAssistantMessage(fauxToolCall("calculate", { operation: "multiply", a: 7, b: 6 }), {
				stopReason: "toolUse",
			});
		},
		async evaluateApproval() {
			return { verdict: "approve" as const, reason: "Test arithmetic", details: {} };
		},
		async calculate() {
			return { content: [{ type: "text" as const, text: "42" }] };
		},
	};
	const makeWorker = () =>
		Worker.create({
			connection: env.nativeConnection,
			taskQueue: queue,
			workflowBundle: bundle,
			dataConverter,
			activities,
		});
	let server = createConsole(client, root, queue);
	const listen = async () => {
		await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
		const address = server.address();
		if (!address || typeof address === "string") throw new Error("No HTTP port");
		return new HttpTransport({ baseUrl: `http://127.0.0.1:${address.port}/api/` });
	};
	const closeServer = async () => {
		server.closeAllConnections();
		await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
	};
	let transport = await listen();
	const sessionId = randomUUID();
	try {
		const worker = await makeWorker();
		await worker.runUntil(async () => {
			await transport.createSession({
				agent_workflow_type: "piSession",
				session_id: sessionId,
				data: { approvalMode: "auto" },
			});
			const first = await transport.submitMessage(sessionId, { type: "ask", payload: { text: "Double 21" } });
			const second = await transport.submitMessage(sessionId, { type: "ask", payload: { text: "Again" } });
			expect(first.disposition).toBe("opened");
			expect(second.disposition).toBe("queued");
			release();
			const frames: AgentSseFrame[] = [];
			for await (const frame of transport.attach(sessionId, 0, AbortSignal.timeout(15_000))) frames.push(frame);
			expect(frames.filter((frame) => frame.event === "turn_end")).toHaveLength(2);
			expect(frames.filter((frame) => frame.event === "tool_end")).toHaveLength(2);
			const state = frames.find((frame) => frame.event === "state_snapshot");
			expect(state?.event).toBe("state_snapshot");
			if (state?.event === "state_snapshot") {
				let value: JsonValue = Value.Parse(
					Type.Object({
						phase: Type.String(),
						completedTools: Type.Number(),
						pendingApprovals: Type.Number(),
						turn: Type.Number(),
					}),
					state.data.value,
				);
				let version = state.data.version;
				for (const frame of frames)
					if (frame.event === "state_patch") {
						expect(frame.data.version).toBe(++version);
						const patch = applyOps(value, frame.data.ops);
						expect(patch.error).toBeNull();
						value = patch.doc;
					}
				expect(value).toEqual({ phase: "complete", completedTools: 2, pendingApprovals: 0, turn: 2 });
			}
			expect(calls).toBe(4);
			expect(frames.filter((frame) => frame.event === "auto_approval_evaluation_ended")).toHaveLength(2);
		});
		await closeServer();
		server = createConsole(client, root, queue);
		transport = await listen();
		const replacement = await makeWorker();
		await replacement.runUntil(async () => {
			const frames: AgentSseFrame[] = [];
			for await (const frame of transport.attach(sessionId, 0, AbortSignal.timeout(15_000))) frames.push(frame);
			expect(frames.filter((frame) => frame.event === "turn_end")).toHaveLength(2);
			expect(calls).toBe(4);
			expect((await transport.listSessions()).map((session) => session.workflow_id)).toContain(sessionId);
			const parent = client.workflow.getHandle(sessionId);
			await Worker.runReplayHistory(
				{ workflowBundle: bundle, dataConverter },
				await parent.fetchHistory(),
				sessionId,
			);
			await transport.closeSession(sessionId);
			await parent.result();
		});
	} finally {
		release();
		await closeServer();
	}
}, 60_000);

it.each(["approve", "deny", "close", "auto-escalate", "auto-error"])(
	"keeps a human %s decision across worker replacement",
	async (decision) => {
		const automatic = decision.startsWith("auto-");
		const accepted = decision === "approve" || automatic;
		const directory = await mkdtemp(join(root, "approval-"));
		const dataConverter = localDataConverter(join(directory, "payloads"));
		const client = new Client({ connection: env.client.connection, dataConverter });
		const queue = randomUUID();
		let effects = 0;
		let calls = 0;
		const activities = {
			async piModel(request: ModelRequest) {
				calls++;
				const userIndex = request.context.messages.reduce(
					(index, message, current) => (message.role === "user" ? current : index),
					-1,
				);
				const result = request.context.messages
					.slice(userIndex + 1)
					.find((message) => message.role === "toolResult");
				if (result) return fauxAssistantMessage(result.isError ? "Denied" : "42");
				return fauxAssistantMessage(fauxToolCall("calculate", { operation: "multiply", a: 7, b: 6 }), {
					stopReason: "toolUse",
				});
			},
			async evaluateApproval() {
				if (decision === "auto-error") throw new Error("Injected evaluator failure");
				return { verdict: "escalate" as const, reason: "Ask a human", details: {} };
			},
			async calculate() {
				effects++;
				return { content: [{ type: "text" as const, text: "42" }] };
			},
		};
		const makeWorker = () =>
			Worker.create({
				connection: env.nativeConnection,
				taskQueue: queue,
				workflowBundle: bundle,
				dataConverter,
				activities,
			});
		const server = createConsole(client, root, queue);
		await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
		const address = server.address();
		if (!address || typeof address === "string") throw new Error("No HTTP port");
		const base = `http://127.0.0.1:${address.port}/api/`;
		const transport = new HttpTransport({ baseUrl: base });
		const sessionId = randomUUID();
		let toolId = "";
		let initialRunId = "";
		const pending = async () => {
			for (let i = 0; i < 100; i++) {
				const snapshot = await client.workflow.getHandle(sessionId).query(status);
				if (snapshot.pending_approvals.length) return snapshot.pending_approvals[0];
				await new Promise((resolve) => setTimeout(resolve, 25));
			}
			throw new Error("Approval request did not appear");
		};
		try {
			const first = await makeWorker();
			await first.runUntil(async () => {
				await transport.createSession({
					agent_workflow_type: "piSession",
					session_id: sessionId,
					data: { approvalMode: automatic ? "auto" : "manual", maxTurnsPerRun: 1 },
				});
				await transport.submitMessage(sessionId, { type: "ask", payload: { text: "Calculate 7 times 6" } });
				initialRunId = (await client.workflow.getHandle(sessionId).describe()).runId;
				toolId = (await pending()).tool_id;
				expect(effects).toBe(0);
				expect(calls).toBe(1);
			});
			const replacement = await makeWorker();
			await replacement.runUntil(async () => {
				expect((await pending()).tool_id).toBe(toolId);
				if (decision === "close") {
					await transport.closeSession(sessionId);
					await client.workflow.getHandle(sessionId).result();
				} else {
					await transport.approveTool(sessionId, toolId, {
						approved: accepted,
						reason: "Test human decision",
						remember: decision === "approve",
					});
					const duplicate = await fetch(`${base}approve`, {
						method: "POST",
						headers: { "Content-Type": "application/json" },
						body: JSON.stringify({ session_id: sessionId, tool_id: toolId, approved: decision !== "approve" }),
					});
					expect(duplicate.status).toBe(409);
					const frames: AgentSseFrame[] = [];
					for await (const frame of transport.attach(sessionId, 0, AbortSignal.timeout(15_000)))
						frames.push(frame);
					expect(frames.filter((frame) => frame.event === "tool_approval_resolved")).toHaveLength(1);
					expect(frames.filter((frame) => frame.event === "tool_start")).toHaveLength(accepted ? 1 : 0);
					if (decision === "approve") {
						for (let attempt = 0; attempt < 100; attempt++) {
							if ((await client.workflow.getHandle(sessionId).describe()).runId !== initialRunId) break;
							await new Promise((resolve) => setTimeout(resolve, 25));
						}
						expect((await client.workflow.getHandle(sessionId).describe()).runId).not.toBe(initialRunId);
						await transport.submitMessage(sessionId, { type: "ask", payload: { text: "Again" } });
						const next: AgentSseFrame[] = [];
						for await (const frame of transport.attach(sessionId, 0, AbortSignal.timeout(15_000)))
							next.push(frame);
						expect(next.filter((frame) => frame.event === "tool_approval_requested")).toHaveLength(1);
						expect(effects).toBe(2);
					}
					await transport.closeSession(sessionId);
					await client.workflow.getHandle(sessionId).result();
				}
				const closedFrames: AgentSseFrame[] = [];
				for await (const frame of transport.attach(sessionId, 0, AbortSignal.timeout(15_000)))
					closedFrames.push(frame);
				expect(closedFrames.filter((frame) => frame.event === "tool_approval_resolved")).toHaveLength(1);
				expect(effects).toBe(decision === "approve" ? 2 : accepted ? 1 : 0);
				await Worker.runReplayHistory(
					{ workflowBundle: bundle, dataConverter },
					await client.workflow.getHandle(sessionId).fetchHistory(),
					sessionId,
				);
			});
		} finally {
			server.closeAllConnections();
			await new Promise<void>((resolve) => server.close(() => resolve()));
		}
	},
	60_000,
);
