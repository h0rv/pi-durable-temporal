import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { extname, resolve } from "node:path";
import type { Api, Model } from "@earendil-works/pi-ai";
import { Type } from "@earendil-works/pi-ai";
import type { Protocol, SessionSummary, SubmitMessageResponse } from "@temporalio/agent-harness-client";
import {
	type Client,
	WorkflowExecutionAlreadyStartedError,
	WorkflowNotFoundError,
	WorkflowUpdateFailedError,
} from "@temporalio/client";
import { ApplicationFailure, defaultPayloadConverter } from "@temporalio/common";
import proto from "@temporalio/proto";
import { WorkflowStreamClient } from "@temporalio/workflow-streams/client";
import type { TSchema } from "typebox";
import { Value } from "typebox/value";
import { exampleModel } from "../providers.js";
import { approveTool, ask, close, status, traceSnapshot } from "./protocol.js";

const textSchema = Type.Object({ text: Type.String({ minLength: 1, maxLength: 32_000 }) });
const createSchema = Type.Object({
	agent_workflow_type: Type.Literal("piSession"),
	data: Type.Optional(
		Type.Object({
			task: Type.Optional(
				Type.Union([
					Type.Literal("calculator"),
					Type.Literal("coding"),
					Type.Literal("mcp"),
					Type.Literal("workspace"),
				]),
			),
			maxTurnsPerRun: Type.Optional(Type.Integer({ minimum: 1, maximum: 100 })),
			approvalMode: Type.Optional(Type.Union([Type.Literal("manual"), Type.Literal("auto")])),
		}),
	),
	session_id: Type.Optional(Type.String({ minLength: 1, maxLength: 200, pattern: "^[a-zA-Z0-9_.-]+$" })),
});
const approvalSchema = Type.Object({
	session_id: Type.String(),
	tool_id: Type.String(),
	approved: Type.Boolean(),
	reason: Type.Optional(Type.Union([Type.String({ maxLength: 4000 }), Type.Null()])),
	remember: Type.Optional(Type.Boolean()),
	rememberScope: Type.Optional(Type.Union([Type.Literal("tool"), Type.Literal("call")])),
});
const chatSchema = Type.Object({
	session_id: Type.String(),
	message: Type.Object({ type: Type.Literal("ask"), payload: textSchema }),
});
class RequestError extends Error {
	readonly status: number;
	constructor(status: number, message: string) {
		super(message);
		this.status = status;
	}
}
async function body<T extends TSchema>(request: IncomingMessage, schema: T) {
	let size = 0;
	const chunks: Buffer[] = [];
	for await (const chunk of request) {
		const bytes = Buffer.from(chunk);
		size += bytes.length;
		if (size > 256_000) throw new RequestError(413, "Request is too large");
		chunks.push(bytes);
	}
	try {
		return Value.Parse(schema, JSON.parse(Buffer.concat(chunks).toString()));
	} catch {
		throw new RequestError(422, "Invalid request");
	}
}
function json(response: ServerResponse, value: unknown, code = 200) {
	response.writeHead(code, { "Content-Type": "application/json" });
	response.end(JSON.stringify(value));
}

export function createConsole(
	client: Client,
	uiDirectory: string,
	taskQueue: string,
	model: Model<Api> = exampleModel(),
) {
	const summary = async (id: string): Promise<SessionSummary> => {
		const info = await client.workflow.getHandle(id).describe();
		if (info.type !== "piSession" && info.type !== "piNativeSession")
			throw new RequestError(404, "Unknown Pi session");
		return {
			workflow_id: id,
			created_at: info.startTime.getTime() / 1000,
			label: info.type === "piNativeSession" ? "Pi native client" : "Pi Durable",
			agent_workflow_type: info.type,
			execution_status: info.status.name,
			closed: info.status.name !== "RUNNING",
		};
	};
	const streamEvents = async (response: ServerResponse, id: string, offset: number, turnId?: string) => {
		const session = await summary(id);
		if (session.closed) {
			const snapshot = await client.workflow.getHandle(id).query(traceSnapshot);
			response.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-cache" });
			for (const [index, item] of snapshot.log.entries()) {
				const eventOffset = snapshot.base_offset + index;
				if (eventOffset < offset || item.topic !== "turn_events") continue;
				const envelope = defaultPayloadConverter.fromPayload<Protocol.AgentEvent>(
					proto.temporal.api.common.v1.Payload.decode(Buffer.from(item.data, "base64")),
				);
				const { event, ...metadata } = envelope;
				response.write(
					`event: ${event.type}\ndata: ${JSON.stringify({ ...event, ...metadata, resume_offset: eventOffset + 1, event_offset: eventOffset })}\n\n`,
				);
			}
			response.end();
			return;
		}
		const stream = WorkflowStreamClient.create(client, id);
		const abort = new AbortController();
		response.on("close", () => abort.abort());
		response.writeHead(200, {
			"Content-Type": "text/event-stream",
			"Cache-Control": "no-cache",
			"X-Accel-Buffering": "no",
		});
		try {
			await client.connection.withAbortSignal(abort.signal, async () => {
				const end = await stream.getOffset();
				const snapshot = await client.workflow.getHandle(id).query(status);
				if (!turnId && !snapshot.turn_active && snapshot.pending_turns.length === 0 && offset >= end) return;
				for await (const item of stream.topic<Protocol.AgentEvent>("turn_events").subscribe(offset)) {
					const { event, ...metadata } = item.data;
					response.write(
						`event: ${event.type}\ndata: ${JSON.stringify({ ...event, ...metadata, resume_offset: item.offset + 1, event_offset: item.offset })}\n\n`,
					);
					if (turnId && metadata.turn_id === turnId && event.type === "turn_end") break;
					if (!turnId && item.offset + 1 >= end) {
						const latest = await client.workflow.getHandle(id).query(status);
						if (
							!latest.turn_active &&
							latest.pending_turns.length === 0 &&
							item.offset + 1 >= (await stream.getOffset())
						)
							break;
					}
				}
			});
		} finally {
			await stream[Symbol.asyncDispose]();
			response.end();
		}
	};
	return createServer(async (request, response) => {
		try {
			const url = new URL(request.url ?? "/", "http://localhost");
			const path = decodeURIComponent(url.pathname);
			if (path === "/api/agents") {
				const queue = await client.workflowService.describeTaskQueue({
					namespace: client.options.namespace,
					taskQueue: { name: taskQueue },
					taskQueueType: 1,
				});
				const count = queue.pollers?.length ?? 0;
				return json(response, {
					agents: [
						{
							key: "pi-durable",
							workflow_type: "piSession",
							task_queue: taskQueue,
							label: "Pi Durable",
							description: "Pi Durable on Temporal",
							agent: null,
							init_data: {
								required: false,
								schema: Type.Object({
									approvalMode: Type.Optional(Type.Union([Type.Literal("manual"), Type.Literal("auto")])),
								}),
							},
							worker: {
								task_queue: taskQueue,
								status: count ? "ready" : "no_worker",
								poller_count: count,
								last_seen: null,
								error: null,
							},
						},
					],
				});
			}
			if (path === "/api/sessions" && request.method === "POST") {
				const input = await body(request, createSchema);
				const id = input.session_id ?? `pi-${randomUUID()}`;
				try {
					await client.workflow.start("piSession", {
						workflowId: id,
						taskQueue,
						args: [{ ...input.data, model }],
					});
				} catch (error) {
					if (!(error instanceof WorkflowExecutionAlreadyStartedError)) throw error;
				}
				return json(response, await summary(id));
			}
			if (path === "/api/sessions" && request.method === "GET") {
				const sessions: SessionSummary[] = [];
				for await (const info of client.workflow.list({
					query: "WorkflowType = 'piSession' OR WorkflowType = 'piNativeSession'",
					pageSize: 100,
				})) {
					sessions.push({
						workflow_id: info.workflowId,
						created_at: info.startTime.getTime() / 1000,
						label: info.type === "piNativeSession" ? "Pi native client" : "Pi Durable",
						agent_workflow_type: info.type,
						execution_status: info.status.name,
						closed: info.status.name !== "RUNNING",
					});
					if (sessions.length >= 100) break;
				}
				return json(response, sessions);
			}
			if (path === "/api/chat" && request.method === "POST") {
				const input = await body(request, chatSchema);
				if ((await summary(input.session_id)).agent_workflow_type === "piNativeSession")
					throw new RequestError(405, "Use the Pi client to send messages to this session");
				const receipt = await client.workflow
					.getHandle(input.session_id)
					.executeUpdate(ask, { args: [input.message.payload] });
				return await streamEvents(response, input.session_id, receipt.accepted_offset, receipt.turn_id);
			}
			if (path === "/api/approve" && request.method === "POST") {
				const { session_id, ...decision } = await body(request, approvalSchema);
				if ((await summary(session_id)).agent_workflow_type === "piNativeSession")
					throw new RequestError(405, "This session does not use console approvals");
				return json(
					response,
					await client.workflow.getHandle(session_id).executeUpdate(approveTool, { args: [decision] }),
				);
			}
			if (path === "/api/messages" && request.method === "POST") {
				const input = await body(request, chatSchema);
				if ((await summary(input.session_id)).agent_workflow_type === "piNativeSession")
					throw new RequestError(405, "Use the Pi client to send messages to this session");
				const receipt: SubmitMessageResponse = await client.workflow
					.getHandle(input.session_id)
					.executeUpdate(ask, { args: [input.message.payload] });
				return json(response, receipt);
			}
			if (path === "/api/attach") {
				const id = url.searchParams.get("session_id");
				const offset = Number(url.searchParams.get("from_offset") ?? 0);
				if (!id || !Number.isSafeInteger(offset) || offset < 0)
					throw new RequestError(422, "Invalid stream cursor");
				return await streamEvents(response, id, offset);
			}
			const workflowStatus = path.match(/^\/api\/workflow-status\/(.+)$/);
			if (workflowStatus) return json(response, await summary(workflowStatus[1]));
			const interfacePath = path.match(/^\/api\/agent-interface\/(.+)$/);
			if (interfacePath) {
				if ((await summary(interfacePath[1])).agent_workflow_type === "piNativeSession") return json(response, []);
				return json(response, [
					{
						name: "ask",
						description: "Run one Pi agent turn",
						parameters: textSchema,
						output: textSchema,
						mid_turn: "enqueue",
						model_callable: true,
					},
				]);
			}
			const statusPath = path.match(/^\/api\/status\/(.+)$/);
			if (statusPath) {
				await summary(statusPath[1]);
				return json(response, await client.workflow.getHandle(statusPath[1]).query(status));
			}
			const closePath = path.match(/^\/api\/sessions\/(.+)\/close$/);
			if (closePath && request.method === "POST") {
				const session = await summary(closePath[1]);
				await client.workflow
					.getHandle(closePath[1])
					.signal(session.agent_workflow_type === "piNativeSession" ? "piCloseSession" : close);
				return json(response, { ok: true });
			}
			if (path.startsWith("/api/")) throw new RequestError(501, "This console supports text turns and traces");
			if (request.method !== "GET") throw new RequestError(405, "Method not allowed");
			const file = resolve(uiDirectory, path === "/" ? "index.html" : `.${path}`);
			if (!file.startsWith(`${uiDirectory}/`)) throw new RequestError(404, "Not found");
			const content = await readFile(file).catch(() => {
				throw new RequestError(404, "Not found");
			});
			const mime: Record<string, string> = {
				".html": "text/html",
				".js": "text/javascript",
				".css": "text/css",
				".svg": "image/svg+xml",
				".woff2": "font/woff2",
			};
			response.writeHead(200, { "Content-Type": mime[extname(file)] ?? "application/octet-stream" });
			response.end(content);
		} catch (error) {
			if (response.destroyed) return;
			if (response.headersSent) {
				response.end();
				return;
			}
			const approvalFailure =
				error instanceof WorkflowUpdateFailedError && error.cause instanceof ApplicationFailure
					? error.cause
					: undefined;
			const code = approvalFailure
				? approvalFailure.type === "ToolApprovalAlreadyResolved"
					? 409
					: approvalFailure.type === "UnknownApproval"
						? 404
						: 422
				: error instanceof RequestError
					? error.status
					: error instanceof WorkflowNotFoundError
						? 404
						: 500;
			json(
				response,
				{
					error: approvalFailure?.type,
					message: approvalFailure?.message ?? (error instanceof Error ? error.message : String(error)),
					detail: error instanceof Error ? error.message : String(error),
				},
				code,
			);
		}
	});
}
