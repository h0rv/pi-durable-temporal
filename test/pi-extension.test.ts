import { mkdtemp, readFile, rm } from "node:fs/promises";
import { createServer, type ServerResponse } from "node:http";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import {
	discoverAndLoadExtensions,
	ExtensionRunner,
	initTheme,
	ModelRegistry,
	ModelRuntime,
	SessionManager,
	UserMessageComponent,
} from "@earendil-works/pi-coding-agent";
import { Container, Markdown } from "@earendil-works/pi-tui";
import type { Protocol } from "@temporalio/agent-harness-client";
import { afterEach, expect, it, vi } from "vitest";

const cleanups: (() => Promise<void>)[] = [];
afterEach(async () => {
	for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
	vi.unstubAllEnvs();
});

async function consoleFixture() {
	const requests: { path: string; method: string; body: string }[] = [];
	const events: Protocol.AgentStreamItem[] = [];
	let rejectMessages = false;
	let rejectSessions = false;
	let holdAttach = false;
	let closedAttachments = 0;
	const streams = new Set<ServerResponse>();
	const frame = (event: Protocol.AgentStreamItem, index: number) =>
		`event: ${event.type}\ndata: ${JSON.stringify({ ...event, agent_id: "pi", turn_id: "turn", turn_number: 1, message_id: "message", timestamp: 1, event_offset: index, resume_offset: index + 1 })}\n\n`;
	const server = createServer(async (request, response) => {
		const chunks: Buffer[] = [];
		for await (const chunk of request) chunks.push(Buffer.from(chunk));
		const url = new URL(request.url ?? "/", "http://localhost");
		requests.push({ path: url.pathname, method: request.method ?? "GET", body: Buffer.concat(chunks).toString() });
		if (url.pathname === "/api/attach") {
			response.writeHead(200, { "Content-Type": "text/event-stream" });
			streams.add(response);
			response.on("close", () => {
				streams.delete(response);
				closedAttachments++;
			});
			for (const [index, event] of events.entries()) {
				response.write(frame(event, index));
			}
			if (!holdAttach) response.end();
			return;
		}
		response.setHeader("Content-Type", "application/json");
		if (
			(url.pathname === "/api/messages" && rejectMessages) ||
			(url.pathname === "/api/sessions" && rejectSessions)
		) {
			response.writeHead(503);
			response.end(JSON.stringify({ message: "Worker unavailable" }));
			return;
		}
		response.end(
			JSON.stringify(
				url.pathname === "/api/messages"
					? { message_id: "message", turn_id: "turn", turn_number: 1, accepted_offset: 0, disposition: "opened" }
					: { workflow_id: "remote", closed: false, turn_active: false, pending_turns: [] },
			),
		);
	});
	await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
	const address = server.address();
	if (!address || typeof address === "string") throw new Error("Missing console address");
	const url = `http://127.0.0.1:${address.port}`;
	vi.stubEnv("PI_TEMPORAL_URL", url);
	cleanups.push(async () => {
		server.closeAllConnections();
		await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
	});
	return {
		url,
		requests,
		events,
		emit: (event: Protocol.AgentStreamItem) => {
			const index = events.length;
			events.push(event);
			for (const stream of streams) stream.write(frame(event, index));
		},
		holdAttach: () => {
			holdAttach = true;
		},
		closedAttachments: () => closedAttachments,
		rejectMessages: () => {
			rejectMessages = true;
		},
		rejectSessions: () => {
			rejectSessions = true;
		},
	};
}

async function loadClient(manager = SessionManager.inMemory()) {
	const directory = await mkdtemp(join(tmpdir(), "pi-temporal-extension-"));
	cleanups.push(() => rm(directory, { recursive: true, force: true }));
	const loaded = await discoverAndLoadExtensions([resolve("src/pi-extension.ts")], directory, directory);
	expect(loaded.errors).toEqual([]);
	const modelRuntime = await ModelRuntime.create({
		authPath: join(directory, "auth.json"),
		modelsPath: null,
		refreshOnCreate: false,
	});
	const runner = new ExtensionRunner(
		loaded.extensions,
		loaded.runtime,
		directory,
		manager,
		new ModelRegistry(modelRuntime),
	);
	const notify = vi.fn();
	const confirm = vi.fn(async () => true);
	const select = vi.fn(
		async (_title: string, _options: string[], _opts?: { signal?: AbortSignal }): Promise<string | undefined> =>
			undefined,
	);
	const setStatus = vi.fn();
	const setWidget = vi.fn();
	runner.setUIContext({ ...runner.getUIContext(), notify, confirm, select, setStatus, setWidget }, "tui");
	const sendMessage = vi.fn();
	const sendUserMessage = vi.fn();
	runner.bindCore(
		{
			...loaded.runtime,
			appendEntry: (type, data) => {
				manager.appendCustomEntry(type, data);
			},
			sendMessage,
			sendUserMessage,
		},
		{
			getModel: () => undefined,
			getScopedModels: () => [],
			isIdle: () => true,
			isProjectTrusted: () => true,
			getSignal: () => undefined,
			abort: () => {},
			hasPendingMessages: () => false,
			shutdown: () => {},
			getContextUsage: () => undefined,
			compact: () => {},
			getSystemPrompt: () => "",
		},
	);
	const command = runner.getCommand("temporal");
	if (!command) throw new Error("Missing Temporal command");
	cleanups.push(async () => {
		await runner.emit({ type: "session_shutdown", reason: "quit" });
	});
	const runCommand = (args: string) => command.handler(args, runner.createCommandContext());
	const transcript = () =>
		manager.getBranch().filter((entry) => entry.type === "custom" && entry.customType === "temporal-transcript");
	return {
		runner,
		manager,
		runCommand,
		getArgumentCompletions: command.getArgumentCompletions,
		transcript,
		notify,
		confirm,
		select,
		setStatus,
		setWidget,
		sendMessage,
		sendUserMessage,
	};
}

it("uses Pi's session ID when creating and reconnecting a workflow", async () => {
	const fixture = await consoleFixture();
	const client = await loadClient();
	const id = client.manager.getSessionId();
	await client.runCommand("");
	await client.runCommand("disconnect");
	await client.runCommand("");
	const created = fixture.requests.filter((request) => request.path === "/api/sessions");
	expect(created).toHaveLength(2);
	for (const request of created) expect(JSON.parse(request.body)).toMatchObject({ session_id: id });
});

it("lets another Pi session attach to the same workflow ID", async () => {
	const fixture = await consoleFixture();
	const first = await loadClient();
	const second = await loadClient();
	expect(first.manager.getSessionId()).not.toBe(second.manager.getSessionId());
	await first.runCommand("");
	await second.runCommand(first.manager.getSessionId());
	await second.runner.emitInput("hello from another client", undefined, "interactive");
	await vi.waitFor(() => expect(fixture.requests.some((request) => request.path === "/api/messages")).toBe(true));
	const submitted = fixture.requests.find((request) => request.path === "/api/messages");
	expect(JSON.parse(submitted?.body ?? "null")).toMatchObject({ session_id: first.manager.getSessionId() });
	expect(fixture.requests.filter((request) => request.path === "/api/sessions")).toHaveLength(1);
});

it("loads through Pi and sends input through the official HTTP client without starting a local turn", async () => {
	const fixture = await consoleFixture();
	const client = await loadClient();
	await client.runCommand("remote");
	expect(await client.runner.emitInput("Check the project", undefined, "interactive")).toEqual({ action: "handled" });
	await vi.waitFor(() =>
		expect(fixture.requests.filter((request) => request.path === "/api/messages")).toHaveLength(1),
	);
	const submit = fixture.requests.find((request) => request.path === "/api/messages");
	expect(JSON.parse(submit?.body ?? "null")).toEqual({
		session_id: "remote",
		message: { type: "ask", payload: { text: "Check the project" } },
	});
	expect(client.sendMessage).not.toHaveBeenCalled();
	expect(client.sendUserMessage).not.toHaveBeenCalled();
	expect(client.runner.getEntryRenderer("temporal-transcript")).toBeDefined();
});

it("keeps rejected inputs and network failures from falling through to the local agent", async () => {
	const fixture = await consoleFixture();
	fixture.rejectMessages();
	const client = await loadClient();
	await client.runCommand("remote");
	for (const [text, images] of [
		["image", [{ type: "image" as const, data: "AA==", mimeType: "image/png" }]],
		["/skill:review", undefined],
	] as const) {
		expect(await client.runner.emitInput(text, images ? [...images] : undefined, "interactive")).toEqual({
			action: "handled",
		});
	}
	expect(fixture.requests.filter((request) => request.path === "/api/messages")).toHaveLength(0);
	expect(await client.runner.emitInput("hello", undefined, "interactive")).toEqual({ action: "handled" });
	await vi.waitFor(() => expect(client.notify).toHaveBeenCalledWith("Worker unavailable", "error"));
	expect(fixture.requests.filter((request) => request.path === "/api/messages")).toHaveLength(1);
	expect(client.sendUserMessage).not.toHaveBeenCalled();
});

it("restores the remote connection and skips already displayed transcript events", async () => {
	const fixture = await consoleFixture();
	fixture.events.push(
		{ type: "message_accepted", handler: "ask", payload: { text: "hello" }, disposition: "opened" },
		{ type: "reply_delta", text: "world" },
		{ type: "turn_end" },
	);
	const client = await loadClient();
	await client.runCommand("remote");
	await vi.waitFor(() => expect(client.transcript()).toHaveLength(2));
	expect(
		client.manager.getBranch().find((entry) => entry.type === "custom" && entry.customType === "temporal-connection"),
	).toMatchObject({ data: { offset: 0 } });
	await client.runner.emit({ type: "session_shutdown", reason: "quit" });
	const resumed = await loadClient(client.manager);
	await resumed.runner.emit({ type: "session_start", reason: "resume" });
	await vi.waitFor(() => expect(resumed.setStatus).toHaveBeenCalledWith("temporal", expect.stringContaining("idle")));
	expect(resumed.transcript()).toHaveLength(2);
	fixture.events.push({ type: "reply_delta", text: "A new reply" });
	await resumed.runCommand("reconnect");
	await vi.waitFor(() => expect(resumed.transcript()).toHaveLength(3));
	expect(resumed.transcript().at(-1)).toMatchObject({ data: { label: "Pi on Temporal", text: "A new reply" } });
	expect(client.manager.buildSessionContext().messages).toEqual([]);
});

it("requires confirmation for approval and routes decisions through the session client", async () => {
	const fixture = await consoleFixture();
	fixture.events.push(
		{ type: "message_accepted", handler: "ask", payload: { text: "publish" }, disposition: "opened" },
		{
			type: "tool_approval_requested",
			tool_id: "remote/publish",
			tool_name: "publish",
			tool_input: { path: "release.txt" },
		},
	);
	const client = await loadClient();
	await client.runCommand("remote");
	await vi.waitFor(() =>
		expect(client.setWidget).toHaveBeenCalledWith(
			"temporal",
			expect.arrayContaining([expect.stringContaining("publish needs approval")]),
		),
	);
	client.confirm.mockResolvedValueOnce(false);
	await client.runCommand("approve");
	expect(fixture.requests.filter((request) => request.path === "/api/approve")).toHaveLength(0);
	await client.runCommand("approve");
	await client.runCommand("deny remote/publish");
	expect(
		fixture.requests.filter((request) => request.path === "/api/approve").map((request) => JSON.parse(request.body)),
	).toEqual([
		{ session_id: "remote", tool_id: "remote/publish", approved: true, reason: "Decision from Pi TUI" },
		{ session_id: "remote", tool_id: "remote/publish", approved: false, reason: "Decision from Pi TUI" },
	]);
	expect(client.confirm).toHaveBeenCalledTimes(2);
});

it("disconnects without closing the workflow and leaves resumed local input available", async () => {
	const fixture = await consoleFixture();
	const client = await loadClient();
	await client.runCommand("remote");
	await client.runCommand("disconnect");
	expect(await client.runner.emitInput("local prompt", undefined, "interactive")).toEqual({ action: "continue" });
	expect(fixture.requests.some((request) => request.path.endsWith("/close"))).toBe(false);
	expect(client.manager.getBranch().at(-1)).toMatchObject({
		type: "custom",
		customType: "temporal-connection",
		data: null,
	});
	const resumed = await loadClient(client.manager);
	await resumed.runner.emit({ type: "session_start", reason: "resume" });
	expect(await resumed.runner.emitInput("another local prompt", undefined, "interactive")).toEqual({
		action: "continue",
	});
	expect(client.setStatus).toHaveBeenCalledWith("temporal", undefined);
	expect(client.setWidget).toHaveBeenCalledWith("temporal", undefined);
});

it("blocks local prompts and shell shortcuts when automatic connection fails", async () => {
	const fixture = await consoleFixture();
	fixture.rejectSessions();
	const client = await loadClient();
	client.runner.setFlagValue("temporal", true);
	await client.runner.emit({ type: "session_start", reason: "startup" });
	expect(client.notify).toHaveBeenCalledWith("Worker unavailable", "error");
	expect(await client.runner.emitInput("Fix the project", undefined, "interactive")).toEqual({ action: "handled" });
	const bash = await client.runner.emitUserBash({
		type: "user_bash",
		command: "touch changed",
		excludeFromContext: false,
		cwd: process.cwd(),
	});
	expect(bash).toMatchObject({
		result: { exitCode: 1, output: expect.stringContaining("Local shell shortcuts are disabled") },
	});
	expect(fixture.requests.filter((request) => request.path === "/api/messages")).toHaveLength(0);
	expect(client.sendUserMessage).not.toHaveBeenCalled();
	await client.runCommand("disconnect");
	expect(await client.runner.emitInput("local again", undefined, "interactive")).toEqual({ action: "continue" });
});

it("aborts its open stream during shutdown without closing the remote session", async () => {
	const fixture = await consoleFixture();
	fixture.holdAttach();
	fixture.events.push(
		{ type: "message_accepted", handler: "ask", payload: { text: "hello" }, disposition: "opened" },
		{ type: "reply_delta", text: "working" },
	);
	const client = await loadClient();
	await client.runCommand("remote");
	await vi.waitFor(() => expect(client.transcript()).toHaveLength(2));
	expect(fixture.closedAttachments()).toBe(0);
	await client.runner.emit({ type: "session_shutdown", reason: "quit" });
	await vi.waitFor(() => expect(fixture.closedAttachments()).toBe(1));
	expect(fixture.requests.some((request) => request.path.endsWith("/close"))).toBe(false);
	expect(client.setStatus).toHaveBeenLastCalledWith("temporal", undefined);
	expect(client.setWidget).toHaveBeenLastCalledWith("temporal", undefined);
});

it("keeps Pi's lazy persistence rule for a new session containing only remote entries", async () => {
	const fixture = await consoleFixture();
	fixture.events.push(
		{ type: "message_accepted", handler: "ask", payload: { text: "hello" }, disposition: "opened" },
		{ type: "reply_delta", text: "world" },
	);
	const directory = await mkdtemp(join(tmpdir(), "pi-temporal-session-"));
	cleanups.push(() => rm(directory, { recursive: true, force: true }));
	const manager = SessionManager.create(directory, directory);
	const client = await loadClient(manager);
	await client.runCommand("remote");
	await vi.waitFor(() => expect(client.transcript()).toHaveLength(2));
	const file = manager.getSessionFile();
	if (!file) throw new Error("Missing Pi session path");
	await expect(readFile(file, "utf8")).rejects.toMatchObject({ code: "ENOENT" });
});

it("restores connection and transcript entries from an existing Pi session file", async () => {
	const fixture = await consoleFixture();
	fixture.events.push(
		{ type: "message_accepted", handler: "ask", payload: { text: "hello" }, disposition: "opened" },
		{ type: "reply_delta", text: "world" },
		{ type: "turn_end" },
	);
	const directory = await mkdtemp(join(tmpdir(), "pi-temporal-session-"));
	cleanups.push(() => rm(directory, { recursive: true, force: true }));
	const manager = SessionManager.create(directory, directory);
	manager.appendMessage({ role: "user", content: "A previous local conversation", timestamp: 1 });
	const client = await loadClient(manager);
	await client.runCommand("remote");
	await vi.waitFor(() => expect(client.transcript()).toHaveLength(2));
	await client.runner.emit({ type: "session_shutdown", reason: "quit" });
	const file = manager.getSessionFile();
	if (!file) throw new Error("Missing Pi session path");
	const reopened = SessionManager.open(file, directory);
	const resumed = await loadClient(reopened);
	await resumed.runner.emit({ type: "session_start", reason: "resume" });
	await vi.waitFor(() => expect(resumed.setStatus).toHaveBeenCalledWith("temporal", expect.stringContaining("idle")));
	expect(resumed.transcript()).toHaveLength(2);
	expect(reopened.buildSessionContext().messages).toEqual([
		{ role: "user", content: "A previous local conversation", timestamp: 1 },
	]);
});

it("attaches with the explicit session flag and overrides an older stored connection", async () => {
	const fixture = await consoleFixture();
	fixture.events.push(
		{ type: "message_accepted", handler: "ask", payload: { text: "hello" }, disposition: "opened" },
		{ type: "reply_delta", text: "world" },
	);
	const manager = SessionManager.inMemory();
	manager.appendCustomEntry("temporal-connection", { url: fixture.url, sessionId: "old", offset: 100 });
	const client = await loadClient(manager);
	client.runner.setFlagValue("temporal-session", "remote");
	await client.runner.emit({ type: "session_start", reason: "startup" });
	await vi.waitFor(() => expect(client.transcript()).toHaveLength(2));
	expect(fixture.requests.some((request) => request.path === "/api/workflow-status/remote")).toBe(true);
	expect(fixture.requests.some((request) => request.path === "/api/sessions")).toBe(false);
	expect(await client.runner.emitInput("Next task", undefined, "interactive")).toEqual({ action: "handled" });
	await vi.waitFor(() => expect(fixture.requests.some((request) => request.path === "/api/messages")).toBe(true));
});

it("lets the user choose between pending tool approvals", async () => {
	const fixture = await consoleFixture();
	fixture.events.push(
		{ type: "message_accepted", handler: "ask", payload: { text: "publish" }, disposition: "opened" },
		{
			type: "tool_approval_requested",
			tool_id: "remote/publish",
			tool_name: "publish",
			tool_input: { path: "release.txt" },
		},
		{
			type: "tool_approval_requested",
			tool_id: "remote/deploy",
			tool_name: "deploy",
			tool_input: { target: "local" },
		},
	);
	const client = await loadClient();
	await client.runCommand("remote");
	await vi.waitFor(() =>
		expect(client.setWidget).toHaveBeenCalledWith(
			"temporal",
			expect.arrayContaining([expect.stringContaining("deploy needs approval")]),
		),
	);
	client.select.mockResolvedValueOnce(undefined);
	await client.runCommand("approve");
	expect(client.confirm).not.toHaveBeenCalled();
	expect(fixture.requests.some((request) => request.path === "/api/approve")).toBe(false);
	client.select.mockResolvedValueOnce('2. deploy: {"target":"local"}');
	await client.runCommand("approve");
	const decision = fixture.requests.find((request) => request.path === "/api/approve");
	expect(JSON.parse(decision?.body ?? "null")).toMatchObject({ tool_id: "remote/deploy", approved: true });
	expect(client.select).toHaveBeenCalledWith(
		"Choose a tool",
		['1. publish: {"path":"release.txt"}', '2. deploy: {"target":"local"}'],
		undefined,
	);
});

it("completes Temporal actions and pending tool IDs", async () => {
	const fixture = await consoleFixture();
	const client = await loadClient();
	const completions = client.getArgumentCompletions;
	if (!completions) throw new Error("Missing Temporal argument completions");
	expect(await completions("d")).toEqual([
		{ value: "deny", label: "deny" },
		{ value: "disconnect", label: "disconnect" },
	]);
	expect(await completions("sta")).toEqual([{ value: "status", label: "status" }]);
	fixture.events.push(
		{ type: "message_accepted", handler: "ask", payload: { text: "publish" }, disposition: "opened" },
		{
			type: "tool_approval_requested",
			tool_id: "remote/publish",
			tool_name: "publish",
			tool_input: { path: "release.txt" },
		},
	);
	await client.runCommand("remote");
	await vi.waitFor(async () =>
		expect(await completions("approve ")).toEqual([{ value: "approve remote/publish", label: "publish" }]),
	);
	expect(await completions("deny remote/pub")).toEqual([{ value: "deny remote/publish", label: "publish" }]);
	expect(await completions("approve other")).toEqual([]);
	await client.runCommand("status");
	expect(client.notify).toHaveBeenCalledWith(`${fixture.url}/?s=remote`, "info");
});

it("hides the pending approval banner while Pi's confirmation dialog is open", async () => {
	const fixture = await consoleFixture();
	fixture.events.push(
		{ type: "message_accepted", handler: "ask", payload: { text: "publish" }, disposition: "opened" },
		{
			type: "tool_approval_requested",
			tool_id: "remote/publish",
			tool_name: "publish",
			tool_input: { path: "release.txt" },
		},
	);
	const client = await loadClient();
	await client.runCommand("remote");
	await vi.waitFor(() =>
		expect(client.setWidget).toHaveBeenCalledWith("temporal", [
			"publish needs approval. /temporal approve or /temporal deny",
		]),
	);
	let finish: ((answer: boolean) => void) | undefined;
	client.confirm.mockImplementationOnce(
		() =>
			new Promise<boolean>((resolve) => {
				finish = resolve;
			}),
	);
	const approval = client.runCommand("approve");
	await vi.waitFor(() => expect(client.confirm).toHaveBeenCalled());
	expect(client.setWidget).toHaveBeenLastCalledWith("temporal", undefined);
	await client.runner.emitInput("Another task", undefined, "interactive");
	await vi.waitFor(() => expect(fixture.requests.some((request) => request.path === "/api/messages")).toBe(true));
	expect(client.setWidget).toHaveBeenLastCalledWith("temporal", undefined);
	if (!finish) throw new Error("Missing confirmation resolver");
	finish(false);
	await approval;
	expect(client.setWidget).toHaveBeenLastCalledWith("temporal", [
		"publish needs approval. /temporal approve or /temporal deny",
	]);
	expect(fixture.requests.some((request) => request.path === "/api/approve")).toBe(false);
});

it("uses Pi's user message and Markdown components for remote conversation rendering", async () => {
	initTheme("dark", false);
	const fixture = await consoleFixture();
	fixture.events.push(
		{ type: "message_accepted", handler: "ask", payload: { text: "Please review this" }, disposition: "opened" },
		{ type: "reply_delta", text: "**Reviewed** the project." },
	);
	const client = await loadClient();
	await client.runCommand("remote");
	await vi.waitFor(() => expect(client.transcript()).toHaveLength(2));
	const renderer = client.runner.getEntryRenderer("temporal-transcript");
	const [user, reply] = client.transcript();
	if (!renderer || user?.type !== "custom" || reply?.type !== "custom")
		throw new Error("Missing transcript renderer or entries");
	const theme = client.runner.getUIContext().theme;
	const userComponent = renderer(user, { expanded: false }, theme);
	expect(userComponent).toBeInstanceOf(UserMessageComponent);
	expect(userComponent?.render(60).join("\n")).toContain("Please review this");
	const replyComponent = renderer(reply, { expanded: false }, theme);
	if (!(replyComponent instanceof Container)) throw new Error("Missing reply container");
	expect(replyComponent.children.some((child) => child instanceof Markdown)).toBe(true);
	expect(replyComponent.render(60).join("\n")).toContain("Reviewed");
});

for (const choice of ["Approve", "Deny"]) {
	it(`opens Pi's selector and sends ${choice} without a slash command`, async () => {
		const fixture = await consoleFixture();
		fixture.events.push(
			{ type: "message_accepted", handler: "ask", payload: { text: "publish" }, disposition: "opened" },
			{
				type: "tool_approval_requested",
				tool_id: "remote/publish",
				tool_name: "publish",
				tool_input: { path: "release.txt" },
			},
		);
		const client = await loadClient();
		client.select.mockResolvedValueOnce(choice);
		await client.runCommand("remote");
		await vi.waitFor(() => expect(fixture.requests.some((request) => request.path === "/api/approve")).toBe(true));
		const decision = fixture.requests.find((request) => request.path === "/api/approve");
		expect(JSON.parse(decision?.body ?? "null")).toMatchObject({
			session_id: "remote",
			tool_id: "remote/publish",
			approved: choice === "Approve",
		});
		expect(client.select).toHaveBeenCalledWith(
			'publish\n{\n  "path": "release.txt"\n}',
			["Approve", "Deny", "Later"],
			{ signal: expect.any(AbortSignal) },
		);
	});
}

it("keeps a dismissed approval pending and reopens it with bare /temporal", async () => {
	const fixture = await consoleFixture();
	fixture.events.push(
		{ type: "message_accepted", handler: "ask", payload: { text: "publish" }, disposition: "opened" },
		{
			type: "tool_approval_requested",
			tool_id: "remote/publish",
			tool_name: "publish",
			tool_input: { path: "release.txt" },
		},
	);
	const client = await loadClient();
	await client.runCommand("remote");
	await vi.waitFor(() => expect(client.select).toHaveBeenCalledTimes(1));
	await vi.waitFor(() =>
		expect(client.setWidget).toHaveBeenLastCalledWith("temporal", [
			"publish needs approval. /temporal approve or /temporal deny",
		]),
	);
	client.select.mockResolvedValueOnce("Approve");
	await client.runCommand("");
	expect(fixture.requests.some((request) => request.path === "/api/sessions")).toBe(false);
	expect(fixture.requests.filter((request) => request.path === "/api/attach")).toHaveLength(1);
	expect(
		JSON.parse(fixture.requests.find((request) => request.path === "/api/approve")?.body ?? "null"),
	).toMatchObject({ session_id: "remote", approved: true });
});

it("dismisses an open approval dialog when the client disconnects", async () => {
	const fixture = await consoleFixture();
	fixture.events.push(
		{ type: "message_accepted", handler: "ask", payload: { text: "publish" }, disposition: "opened" },
		{ type: "tool_approval_requested", tool_id: "remote/publish", tool_name: "publish", tool_input: {} },
	);
	const client = await loadClient();
	let finish: ((choice: string) => void) | undefined;
	client.select.mockImplementationOnce(
		() =>
			new Promise((resolve) => {
				finish = resolve;
			}),
	);
	await client.runCommand("remote");
	await vi.waitFor(() => expect(client.select).toHaveBeenCalled());
	const options = client.select.mock.calls[0]?.[2] as { signal: AbortSignal };
	await client.runCommand("disconnect");
	expect(options.signal.aborted).toBe(true);
	finish?.("Approve");
	await new Promise((resolve) => setTimeout(resolve, 10));
	expect(fixture.requests.some((request) => request.path === "/api/approve")).toBe(false);
});

it("dismisses an approval answered by another client", async () => {
	const fixture = await consoleFixture();
	fixture.holdAttach();
	fixture.events.push(
		{ type: "message_accepted", handler: "ask", payload: { text: "publish" }, disposition: "opened" },
		{ type: "tool_approval_requested", tool_id: "remote/publish", tool_name: "publish", tool_input: {} },
	);
	const client = await loadClient();
	let finish: ((choice: string) => void) | undefined;
	client.select.mockImplementationOnce(
		() =>
			new Promise((resolve) => {
				finish = resolve;
			}),
	);
	await client.runCommand("remote");
	await vi.waitFor(() => expect(client.select).toHaveBeenCalled());
	const options = client.select.mock.calls[0]?.[2] as { signal: AbortSignal };
	fixture.emit({
		type: "tool_approval_resolved",
		tool_id: "remote/publish",
		tool_name: "publish",
		approved: true,
		reason: "Another client",
		remember: false,
	});
	await vi.waitFor(() => expect(options.signal.aborted).toBe(true));
	finish?.("Approve");
	await vi.waitFor(() => expect(client.setWidget).toHaveBeenLastCalledWith("temporal", undefined));
	expect(fixture.requests.some((request) => request.path === "/api/approve")).toBe(false);
});
