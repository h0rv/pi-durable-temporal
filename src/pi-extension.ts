import {
	type ExtensionAPI,
	type ExtensionContext,
	getMarkdownTheme,
	UserMessageComponent,
} from "@earendil-works/pi-coding-agent";
import { Container, Markdown, Spacer, Text } from "@earendil-works/pi-tui";
import { AgentSessionCore, HttpTransport, PlainSessionState, waitingCalls } from "@temporalio/agent-harness-client";

type Connection = { url: string; sessionId: string; offset: number };
type Transcript = { label: string; text: string };
function toolText(output: string) {
	try {
		const result: unknown = JSON.parse(output);
		if (result && typeof result === "object" && "content" in result && Array.isArray(result.content))
			return (
				result.content
					.flatMap((block: unknown) =>
						block && typeof block === "object" && "text" in block && typeof block.text === "string"
							? [block.text]
							: [],
					)
					.join("\n") || output
			);
	} catch {}
	return output;
}
const connectionEntry = "temporal-connection";
const transcriptEntry = "temporal-transcript";

export default function temporalExtension(pi: ExtensionAPI) {
	let connection: Connection | undefined;
	let enabled = false;
	let session: AgentSessionCore | undefined;
	let unsubscribe: (() => void) | undefined;
	let context: ExtensionContext | undefined;
	let updateWidget: (() => void) | undefined;
	let confirming = false;
	let dialog: AbortController | undefined;
	let dialogToolId: string | undefined;
	let review: ((reopen?: boolean) => Promise<void>) | undefined;
	pi.registerFlag("temporal", {
		type: "boolean",
		description: "Connect the TUI to a Temporal session",
		default: false,
	});
	pi.registerFlag("temporal-session", {
		type: "string",
		description: "Attach to an existing Temporal workflow ID",
	});
	pi.registerEntryRenderer<Transcript>(transcriptEntry, (entry, _options, theme) => {
		if (!entry.data) return undefined;
		const { label, text } = entry.data;
		if (label === "You") return new UserMessageComponent(text);
		if (label === "Pi on Temporal") {
			const component = new Container();
			component.addChild(new Spacer(1));
			component.addChild(new Markdown(text.trim(), 1, 0, getMarkdownTheme()));
			return component;
		}
		return new Text(`${theme.fg("muted", label)}\n${text}`, 1, 1);
	});
	const save = () => pi.appendEntry(connectionEntry, connection ? { ...connection } : null);
	const show = (label: string, text: string) => pi.appendEntry<Transcript>(transcriptEntry, { label, text });
	const stop = () => {
		dialog?.abort();
		dialog = undefined;
		confirming = false;
		review = undefined;
		unsubscribe?.();
		unsubscribe = undefined;
		session?.stop();
		session = undefined;
		context?.ui.setStatus("temporal", undefined);
		context?.ui.setWidget("temporal", undefined);
		updateWidget = undefined;
	};
	const observe = (ctx: ExtensionContext) => {
		stop();
		context = ctx;
		if (!connection) return;
		const current = connection;
		const state = new PlainSessionState();
		const client = new AgentSessionCore(
			{
				sessionId: current.sessionId,
				transport: new HttpTransport({ baseUrl: `${current.url}/api/` }),
				schedule: (flush) => queueMicrotask(flush),
				onError: (error) => ctx.ui.notify(error.message, "error"),
			},
			state,
		);
		session = client;
		const prompted = new Set<string>();
		review = async (reopen = false) => {
			if (confirming || !ctx.hasUI) return;
			const pending = waitingCalls(state.agents, () => false).approvals;
			if (reopen) prompted.clear();
			const selected = pending.find(({ part }) => !prompted.has(part.toolId));
			if (!selected) return;
			prompted.add(selected.part.toolId);
			confirming = true;
			const controller = new AbortController();
			dialog = controller;
			dialogToolId = selected.part.toolId;
			ctx.ui.setWidget("temporal", undefined);
			try {
				const choice = await ctx.ui.select(
					`${selected.part.toolName}\n${JSON.stringify(selected.part.input, null, 2)}`,
					["Approve", "Deny", "Later"],
					{ signal: controller.signal },
				);
				if (controller.signal.aborted || session !== client) return;
				if (choice === "Approve" || choice === "Deny") {
					await client.respondToApproval(selected.part.toolId, {
						approved: choice === "Approve",
						reason: "Decision from Pi TUI",
					});
				}
			} catch (error) {
				if (!controller.signal.aborted) {
					ctx.ui.notify(error instanceof Error ? error.message : String(error), "error");
				}
			} finally {
				if (dialog === controller) {
					dialog = undefined;
					confirming = false;
					updateWidget?.();
					queueMicrotask(() => void review?.());
				}
			}
		};
		updateWidget = () => {
			if (confirming) return;
			const approvals = waitingCalls(state.agents, () => false).approvals;
			ctx.ui.setWidget(
				"temporal",
				approvals.length
					? approvals.map(({ part }) => `${part.toolName} needs approval. /temporal approve or /temporal deny`)
					: undefined,
			);
		};
		let processed = 0;
		unsubscribe = state.subscribe(() => {
			let changed = false;
			for (const frame of state.frames.slice(processed)) {
				if (frame.data.resume_offset <= current.offset) continue;
				switch (frame.event) {
					case "message_accepted":
						if (typeof frame.data.payload.text === "string") show("You", frame.data.payload.text);
						break;
					case "reply_delta":
						show("Pi on Temporal", frame.data.text);
						break;
					case "tool_end":
						show(frame.data.tool_name, toolText(frame.data.tool_output));
						break;
					case "tool_error":
						show(frame.data.tool_name, toolText(frame.data.message));
						break;
					case "message_handler_error":
						show("Temporal error", frame.data.message);
						break;
				}
				current.offset = frame.data.resume_offset;
				changed = true;
			}
			processed = state.frames.length;
			if (changed) save();
			ctx.ui.setStatus("temporal", `Temporal ${state.connection} · ${state.agentStatus}`);
			const pending = waitingCalls(state.agents, () => false).approvals;
			if (dialog && !pending.some(({ part }) => part.toolId === dialogToolId)) dialog.abort();
			updateWidget?.();
			queueMicrotask(() => void review?.());
		});
		client.start();
	};
	const connect = async (id: string | undefined, ctx: ExtensionContext) => {
		enabled = true;
		const url = (process.env.PI_TEMPORAL_URL ?? "http://localhost:8000").replace(/\/$/, "");
		const transport = new HttpTransport({ baseUrl: `${url}/api/` });
		const sessionId = id || ctx.sessionManager.getSessionId();
		if (id) {
			const status = await transport.workflowStatus(sessionId, AbortSignal.timeout(10_000));
			if (status.closed) throw new Error("This Temporal session is closed");
		} else {
			await transport.createSession(
				{
					agent_workflow_type: "piSession",
					session_id: sessionId,
					data: { task: process.env.PI_TEMPORAL_TASK ?? "workspace", approvalMode: "manual" },
				},
				AbortSignal.timeout(10_000),
			);
		}
		connection = { url, sessionId, offset: 0 };
		save();
		observe(ctx);
	};
	pi.registerCommand("temporal", {
		description: "Connect, approve, deny, reconnect or disconnect a Temporal session",
		getArgumentCompletions: (prefix) => {
			const actions = ["approve", "deny", "reconnect", "disconnect", "status"];
			const [action, input] = prefix.split(/\s+/, 2);
			const values =
				input !== undefined && (action === "approve" || action === "deny") && session
					? waitingCalls(session.state.agents, () => false).approvals.map(({ part }) => ({
							value: `${action} ${part.toolId}`,
							label: part.toolName,
						}))
					: actions.map((value) => ({ value, label: value }));
			return values.filter(({ value }) => value.startsWith(prefix));
		},
		handler: async (args, ctx) => {
			try {
				const [action, id] = args.trim().split(/\s+/);
				if (!action && connection) {
					await review?.(true);
				} else if (action === "new") {
					ctx.ui.notify("Use /new in Pi, then /temporal to connect the new session.", "info");
				} else if (action === "status") {
					ctx.ui.notify(
						connection ? `${connection.url}/?s=${encodeURIComponent(connection.sessionId)}` : "Disconnected",
						"info",
					);
				} else if (action === "disconnect") {
					stop();
					connection = undefined;
					enabled = false;
					save();
					ctx.ui.notify("Disconnected. The Temporal session continues running.", "info");
				} else if (action === "reconnect") {
					if (!connection) throw new Error("Connect with /temporal first");
					observe(ctx);
				} else if (action === "approve" || action === "deny") {
					if (confirming) return;
					if (!session) throw new Error("Connect with /temporal first");
					const approvals = waitingCalls(session.state.agents, () => false).approvals;
					let selected = id
						? approvals.find(({ part }) => part.toolId === id)
						: approvals.length === 1
							? approvals[0]
							: undefined;
					if (!id && approvals.length > 1) {
						const labels = approvals.map(
							({ part }, index) => `${index + 1}. ${part.toolName}: ${JSON.stringify(part.input).slice(0, 120)}`,
						);
						const choice = await ctx.ui.select("Choose a tool", labels);
						if (!choice) return;
						selected = approvals[labels.indexOf(choice)];
					}
					if (!selected) {
						ctx.ui.notify(
							approvals.map(({ part }) => `${part.toolId}: ${part.toolName}`).join("\n") ||
								"No pending approvals",
							"info",
						);
						return;
					}
					if (action === "approve") {
						confirming = true;
						ctx.ui.setWidget("temporal", undefined);
						let approved: boolean;
						try {
							approved = await ctx.ui.confirm(
								`Approve ${selected.part.toolName}`,
								JSON.stringify(selected.part.input, null, 2),
							);
						} finally {
							confirming = false;
							updateWidget?.();
						}
						if (!approved) return;
					}
					await session.respondToApproval(selected.part.toolId, {
						approved: action === "approve",
						reason: "Decision from Pi TUI",
					});
				} else {
					if (!ctx.isIdle()) throw new Error("Wait for the local agent before connecting");
					await connect(action || undefined, ctx);
				}
			} catch (error) {
				ctx.ui.notify(error instanceof Error ? error.message : String(error), "error");
			}
		},
	});
	pi.on("input", (event, ctx) => {
		if (!enabled) return { action: "continue" };
		if (event.images?.length || event.text.startsWith("/")) {
			ctx.ui.notify("Temporal accepts text prompts. Images and local slash commands are not forwarded.", "error");
			return { action: "handled" };
		}
		if (!session) {
			ctx.ui.notify("Use /temporal reconnect before sending a prompt", "error");
			return { action: "handled" };
		}
		void session
			.sendMessage("ask", { text: event.text })
			.catch((error: unknown) => ctx.ui.notify(error instanceof Error ? error.message : String(error), "error"));
		return { action: "handled" };
	});
	pi.on("user_bash", () =>
		enabled
			? {
					result: {
						output:
							"Ask the Temporal agent to run the command. Local shell shortcuts are disabled while connected.",
						exitCode: 1,
						cancelled: false,
						truncated: false,
					},
				}
			: undefined,
	);
	const restore = async (ctx: ExtensionContext) => {
		stop();
		connection = undefined;
		for (const entry of ctx.sessionManager.getBranch()) {
			if (entry.type !== "custom" || entry.customType !== connectionEntry) continue;
			const data = entry.data;
			connection =
				data &&
				typeof data === "object" &&
				"url" in data &&
				"sessionId" in data &&
				"offset" in data &&
				typeof data.url === "string" &&
				typeof data.sessionId === "string" &&
				typeof data.offset === "number"
					? { url: data.url, sessionId: data.sessionId, offset: data.offset }
					: undefined;
		}
		const id = pi.getFlag("temporal-session");
		enabled = Boolean(connection || pi.getFlag("temporal") || id);
		if (connection && (!id || id === connection.sessionId)) observe(ctx);
		else if (enabled) {
			try {
				await connect(typeof id === "string" ? id : undefined, ctx);
			} catch (error) {
				ctx.ui.notify(error instanceof Error ? error.message : String(error), "error");
			}
		}
	};
	pi.on("session_start", (_event, ctx) => restore(ctx));
	pi.on("session_tree", (_event, ctx) => restore(ctx));
	pi.on("session_shutdown", stop);
}
