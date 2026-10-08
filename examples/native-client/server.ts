import { randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { createRemoteServiceEndpoint, RemoteServiceProvider, replicatedState } from "@earendil-works/chord";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { clampThinkingLevel, getSupportedThinkingLevels } from "@earendil-works/pi-ai";
import type { AgentChange, AgentState, ConversationView } from "@earendil-works/pi-durable";
import type { RoutedSessionHandle, ServerHost } from "@earendil-works/pi-server";
import { SessionNotFoundError } from "@earendil-works/pi-server";
import { createUnixServer, getUnixSocketPath } from "@earendil-works/pi-server/unix";
import { Client, Connection, WorkflowUpdateFailedError } from "@temporalio/client";
import { ApplicationFailure } from "@temporalio/common";
import { getAgentDir } from "../../.local/pi-upstream/packages/coding-agent/src/config.ts";
import { loadProjectContextFiles } from "../../.local/pi-upstream/packages/coding-agent/src/core/resource-loader.ts";
import { SettingsManager } from "../../.local/pi-upstream/packages/coding-agent/src/core/settings-manager.ts";
import { loadSkills } from "../../.local/pi-upstream/packages/coding-agent/src/core/skills.ts";
import { buildSystemPrompt } from "../../.local/pi-upstream/packages/coding-agent/src/core/system-prompt.ts";
import { createHarnessSettings } from "../../.local/pi-upstream/packages/coding-agent/src/experimental/durable/harness-setup.ts";
import { AgentController } from "../../.local/pi-upstream/packages/coding-agent/src/experimental/services/agent-controller.ts";
import {
	Models,
	type ModelsState,
} from "../../.local/pi-upstream/packages/coding-agent/src/experimental/services/models.ts";
import { SessionPlugins } from "../../.local/pi-upstream/packages/coding-agent/src/experimental/services/plugins.ts";
import { createExperimentalServerServices } from "../../.local/pi-upstream/packages/coding-agent/src/experimental/services/server.ts";
import { Transcript } from "../../.local/pi-upstream/packages/coding-agent/src/experimental/services/transcript.ts";
import { connectionSettings } from "../connection.js";
import { exampleModel } from "../providers.js";

const settings = await connectionSettings();
const connection = await Connection.connect(settings.connection);
const client = new Client({ connection, namespace: settings.namespace, dataConverter: settings.dataConverter });
const directory = resolve(".local/pi-sockets");
await mkdir(directory, { recursive: true, mode: 0o700 });
const identityFile = resolve(directory, "server-id");
let serverId: string;
try {
	serverId = (await readFile(identityFile, "utf8")).trim();
} catch (error) {
	if (!(error && typeof error === "object" && "code" in error && error.code === "ENOENT")) throw error;
	serverId = randomUUID();
	await writeFile(identityFile, serverId, { mode: 0o600 });
}
const model = exampleModel();
const cwd = resolve(process.env.PI_WORKSPACE_DIRECTORY ?? process.cwd());
const piSettings = SettingsManager.create(cwd, getAgentDir());
const services = await createExperimentalServerServices({
	async list() {
		const sessions = [];
		for await (const execution of client.workflow.list({
			query: "WorkflowType = 'piNativeSession' AND ExecutionStatus = 'Running'",
		})) {
			sessions.push({ serverId, sessionId: execution.workflowId, createdAt: execution.startTime.getTime() });
		}
		return sessions;
	},
	async create(options) {
		const sessionId = options.id ?? randomUUID();
		await client.workflow.start("piNativeSession", {
			workflowId: sessionId,
			taskQueue: process.env.PI_CLIENT_TASK_QUEUE ?? "pi-native-client",
			args: [
				{
					model,
					cwd,
					maxTurnsPerRun: Number(process.env.PI_SESSION_TURNS_PER_RUN ?? 20),
					retainTraceTurns: Number(process.env.PI_TRACE_RETAIN_TURNS ?? 20),
					settings: JSON.parse(JSON.stringify(createHarnessSettings(piSettings))),
					agent: {
						thinkingLevel: piSettings.getDefaultThinkingLevel(),
						instructions: buildSystemPrompt({
							cwd,
							selectedTools: ["read", "write", "edit", "bash"],
							contextFiles: loadProjectContextFiles({ cwd, agentDir: getAgentDir() }),
							skills: loadSkills({
								cwd,
								agentDir: getAgentDir(),
								skillPaths: piSettings.getSkillPaths(),
								includeDefaults: true,
							}).skills,
						}),
					},
				},
			],
		});
		return { serverId, sessionId, createdAt: Date.now() };
	},
	async remove(sessionId) {
		const handle = client.workflow.getHandle(sessionId);
		await handle.signal("piCloseSession");
		await handle.result();
	},
	async prepareSessionPlugins(_id, packages) {
		if (packages?.length) throw new Error("Session plugin packages are not supported by this Temporal example");
		return { packagePaths: [], presentationPlugins: {} };
	},
	async reloadPresentationPlugins() {
		return {};
	},
});

const host: ServerHost = {
	serverServices: services.host,
	async resolveSession(id) {
		const execution = await client.workflow.getHandle(id).describe();
		if (execution.type !== "piNativeSession" || execution.status.name !== "RUNNING")
			throw new SessionNotFoundError(id);
		return { id };
	},
	async openSession({ id }): Promise<RoutedSessionHandle> {
		const handle = client.workflow.getHandle(id);
		const update = async <T>(name: string, args: [] | [unknown] = []): Promise<T> => {
			for (let attempt = 0; ; attempt++) {
				try {
					return await (args.length
						? handle.executeUpdate<T, [unknown]>(name, { args })
						: handle.executeUpdate<T, []>(name));
				} catch (error) {
					if (
						attempt >= 100 ||
						!(error instanceof WorkflowUpdateFailedError) ||
						!(error.cause instanceof ApplicationFailure) ||
						error.cause.type !== "SessionRollingOver"
					)
						throw error;
					await delay(100);
				}
			}
		};
		const view = replicatedState(await handle.query<ConversationView>("piTranscript"));
		const models = replicatedState<ModelsState>({
			catalog: {
				revision: 1,
				availableModels: [
					{ provider: model.provider, modelId: model.id, name: model.name, reasoning: model.reasoning },
				],
			},
			configuration: { model: { provider: model.provider, modelId: model.id }, thinkingLevel: "off" },
			refresh: { status: "idle" },
		});
		const provider = new RemoteServiceProvider([
			{ service: AgentController, mode: "singleton" },
			{ service: Transcript, mode: "singleton" },
			{ service: Models, mode: "singleton" },
			{ service: SessionPlugins, mode: "singleton" },
		]);
		provider.provide(Transcript, { state: view });
		provider.provide(AgentController, {
			prompt: (request) => update("piController.prompt", [request]),
			steer: (request) => update("piController.steer", [request]),
			followUp: (request) => update("piController.followUp", [request]),
			cancelQueued: (entryId) => update("piController.cancelQueued", [entryId]),
			abort: () => update("piController.abort"),
			compact: (request) => update("piController.compact", [request]),
			waitForPrompt: (operationId) => update("piController.waitForPrompt", [operationId]),
		});
		const configure = (change: AgentChange) => update<void>("piConfigure", [change]);
		provider.provide(Models, {
			state: models,
			async cycleThinking() {
				const levels = getSupportedThinkingLevels(model);
				await configure({
					thinkingLevel: levels[(levels.indexOf(models.value.configuration.thinkingLevel) + 1) % levels.length],
				});
			},
			async getThinkingLevels() {
				return getSupportedThinkingLevels(model);
			},
			async refresh() {},
			async select(selected) {
				if (selected.provider !== model.provider || selected.modelId !== model.id)
					throw new Error("Model is not configured on this worker");
				await configure({
					model: selected,
					thinkingLevel: clampThinkingLevel(model, models.value.configuration.thinkingLevel),
				});
			},
			async selectThinking(thinkingLevel) {
				if (!getSupportedThinkingLevels(model).includes(thinkingLevel))
					throw new Error("Thinking level is unavailable for this model");
				await configure({ thinkingLevel });
			},
		});
		provider.provide(SessionPlugins, {
			async reload() {
				throw new Error("Restart the Temporal worker to load workflow changes");
			},
		});
		let stopped = false;
		let timer: NodeJS.Timeout | undefined;
		const poll = async () => {
			try {
				const next = await handle.query<ConversationView>("piTranscript");
				if (stopped) return;
				view.replace(BACKGROUND_CONTEXT, next);
				const agent = next.docs["pi.agent"] as AgentState;
				models.change(BACKGROUND_CONTEXT, (draft) => {
					draft.configuration.model = agent.model ?? null;
					draft.configuration.thinkingLevel = agent.thinkingLevel ?? "off";
				});
			} catch (error) {
				if (!stopped) console.error(error);
			} finally {
				if (!stopped) timer = setTimeout(poll, 100);
			}
		};
		void poll();
		const endpoints = new Set<ReturnType<typeof createRemoteServiceEndpoint>>();
		return {
			attachClient() {
				const endpoint = createRemoteServiceEndpoint(provider);
				endpoints.add(endpoint);
				return {
					invokeService: (call, publish, context) => endpoint.invoke(call, publish, context),
					release() {
						endpoint.dispose();
						endpoints.delete(endpoint);
					},
				};
			},
			async close() {
				stopped = true;
				clearTimeout(timer);
				for (const endpoint of endpoints) endpoint.dispose();
				provider.dispose();
			},
		};
	},
};
const server = createUnixServer(host, { serverId, path: getUnixSocketPath(serverId, directory) });
await server.start();
console.log(`Pi server: ${serverId}\nConnect: npm run example:client`);
const close = async () => {
	await server.close();
	await services.dispose();
	await connection.close();
};
process.once("SIGINT", close);
process.once("SIGTERM", close);
