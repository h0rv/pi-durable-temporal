import type { AssistantMessage, AssistantMessageEventStream, Models } from "@earendil-works/pi-ai";
import { isRetryableAssistantError } from "@earendil-works/pi-ai/utils/retry";
import { Context, getClient } from "@temporalio/activity";
import { ApplicationFailure } from "@temporalio/common";
import { withHeartbeat } from "./heartbeat.js";
import type { DeferredRequest, ModelProgress, ModelRequest, ProgressTarget } from "./types.js";

export type { PiCheckpoint } from "./state.js";
export type { DeferredRequest, ModelProgress, ModelRequest, ToolCallInfo } from "./types.js";

async function complete(stream: AssistantMessageEventStream, target?: ProgressTarget): Promise<AssistantMessage> {
	let events: ModelProgress["events"] = [];
	let last = Date.now();
	const flush = async () => {
		if (!target || !events.length) return;
		await getClient()
			.workflow.getHandle(target.workflowId, target.runId)
			.signal("pi_model_progress", {
				requestId: target.requestId,
				attempt: Context.current().info.attempt,
				events,
			} satisfies ModelProgress);
		events = [];
		last = Date.now();
	};
	if (target) {
		for await (const event of stream) {
			events.push(event);
			if (events.length >= 32 || Date.now() - last >= 100) await flush();
		}
		await flush();
	}
	return checked(await stream.result());
}

function checked(message: AssistantMessage): AssistantMessage {
	Context.current().cancellationSignal.throwIfAborted();
	if (isRetryableAssistantError(message)) {
		throw ApplicationFailure.create({
			message: message.errorMessage ?? message.stopReason,
			type: "PiModelError",
			details: [message],
		});
	}
	return message;
}

/** Register these activities alongside your tools. Providers and credentials stay on the worker. */
export function createModelActivities(models: Models) {
	const resolve = (request: ModelRequest["model"]) => {
		const model = models.getModel(request.provider, request.id);
		if (!model) throw ApplicationFailure.nonRetryable(`Unknown model ${request.provider}/${request.id}`);
		return model;
	};
	return {
		piModel: (request: ModelRequest) =>
			withHeartbeat(() =>
				complete(
					models.streamSimple(resolve(request.model), request.context, {
						...request.options,
						signal: Context.current().cancellationSignal,
					}),
					request.progress,
				),
			),
		piFetchDeferred: async (request: DeferredRequest) =>
			withHeartbeat(async () =>
				checked(
					await models.fetchDeferred(resolve(request.model), request.handle, {
						...request.options,
						wait: request.wait,
						signal: Context.current().cancellationSignal,
					}),
				),
			),
		piCancelDeferred: (request: DeferredRequest) =>
			withHeartbeat(() =>
				models.cancelDeferred(resolve(request.model), request.handle, {
					...request.options,
					signal: Context.current().cancellationSignal,
				}),
			),
	};
}

export { createToolActivities, type ToolActivityOptions } from "./tools.js";
