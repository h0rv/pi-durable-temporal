import { inWorkflowContext } from "@temporalio/workflow";
import structuredClone from "@ungap/structured-clone";

if (!inWorkflowContext()) throw new Error("Import pi-durable-temporal/workflow only inside a Temporal workflow bundle");

// Keep Chord's references alive so replay does not depend on garbage collection.
class WorkflowRef<T extends object> {
	private readonly target: T;
	constructor(target: T) {
		this.target = target;
	}
	deref(): T {
		return this.target;
	}
}

const clearWorkflowTimeout = globalThis.clearTimeout;

Object.defineProperties(globalThis, {
	structuredClone: { value: structuredClone, configurable: true },
	// Pi calls clearTimeout(undefined), which Temporal treats as a timer handle.
	clearTimeout: {
		value: (handle: ReturnType<typeof setTimeout> | undefined) => {
			if (handle !== undefined) clearWorkflowTimeout(handle);
		},
		configurable: true,
	},
	WeakRef: { value: WorkflowRef, configurable: true },
	AbortSignal: { value: new AbortController().signal.constructor, configurable: true },
	queueMicrotask: {
		value: (callback: () => void) => {
			void Promise.resolve().then(callback);
		},
		configurable: true,
	},
	performance: { value: { now: () => Date.now() }, configurable: true },
	crypto: {
		// Used only for provider session UUIDs. Never use this for secrets.
		value: {
			getRandomValues(bytes: Uint8Array) {
				for (let i = 0; i < bytes.length; i++) bytes[i] = Math.floor(Math.random() * 256);
				return bytes;
			},
		},
		configurable: true,
	},
});
