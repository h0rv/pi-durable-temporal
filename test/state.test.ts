import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { type ConversationId, type DocumentId, MemoryStorage } from "@earendil-works/pi-durable";
import { registerStorageConformance } from "@earendil-works/pi-durable/testing";
import { describe, expect, it } from "vitest";
import { TemporalStorage } from "../src/state.js";

registerStorageConformance({ describe, expect, it }, "Temporal checkpoint storage", async (use) => {
	await use(new TemporalStorage());
});

it("restores commits and unused identifier allocations in their original order", async () => {
	let storage = new TemporalStorage();
	for (let iteration = 0; iteration < 10; iteration++) {
		const id = await storage.mintId<ConversationId>();
		await storage.commit([{ type: "conversation", value: { id } }], BACKGROUND_CONTEXT);
		await storage.mintId();
		const checkpoint = JSON.parse(JSON.stringify(storage.checkpoint()));
		const restored = await TemporalStorage.restore(checkpoint);
		expect(await restored.conversation(id, BACKGROUND_CONTEXT)).toEqual({ id });
		expect(await restored.mintId()).toBe(await storage.mintId());
		storage = restored;
	}
});

it("preserves allocator advancement from externally supplied records", async () => {
	const storage = new TemporalStorage();
	const source = new MemoryStorage();
	let id = await source.mintId<ConversationId>();
	for (let index = 0; index < 100; index++) id = await source.mintId<ConversationId>();
	await storage.commit([{ type: "conversation", value: { id } }], BACKGROUND_CONTEXT);
	await storage.mintId();
	const restored = await TemporalStorage.restore(storage.checkpoint());
	expect(await restored.mintId()).toBe(await storage.mintId());
});

it("does not include rejected commits in a checkpoint", async () => {
	const storage = new TemporalStorage();
	const id = await storage.mintId<DocumentId>();
	const before = storage.checkpoint();
	await expect(storage.commit([{ type: "document.retire", id }], BACKGROUND_CONTEXT)).rejects.toThrow();
	expect(storage.checkpoint()).toEqual(before);
});
