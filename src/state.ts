import type { Context } from "@earendil-works/chord";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { type Id, MemoryStorage, type StorageWrite } from "@earendil-works/pi-durable";

export type PiCheckpoint = {
	version: 1;
	journal: readonly ({ type: "mint" } | { type: "commit"; writes: readonly StorageWrite[] })[];
};

/** Pi storage that can be carried into another Temporal run. */
export class TemporalStorage extends MemoryStorage {
	private readonly journal: Array<PiCheckpoint["journal"][number]> = [];

	static async restore(checkpoint?: PiCheckpoint): Promise<TemporalStorage> {
		const storage = new TemporalStorage();
		if (!checkpoint) return storage;
		if (checkpoint.version !== 1 || !Array.isArray(checkpoint.journal)) throw new Error("Invalid Pi checkpoint");
		for (const operation of checkpoint.journal) {
			if (operation.type === "mint") await storage.mintId();
			else if (operation.type === "commit") await storage.commit(operation.writes, BACKGROUND_CONTEXT);
			else throw new Error("Invalid Pi checkpoint operation");
		}
		return storage;
	}

	override async mintId<I extends Id<string>>(): Promise<I> {
		const id = await super.mintId<I>();
		this.journal.push(Object.freeze({ type: "mint" }));
		return id;
	}

	override async commit(writes: readonly StorageWrite[], _context: Context) {
		const prepared = this.prepareCommit(writes);
		const sequence = prepared.apply();
		this.journal.push(Object.freeze({ type: "commit", writes: prepared.writes }));
		return sequence;
	}

	checkpoint(): PiCheckpoint {
		return Object.freeze({ version: 1, journal: Object.freeze([...this.journal]) });
	}
}
