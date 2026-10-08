import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { Type } from "@earendil-works/pi-ai";
import { defineTool } from "@earendil-works/pi-durable";

export const reportTool = defineTool({
	name: "report",
	description: "Report output",
	parameters: Type.Object({ fail: Type.Boolean() }),
	outputLimits: { maxBytes: 12, maxLines: 2, retain: "tail" },
	execute: async ({ fail }, api) => {
		const bytes = new TextEncoder().encode("one\n🦀two\nthree\n");
		api.output(bytes.slice(0, 5));
		api.output(bytes.slice(5, 7));
		api.output(bytes.slice(7));
		api.diagnostic({ severity: "warn", code: "test", message: "reported warning" });
		await api.details({ reported: true }, BACKGROUND_CONTEXT);
		if (fail) throw new Error("reported failure");
		return {};
	},
});
