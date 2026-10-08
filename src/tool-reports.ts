import type { JsonValue } from "@earendil-works/chord";
import type { ToolDiagnostic, ToolExecutionResult } from "@earendil-works/pi-durable";
import type { ShellOutputSkip } from "@earendil-works/pi-durable/env";

export type ToolReport =
	| { type: "output"; chunk: string | number[]; skipped?: ShellOutputSkip }
	| { type: "diagnostic"; value: ToolDiagnostic }
	| { type: "details"; value: JsonValue };

export type ToolReports = { piToolReports: ToolReport[]; attempt: number };
export type ToolActivityResult = ToolExecutionResult & { reports: ToolReports };
