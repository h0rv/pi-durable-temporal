import { createHash } from "node:crypto";
import { access, mkdir, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { unzipSync } from "fflate";

export async function installUi() {
	const directory = resolve(".local/agent-harness-ui-0.6.0");
	try {
		await access(`${directory}/index.html`);
		return directory;
	} catch {}
	const response = await fetch(
		"https://codeload.github.com/temporal-community/temporal-agent-harness/zip/refs/tags/0.6.0",
		{ signal: AbortSignal.timeout(60_000) },
	);
	if (!response.ok) throw new Error(`Download console: HTTP ${response.status}`);
	const bytes = new Uint8Array(await response.arrayBuffer());
	if (
		createHash("sha256").update(bytes).digest("hex") !==
		"d1740ee420c4472a58a9c3e5cb362642c2d74e20d321992a22cb5b62867fab29"
	)
		throw new Error("Console archive checksum mismatch");
	const prefix = "temporal-agent-harness-0.6.0/temporal_agent_harness/ui/dist/";
	const entries = unzipSync(bytes);
	for (const [name, content] of Object.entries(entries)) {
		const relative =
			name === "temporal-agent-harness-0.6.0/LICENSE"
				? "LICENSE"
				: name.startsWith(prefix)
					? name.slice(prefix.length)
					: undefined;
		if (!relative || relative.endsWith("/")) continue;
		const target = resolve(directory, relative);
		if (!target.startsWith(`${directory}/`)) throw new Error("Invalid console archive path");
		await mkdir(dirname(target), { recursive: true });
		await writeFile(target, content);
	}
	await access(`${directory}/index.html`);
	return directory;
}
