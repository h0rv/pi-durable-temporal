import { mkdir, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { CodemodeSandbox } from "@earendil-works/pi-codemode";
import { McpClient, StdioTransport, toLlmContent } from "@earendil-works/pi-mcp";
import { Context } from "@temporalio/activity";
import { ApplicationFailure } from "@temporalio/common";

export async function prepareMcpDirectory(directory = resolve(".local/mcp")) {
	await mkdir(directory, { recursive: true });
	await writeFile(
		resolve(directory, "inventory.json"),
		JSON.stringify([
			{ name: "keyboard", price: 80, quantity: 2 },
			{ name: "monitor", price: 300, quantity: 1 },
		]),
		{ flag: "wx" },
	).catch((error: NodeJS.ErrnoException) => {
		if (error.code !== "EEXIST") throw error;
	});
	return directory;
}

export function createMcpActivities(directory = resolve(".local/mcp")) {
	const connected = async <T>(run: (client: McpClient) => Promise<T>): Promise<T> => {
		await prepareMcpDirectory(directory);
		const client = new McpClient({ name: "pi-temporal-example", version: "0.1.0" });
		try {
			await client.connect(
				new StdioTransport({
					command: process.execPath,
					args: [
						fileURLToPath(import.meta.resolve("@modelcontextprotocol/server-filesystem/dist/index.js")),
						directory,
					],
				}),
			);
			return await run(client);
		} finally {
			await client.close();
		}
	};
	return {
		mcpReadInventory: () =>
			connected(async (client) => {
				const result = await client.callTool(
					"read_text_file",
					{ path: resolve(directory, "inventory.json") },
					{ signal: Context.current().cancellationSignal },
				);
				return { content: toLlmContent(result), isError: result.isError === true };
			}),
		codemode: ({ code }: { code: string }) =>
			connected(async (client) => {
				const sandbox = new CodemodeSandbox({
					tools: [
						{
							name: "read_inventory",
							description: "Read inventory records as JSON",
							execute: async (_args, { signal }) => {
								const result = await client.callTool(
									"read_text_file",
									{ path: resolve(directory, "inventory.json") },
									{ signal },
								);
								if (result.isError) throw new Error(JSON.stringify(result.content));
								return toLlmContent(result)
									.filter((block) => block.type === "text")
									.map((block) => block.text)
									.join("\n");
							},
						},
					],
				});
				try {
					const result = await sandbox.execute(code, { signal: Context.current().cancellationSignal });
					if (process.env.PI_MCP_INJECT_FAILURE === "true" && Context.current().info.attempt === 1)
						throw ApplicationFailure.create({
							message: "Injected failure after MCP reads",
							type: "McpExampleFailure",
						});
					const content = [...result.output];
					if (result.ok && result.value !== undefined)
						content.push({ type: "text", text: JSON.stringify(result.value) });
					if (!result.ok) content.push({ type: "text", text: result.error.message });
					return { content, isError: !result.ok, details: { calls: result.calls } };
				} finally {
					await sandbox.close();
				}
			}),
	};
}
