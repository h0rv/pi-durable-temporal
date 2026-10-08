import { resolve } from "node:path";
import { runClientTui } from "../../.local/pi-upstream/packages/coding-agent/src/experimental/client-tui.ts";

const sessionId = process.argv[2];
const directory = resolve(import.meta.dirname, "../../.local/pi-sockets");
if (process.env.PI_WORKSPACE_DIRECTORY) process.chdir(resolve(process.env.PI_WORKSPACE_DIRECTORY));
await runClientTui({ command: "client", ...(sessionId ? { sessionId } : {}) }, { directory });
