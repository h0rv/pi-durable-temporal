import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { discoverAndLoadExtensions } from "@earendil-works/pi-coding-agent";

const run = promisify(execFile);
const root = process.cwd();
const directory = await mkdtemp(join(tmpdir(), "pi-package-"));
const managed = await mkdtemp(join(tmpdir(), "pi-extension-package-"));
try {
	const { stdout } = await run("npm", ["pack", "--json", "--pack-destination", directory], { cwd: root });
	const [archive] = JSON.parse(stdout);
	assert(archive.files.some((file) => file.path === "dist/workflow.js"));
	assert(!archive.files.some((file) => file.path.startsWith("examples/") || file.path.endsWith(".py")));
	await writeFile(join(directory, "package.json"), JSON.stringify({ private: true, type: "module" }));
	await run(
		"npm",
		[
			"install",
			"--ignore-scripts",
			"--legacy-peer-deps=false",
			"--save-exact",
			join(directory, archive.filename),
			"@temporalio/worker@1.24.0",
			"@temporalio/testing@1.24.0",
			"@earendil-works/pi-ai@1.0.4",
			"@earendil-works/pi-durable@1.0.4",
			"@earendil-works/chord@1.0.4",
		],
		{ cwd: directory },
	);
	await writeFile(
		join(directory, "workflows.js"),
		`import { runTemporalAgent, createTemporalModels } from "@h0rv/pi-durable-temporal/workflow";
import { createRegistry } from "@earendil-works/pi-durable";
const model = { id:"smoke", name:"Smoke", provider:"faux", api:"faux", baseUrl:"http://localhost", reasoning:false, input:["text"], contextWindow:10000, maxTokens:100, cost:{input:0,output:0,cacheRead:0,cacheWrite:0} };
export async function smoke() {const result = await runTemporalAgent({type:"input",content:"Hello"}, {registry:createRegistry(), models:createTemporalModels([model]), agent:{model:{provider:"faux",modelId:"smoke"}}});return result.context.messages.at(-1).content[0].text;}
`,
	);
	await writeFile(
		join(directory, "smoke.mjs"),
		`import assert from "node:assert/strict";
import { createModelActivities } from "@h0rv/pi-durable-temporal";
import { createModels } from "@earendil-works/pi-ai/models";
import { fauxProvider, fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { Worker } from "@temporalio/worker";
import { Client } from "@temporalio/client";
import { ExternalStorage, StorageDriverClaim } from "@temporalio/common";
const payloads = new Map();
const driver = {name:"smoke",type:"smoke",async store(_context,values) {return values.map(value=>{const key=String(payloads.size);payloads.set(key,value);return new StorageDriverClaim({key});});},async retrieve(_context,claims) {return claims.map(claim=>{assert(payloads.has(claim.claimData.key));return payloads.get(claim.claimData.key);});}};
const dataConverter={externalStorage:new ExternalStorage({drivers:[driver],payloadSizeThreshold:0})};
const globals = {crypto:globalThis.crypto,WeakRef:globalThis.WeakRef};
await assert.rejects(import("@h0rv/pi-durable-temporal/workflow"), /only inside a Temporal workflow bundle/);
assert.equal(globalThis.crypto,globals.crypto);assert.equal(globalThis.WeakRef,globals.WeakRef);
const faux=fauxProvider({models:[{id:"smoke"}]});faux.setResponses([fauxAssistantMessage("Hello")]);
const models=createModels();models.setProvider(faux.provider);
const path=process.env.TEMPORAL_CLI_PATH;
const env=await TestWorkflowEnvironment.createLocal(path?{server:{executable:{type:"existing-path",path}}}:undefined);
try {const client=new Client({connection:env.client.connection,dataConverter});const worker=await Worker.create({connection:env.nativeConnection,dataConverter,taskQueue:"smoke",workflowsPath:new URL("./workflows.js",import.meta.url).pathname,activities:createModelActivities(models)});
await worker.runUntil(async()=>{const handle=await client.workflow.start("smoke",{workflowId:"smoke",taskQueue:"smoke",workflowExecutionTimeout:"15 seconds"});assert.equal(await handle.result(),"Hello");assert(payloads.size>0);const history=await handle.fetchHistory();const result=history.events.find(event=>event.workflowExecutionCompletedEventAttributes).workflowExecutionCompletedEventAttributes.result.payloads[0];assert.equal(new TextDecoder().decode(result.metadata.messageType),"temporal.api.sdk.v1.ExternalStorageReference");});} finally {await env.teardown();}
`,
	);
	const result = await run(process.execPath, ["smoke.mjs"], { cwd: directory, timeout: 90_000, maxBuffer: 4_000_000 });
	assert(!result.stderr.includes("DeterminismViolationError"));
	await writeFile(join(managed, "package.json"), JSON.stringify({ private: true, type: "module" }));
	await run(
		"npm",
		["install", "--ignore-scripts", "--omit=dev", "--legacy-peer-deps", join(directory, archive.filename)],
		{ cwd: managed },
	);
	const loaded = await discoverAndLoadExtensions(
		[join(managed, "node_modules/@h0rv/pi-durable-temporal/dist/pi-extension.js")],
		managed,
		managed,
	);
	assert.deepEqual(loaded.errors, []);
	assert.equal(loaded.extensions.length, 1);
	console.log(`Packed package passed a workflow run in a fresh project (${archive.size} bytes).`);
	console.log("Packed extension loaded with Pi's managed-install settings.");
} finally {
	await rm(directory, { recursive: true, force: true });
	await rm(managed, { recursive: true, force: true });
}
