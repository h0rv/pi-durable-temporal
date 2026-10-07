# Replay compatibility

This adapter runs Pi's loop in Temporal's workflow sandbox. Pi's model providers and tool implementations run as activities. Temporal history is the source of completed activity results. Pi's in-memory storage is rebuilt during replay.

The workflow entry point rejects imports outside that sandbox before changing any globals. The package test checks that a rejected Node import leaves `crypto` and `WeakRef` unchanged.

## Globals

Chord uses weak references for tracking. Garbage collection cannot decide workflow behavior during replay. The adapter uses strong references in the workflow sandbox. This changes memory retention. Keep contexts and workflow runs bounded.

Pi calls `clearTimeout(undefined)`. Temporal otherwise treats that as a timer handle. The adapter ignores the undefined value. The compatibility test checks that no timer event is created.

The structured clone implementation copies Pi's plain state and typed arrays. Microtasks use promises. The performance clock uses Temporal's deterministic time. Random bytes use Temporal's seeded random sequence. These bytes are for session identifiers. They must not be used for credentials or other secrets. Provider authentication runs outside the workflow.

## Evidence

Tests replay complete histories after worker replacement and process termination. They include concurrent tools that finish in reverse order. Cancellation reaches model activities. A separate compatibility workflow checks the globals and replays its history.

The package test installs a tarball in a fresh project. It imports the worker entry point and runs a workflow from the published workflow entry point. It also checks that external storage uses references in history. Temporal SDK 1.24 requires a shared protobuf library for this. The package declares that library as a peer dependency.

## Limits

An activity interrupted before completion can run again. An external service may have received a request even when Temporal has no completed result. Tool implementations need a stable request key or a saved result to prevent repeated effects. An interrupted model request may also incur another charge on retry.

Extensions and hooks run in the workflow sandbox. They must not read the filesystem or use network clients there. Changes to running workflow code need Temporal's versioning rules. The examples use patch markers when adding approval gates and model selection to existing histories.

These tests cover the supported Pi and Temporal versions in package.json. They do not establish compatibility with later releases. Remote Temporal and S3 deployments have not been exercised. The console implements a subset of the community harness protocol.
