# pi-durable-temporal

Temporal integration for [Pi Durable](https://github.com/earendil-works/pi/tree/main/packages/durable). Pi's harness runs in a workflow. Model calls and tools run as activities.

Experimental. Tested with Pi 1.0.4 and Temporal SDK 1.24.0.

Tested locally and on Temporal Cloud, including worker restart, Continue-as-New and history replay. The Cloud test used payload files on one machine. Shared S3 storage has not been tested.

## Install

Available on [npm](https://www.npmjs.com/package/@h0rv/pi-durable-temporal).

```sh
npm install @h0rv/pi-durable-temporal
```

[Install and publishing](docs/install.md).

## Usage

Use `createTemporalModels` for model requests and `temporalTool` for tools in your registry. Pass the models and registry to `openTemporalHarness` with your Pi options. These functions are exported from `@h0rv/pi-durable-temporal/workflow`. Register `createModelActivities(models)` and your tool activities on the worker.

- [API](docs/api.md)
- [State and checkpoints](docs/state.md)
- [Configuration](docs/configuration.md)
- [Compatibility and retry behavior](docs/compatibility.md)
- [Pi's experimental client TUI](examples/native-client/README.md)
- [Examples](docs/examples.md)

## Development

```sh
npm ci --ignore-scripts
npm run check
npm test
npm run test:package
```

MIT.
