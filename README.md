# pi-durable-temporal

Temporal integration for [Pi Durable](https://github.com/earendil-works/pi/tree/main/packages/durable). Pi's harness runs in a workflow. Model calls and tools run as activities.

Experimental. Tested with Pi 1.0.4 and Temporal SDK 1.24.0.

## Install

```sh
npm install \
  https://github.com/h0rv/pi-durable-temporal/releases/download/v0.1.0/h0rv-pi-durable-temporal-0.1.0.tgz \
  @earendil-works/pi-durable@1.0.4 @earendil-works/pi-ai@1.0.4 \
  @earendil-works/chord@1.0.4 protobufjs@8.8.0 typebox@1.3.27
```

[GitHub Packages setup](docs/install.md).

## Usage

Use `createTemporalModels` for model requests and `temporalTool` for tools in your registry. Pass the models and registry to `openTemporalHarness` with your Pi options. These functions are exported from `@h0rv/pi-durable-temporal/workflow`. Register `createModelActivities(models)` and your tool activities on the worker.

- [API](docs/api.md)
- [State and checkpoints](docs/state.md)
- [Configuration](docs/configuration.md)
- [Compatibility and retry behavior](docs/compatibility.md)
- [Examples](docs/examples.md)

## Development

```sh
npm ci --ignore-scripts
npm run check
npm test
npm run test:package
```

MIT.
