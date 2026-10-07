# Install

The [release archive](https://github.com/h0rv/pi-durable-temporal/releases/tag/v0.1.0) can be installed without a registry login. See the command in the [README](../README.md).

The package is also published to GitHub Packages as `@h0rv/pi-durable-temporal`. GitHub requires authentication to install npm packages, including public ones. Use a classic token with `read:packages` when prompted for the password:

```sh
npm login --scope=@h0rv --auth-type=legacy --registry=https://npm.pkg.github.com
npm install @h0rv/pi-durable-temporal@0.1.0 \
  @earendil-works/pi-durable@1.0.4 @earendil-works/pi-ai@1.0.4 \
  @earendil-works/chord@1.0.4 protobufjs@8.8.0 typebox@1.3.27
```

See [GitHub's registry instructions](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-npm-registry).

CI checks Node 22 and 24. A published release runs those checks before publishing the package and attaching its archive. The release tag must match the package version.
