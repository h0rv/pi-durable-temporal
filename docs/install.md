# Install

The package is published to GitHub Packages as `@h0rv/pi-durable-temporal`. GitHub requires authentication to install npm packages, including public ones. Use a classic token with `read:packages` when prompted for the password:

```sh
npm login --scope=@h0rv --auth-type=legacy --registry=https://npm.pkg.github.com
npm install @h0rv/pi-durable-temporal
```

See [GitHub's registry instructions](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-npm-registry).

CI checks Node 22 and 24. A published release runs those checks before publishing to GitHub Packages. The release tag must match the package version.
