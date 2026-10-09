# Install

Install from public npm. No token is required:

```sh
npm install @h0rv/pi-durable-temporal
```

If you previously configured the `@h0rv` scope for GitHub Packages, point it to npm:

```sh
npm config set @h0rv:registry https://registry.npmjs.org
```

CI checks Node 22 and 24. The Release workflow runs those checks before publishing to npm. Run it manually from GitHub Actions, or publish a GitHub release whose tag matches the package version. Publishing uses the repository's `NPM_TOKEN` secret.
