# Publishing

This is a single ESM package, `@humanlayer/effect-durable-streams-client`. There is no monorepo version rewriting, staging directory, binary matrix, or dependency publication order. The pattern follows Fold's `docs/publishing.md`, `.github/workflows/release.yml`, and `scripts/release/{tag-info,publish}.ts`, adapted to the existing Vite+ build.

## Release contract

- `package.json` contains the release version; initially `0.1.0-rc.1`.
- `.github/workflows/release.yml` runs only on pushed `v*` tags. `v0.1.0-rc.2` must point at a commit whose manifest already says `0.1.0-rc.2`. CI refuses mismatches; it never edits the version.
- Supported versions are `X.Y.Z` or `X.Y.Z-channel.N`, with no leading numeric zeroes. Stable versions publish to `latest`; prereleases publish to their first identifier (`rc`, `beta`, etc.). A prerelease channel named `latest` is rejected. Build metadata and numeric-only prerelease channels are deliberately not supported.
- `publishConfig.tag` defaults to `rc` to protect accidental local RC publication. CI always supplies the derived `--tag` explicitly. Set that default appropriately when preparing a stable release; do not rely on npm's default `latest` for prereleases.
- The checked-in export map permits only the root and `package.json`; the build does not rewrite it. `dist/index.mjs` and `dist/index.d.mts` are the runtime/declaration entry points. Effect stays external and remains the exact runtime dependency `4.0.0-rc.112`. Peer policy and a `/client` facade are Phase 9, not part of this release.
- `devEngines.packageManager` still records Bun 1.4.0, but uses `onFail: ignore` so npm can pack/publish the same manifest. npm rejects the Vite+ starter's `onFail: download` with `EBADDEVENGINES` when the selected development manager is Bun. Builds/installations still use Vite+ and CI explicitly installs Bun 1.4.0.

## First local publication (owner action only)

Run from the repository root on the reviewed release commit with Node 24, Bun 1.4.0 and Vite+ installed. Confirm the repository really is `humanlayer/effect-durable-streams-client`, the version is unused, and all manual release reviews are complete. Commit preparation and tag/push operations are separate maintainer actions; none of the commands below creates a commit or tag.

```bash
npm install --global npm@^11.15.0
vp install --frozen-lockfile
vp run check
vp run diff:check
vp test
vp run test:integration
vp run test:conformance
vp run build
vp run test:package
vp run release:tag-info v0.1.0-rc.1
git diff --exit-code
npm pack --dry-run --ignore-scripts
npm publish --dry-run --ignore-scripts --access public --tag rc
```

`test:package` rebuilds and packs into a temporary directory, installs that tarball into an isolated consumer with lifecycle scripts disabled, typechecks the actual README blocks and negative public-contract assertions, then runs native ESM against a real ephemeral server. It needs npm registry access for the exact Effect dependency. It removes its temporary files on exit. `--ignore-scripts` is intentional for pack/publish after explicit validation/build: do not run publication commands against missing or stale `dist`.

Review `npm view @humanlayer/effect-durable-streams-client versions --json`. An E404 may mean the package is new; other registry/auth/network errors do not establish availability. Then authenticate as an npm account authorized to create public packages in `@humanlayer`, with 2FA enabled:

```bash
npm login --registry=https://registry.npmjs.org/
npm whoami --registry=https://registry.npmjs.org/
npm publish --ignore-scripts --access public --tag rc
npm view @humanlayer/effect-durable-streams-client@0.1.0-rc.1 version
npm view @humanlayer/effect-durable-streams-client dist-tags --json
```

Do not use `--provenance` locally: the local machine is not the trusted CI identity. npm versions are immutable. If a publish response is ambiguous, verify the exact version in the registry before retrying; do not assume an error means nothing was published. Allow for registry propagation. Fold reports that initial publication can also establish `latest` despite an explicit prerelease tag; inspect the actual dist-tags rather than assuming `latest` is absent. Do not advertise a stable release or try to remove the only version's mandatory tag blindly.

## Register the trusted publisher after the package exists

The official npm pages were checked during implementation:

- [Trusted publishing requirements](https://docs.npmjs.com/trusted-publishers/): npm ≥11.5.1, Node ≥22.14.0, GitHub-hosted runners, and `id-token: write`.
- [`npm trust` command](https://docs.npmjs.com/cli/v11/commands/npm-trust): npm ≥11.15.0, existing package, package write permission, account-level 2FA, and interactive authentication (not a bypass-2FA granular token). `--allow-publish` authorizes this workflow's direct `npm publish`.

Using the authenticated owner session from the first publish:

```bash
npm trust github @humanlayer/effect-durable-streams-client \
  --repository humanlayer/effect-durable-streams-client \
  --file release.yml \
  --allow-publish \
  --yes
npm trust list @humanlayer/effect-durable-streams-client
```

This is a registry mutation and requires approval/2FA; `--yes` does not bypass 2FA. Use only the workflow filename `release.yml`, not `.github/workflows/release.yml`. Leave the environment unspecified because this workflow declares no GitHub environment. If adding a protected environment later, update both workflow and trust configuration to the same exact name. `repository.url` must match the GitHub repository. Saving trust does not verify that a future workflow can publish.

The npm UI alternative is the package's Settings → Trusted publishing → GitHub Actions: organization `humanlayer`, repository `effect-durable-streams-client`, file `release.yml`, no environment, allow direct publish. Do not add an npm publish token to GitHub. After a successful OIDC release, consider selecting “Require two-factor authentication and disallow tokens” in Publishing access, and remove obsolete automation tokens. Protect release tags with repository rulesets.

## Later releases

Prepare an unused version in `package.json`, update the lockfile if dependencies changed, run all gates, and have a maintainer commit/review it. Then the maintainer pushes the matching `vVERSION` tag. Do not push `v0.1.0-rc.1` to automate the already-published bootstrap version: the single-package workflow intentionally fails rather than treating an existing immutable npm version as a verified release.

CI installs dependencies with the frozen lockfile, runs static/unit/integration/conformance/package gates, verifies the tracked tree was not rewritten, inspects publication, and publishes using OIDC. The workflow has a 30-minute ceiling, no dependency cache, no token, no manual version override, and no GitHub Release creation step. For a pre-publish CI failure, rerun the same tagged workflow after diagnosing the issue. If npm already accepted that version, confirm it and use a new reviewed version/tag for changed content; never move an existing tag to different code.

GitHub OIDC provenance is automatically generated when both the source repository and package are public. Private source repositories cannot receive npm provenance even when the package is public. Repository visibility, npm organization permissions, trust registration, protected tags, and an actual CI publish must be verified by the owner; local tests and a dry run cannot verify those external settings. `npm whoami` is for the local login only and is not an OIDC health check.

## Conformance scope and resource ownership

The pinned runner owns the reference server and JSONL adapter. Normal success/test-failure/initialization-error paths enter its `finally` block, send shutdown (with a five-second deadline), signal the adapter, close readline, and stop the server. The SDK's adapter scope releases cached producers on shutdown/EOF; deterministic tests separately check interruption and delayed cleanup. The official runner does not await the child exit after `kill`, and server start/adapter construction occur before its `try`; no unconditional guarantee is made for process crashes, acquisition defects, SIGKILL, or CI cancellation. These upstream limitations are not repaired by changing this client's protocol implementation.

Conformance runs the source adapter over the native client; the separate tarball test exercises built ESM/declarations. Both must pass on the same release tree. Expected full-corpus skips are six unsupported auto-mode cases, two unsupported batch-item-limit validation cases, and two unconditional upstream SSE skips. Never count a skip as a pass or suppress a failing advertised capability.
