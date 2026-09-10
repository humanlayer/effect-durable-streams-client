#!/usr/bin/env bash
set -euo pipefail

# The committed package is authoritative; CI never rewrites its version.
version=$(node -p 'require("./package.json").version')
ref=${1:-${GITHUB_REF_NAME:-}}
if [[ "$ref" != "v$version" ]]; then
  echo "Release tag must equal v$version; received: $ref" >&2
  exit 1
fi
if [[ ! "$version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-([a-z][a-z0-9-]*)\.(0|[1-9][0-9]*))?$ ]]; then
  echo "Use X.Y.Z or X.Y.Z-channel.N (for example 0.1.0-rc.1)" >&2
  exit 1
fi
tag=${BASH_REMATCH[5]:-latest}
if [[ "$version" == *-* && "$tag" == latest ]]; then
  echo "Prereleases cannot use the latest dist-tag" >&2
  exit 1
fi
printf 'version=%s\ntag=%s\n' "$version" "$tag"
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  printf 'version=%s\ntag=%s\n' "$version" "$tag" >> "$GITHUB_OUTPUT"
fi
