#!/usr/bin/env bash
set -euo pipefail

# Resolve independently of the source manifest, as in Fold.
if [[ $# == 2 && "$1" == --version ]]; then
  version=$2
elif [[ $# -le 1 && "${1:-${GITHUB_REF_NAME:-}}" == v* ]]; then
  ref=${1:-${GITHUB_REF_NAME:-}}
  version=${ref#v}
else
  echo 'Provide vVERSION, GITHUB_REF_NAME, or --version VERSION' >&2
  exit 1
fi
if [[ ! "$version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-([a-z][a-z0-9-]*)\.(0|[1-9][0-9]*))?$ ]]; then
  echo "Use X.Y.Z or X.Y.Z-channel.N (for example 0.1.0-rc.1)" >&2
  exit 1
fi
tag=${BASH_REMATCH[5]:-latest}
if [[ "$version" == 0.0.0 ]]; then
  echo 'The source placeholder 0.0.0 cannot be released' >&2
  exit 1
fi
if [[ "$version" == *-* && "$tag" == latest ]]; then
  echo "Prereleases cannot use the latest dist-tag" >&2
  exit 1
fi
printf 'version=%s\ntag=%s\n' "$version" "$tag"
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  printf 'version=%s\ntag=%s\n' "$version" "$tag" >> "$GITHUB_OUTPUT"
fi
