#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
publish_args=(--ignore-scripts --access public)
if [[ $# == 3 && "$3" == --dry-run ]]; then
  publish_args+=(--dry-run)
elif [[ $# != 2 ]]; then
  echo 'Usage: release:publish --version VERSION [--dry-run]' >&2
  exit 1
fi
[[ "$1" == --version ]]
info=$(GITHUB_OUTPUT='' bash "$root/scripts/release-tag-info.sh" --version "$2")
tag=$(printf '%s\n' "$info" | sed -n 's/^tag=//p')
temp=$(mktemp -d)
trap 'rm -rf "$temp"' EXIT
cd "$root"
vp run build
bash scripts/release-prepare.sh --version "$2" --destination "$temp/package"
cd "$temp/package"
npm pack --ignore-scripts --pack-destination "$temp"
# Verify the actual artifact, not merely the staging input.
tar -xOf "$temp"/*.tgz package/package.json > "$temp/manifest.json"
node --input-type=module - "$temp/manifest.json" "$2" "$tag" <<'JS'
import { readFileSync } from 'node:fs';
import assert from 'node:assert/strict';
const [file, version, tag] = process.argv.slice(2);
const manifest = JSON.parse(readFileSync(file, 'utf8'));
assert.equal(manifest.version, version);
assert.notEqual(manifest.version, '0.0.0');
assert.equal(manifest.private, false);
assert.equal(manifest.publishConfig.tag, tag);
JS
npm publish "$temp"/*.tgz --tag "$tag" "${publish_args[@]}"
