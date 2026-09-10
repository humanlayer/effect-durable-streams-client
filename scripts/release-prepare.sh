#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
if [[ $# != 4 || "$1" != --version || "$3" != --destination ]]; then
  echo 'Usage: release:prepare --version VERSION --destination NEW_DIRECTORY' >&2
  exit 1
fi
info=$(GITHUB_OUTPUT='' bash "$root/scripts/release-tag-info.sh" --version "$2")
version=$(printf '%s\n' "$info" | sed -n 's/^version=//p')
tag=$(printf '%s\n' "$info" | sed -n 's/^tag=//p')
# Refuse reused stages, missing builds, or unexpected source manifest changes.
for file in index.mjs index.d.mts async-await.mjs async-await.d.mts; do
  test -s "$root/dist/$file"
done
mkdir "$4"
cp -R "$root/dist" "$4/dist"
cp "$root/README.md" "$root/LICENSE" "$4/"
node --input-type=module - "$root/package.json" "$4/package.json" "$version" "$tag" <<'JS'
import { readFileSync, writeFileSync } from 'node:fs';
import assert from 'node:assert/strict';
const [source, destination, version, tag] = process.argv.slice(2);
const manifest = JSON.parse(readFileSync(source, 'utf8'));
assert.equal(manifest.version, '0.0.0');
assert.equal(manifest.private, true);
manifest.version = version;
manifest.private = false;
manifest.publishConfig = {...manifest.publishConfig, tag};
delete manifest.scripts;
delete manifest.devDependencies;
writeFileSync(destination, `${JSON.stringify(manifest, null, 2)}\n`);
JS
