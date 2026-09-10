#!/usr/bin/env bash
set -euo pipefail
root=$PWD
temp=$(mktemp -d)
trap 'rm -rf "$temp"' EXIT

cd "$temp"
printf '{"version":"0.0.0","private":true}\n' > package.json
for version in 0.1.0-rc.1 1.2.3-beta.4 1.2.3; do
  output=$(bash "$root/scripts/release-tag-info.sh" "v$version")
  case "$version" in
    *-rc.*) expected=rc ;;
    *-beta.*) expected=beta ;;
    *) expected=latest ;;
  esac
  [[ "$output" == "$(printf 'version=%s\ntag=%s' "$version" "$expected")" ]]
  [[ "$(bash "$root/scripts/release-tag-info.sh" --version "$version")" == "$output" ]]
  [[ "$(GITHUB_REF_NAME="v$version" bash "$root/scripts/release-tag-info.sh")" == "$output" ]]
done
for version in 0.0.0 01.2.3 1.2.3-rc.01 1.2.3-latest.1 1.2.3-1 1.2.3+build; do
  if bash "$root/scripts/release-tag-info.sh" "v$version"; then exit 1; fi
done
if bash "$root/scripts/release-tag-info.sh" --version; then exit 1; fi
if bash "$root/scripts/release-tag-info.sh" --version 1.2.3 --unexpected; then exit 1; fi
GITHUB_OUTPUT="$temp/output" bash "$root/scripts/release-tag-info.sh" v2.3.4-rc.5
[[ "$(cat "$temp/output")" == "$(printf 'version=2.3.4-rc.5\ntag=rc')" ]]
cd "$root"

# Exercise the pinned runner's finally path with a real SDK adapter and a rejected init.
ROOT="$root" TEMP="$temp" node --input-type=module <<'JS'
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { writeFile, readFile } from 'node:fs/promises';
import { createConnection } from 'node:net';
const { ROOT: root, TEMP: temp } = process.env;
await writeFile(`${temp}/reject.mjs`, `process.stdin.on('data', () => process.stdout.write('{"type":"error","success":false,"commandType":"init","errorCode":"INVALID_ARGUMENT","message":"intentional cleanup probe"}\\n'));`);
for (const fail of [false, true]) {
  const pidFile = `${temp}/adapter.pid`;
  const wrapper = `${temp}/adapter.sh`;
  await writeFile(wrapper, `#!/bin/bash\necho $$ > '${pidFile}'\nexec ${fail ? `node '${temp}/reject.mjs'` : `bash '${root}/tests/conformance/run-adapter.sh'`}\n`, {mode: 0o700});
  const child = spawn('node', ['node_modules/@durable-streams/client-conformance-tests/dist/cli.js', '--run', wrapper, '--tag', 'package-cleanup-probe'], {cwd: root});
  let output = '';
  child.stdout.on('data', (data) => { output += data; });
  child.stderr.on('data', (data) => { output += data; });
  const code = await new Promise((resolve, reject) => {
    child.on('error', reject);
    child.on('close', resolve);
  });
  assert.equal(code, fail ? 1 : 0, output);
  const url = output.match(/Reference server started at (http:\/\/[^\s]+)/)?.[1];
  assert.ok(url, output);
  const {hostname, port} = new URL(url);
  await new Promise((resolve, reject) => {
    const socket = createConnection({host: hostname, port: Number(port)});
    socket.once('connect', () => { socket.destroy(); reject(new Error('Conformance server still listening')); });
    socket.once('error', (error) => {
      if (error.code === 'ECONNREFUSED') resolve(); else reject(error);
    });
  });
  const pid = Number((await readFile(pidFile, 'utf8')).trim());
  assert.throws(() => process.kill(pid, 0), {code: 'ESRCH'});
}
JS
echo 'Release validation passed'
