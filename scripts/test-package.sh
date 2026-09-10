#!/usr/bin/env bash
set -euo pipefail
root=$PWD
temp=$(mktemp -d)
trap 'rm -rf "$temp"' EXIT

vp run build
cp "$root/package.json" "$temp/source-manifest.json"
cp "$root/bun.lock" "$temp/source.lock"
for version in 1.2.3 1.2.3-beta.4; do
  stage="$temp/stage-$version"
  bash scripts/release-prepare.sh --version "$version" --destination "$stage"
  node --input-type=module - "$stage/package.json" "$version" <<'JS'
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
const [path, version] = process.argv.slice(2);
const manifest = JSON.parse(readFileSync(path, 'utf8'));
assert.equal(manifest.version, version);
assert.equal(manifest.private, false);
assert.equal(manifest.publishConfig.tag, version.includes('-') ? 'beta' : 'latest');
assert.equal(manifest.scripts, undefined);
assert.equal(manifest.devDependencies, undefined);
assert.equal(manifest.peerDependencies.effect, '4.0.0-rc.112');
assert.deepEqual(Object.keys(manifest.exports), ['.', './async-await', './package.json']);
JS
  if bash scripts/release-prepare.sh --version "$version" --destination "$stage"; then exit 1; fi
done
if bash scripts/release-prepare.sh --version 0.0.0 --destination "$temp/placeholder"; then exit 1; fi
test ! -e "$temp/placeholder"
if bash scripts/release-publish.sh --version 0.0.0 --dry-run; then exit 1; fi
if bash scripts/release-publish.sh --version 1.2.3 --unknown; then exit 1; fi
# Exercise publish argument routing and cleanup without any registry mutation.
mkdir "$temp/bin"
cat > "$temp/bin/npm" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == pack ]]; then exec "$REAL_NPM" "$@"; fi
[[ "$1" == publish ]]
printf '%s\n' "$PWD" > "$PUBLISH_STAGE"
printf '%s\n' "$@" > "$PUBLISH_ARGS"
exit "${PUBLISH_EXIT:-0}"
SH
chmod +x "$temp/bin/npm"
export REAL_NPM
REAL_NPM=$(command -v npm)
export PUBLISH_STAGE="$temp/publish-stage" PUBLISH_ARGS="$temp/publish-args"
PATH="$temp/bin:$PATH" bash scripts/release-publish.sh --version 1.2.3
grep -qx latest "$PUBLISH_ARGS"
if grep -qx -- --dry-run "$PUBLISH_ARGS"; then exit 1; fi
test ! -d "$(cat "$PUBLISH_STAGE")"
PATH="$temp/bin:$PATH" bash scripts/release-publish.sh --version 1.2.3-rc.2 --dry-run
grep -qx rc "$PUBLISH_ARGS"
grep -qx -- --dry-run "$PUBLISH_ARGS"
test ! -d "$(cat "$PUBLISH_STAGE")"
if PATH="$temp/bin:$PATH" PUBLISH_EXIT=42 bash scripts/release-publish.sh --version 1.2.3 --dry-run; then exit 1; fi
test ! -d "$(cat "$PUBLISH_STAGE")"
cmp "$root/package.json" "$temp/source-manifest.json"
cmp "$root/bun.lock" "$temp/source.lock"
bash scripts/release-prepare.sh --version 0.1.0-rc.1 --destination "$temp/package"
(cd "$temp/package" && npm pack --ignore-scripts --pack-destination "$temp")
mkdir "$temp/consumer"
cd "$temp/consumer"
printf '{"private":true,"type":"module"}\n' > package.json
npm install --ignore-scripts --no-audit --no-fund --package-lock=false "$temp"/*.tgz effect@4.0.0-rc.112

# Compile the actual README examples, not a separately maintained approximation.
awk '/^```ts$/{code=1;next} /^```$/{code=0;next} code{print}' "$root/README.md" > readme.mts
cat > contracts.mts <<'TS'
import { DurableStreamsClient, type ReadError, type IdempotentProducer } from '@humanlayer/effect-durable-streams-client';
import { Context, Effect, Schema, SchemaGetter, Stream, type Scope } from 'effect';
import type { HttpClient } from 'effect/unstable/http';
import { DurableStream, IdempotentProducer as AsyncProducer, makeEffectClient } from '@humanlayer/effect-durable-streams-client/async-await';
const handle = new DurableStream({url: 'https://example.com', contentType: 'application/json'});
const asyncProducer = new AsyncProducer(handle, 'writer');
const admission: void = asyncProducer.append('{"id":"one"}');
const ordinary: Promise<void> = handle.append('{"id":"one"}');
const writable: WritableStream<string | Uint8Array> = handle.writable();
const writer = writable.getWriter();
// @ts-expect-error Serialized writable inputs exclude objects.
handle.writable().getWriter().write({ id: 'wrong' });
// @ts-expect-error Serialized writable inputs exclude numbers.
handle.writable().getWriter().write(42);
const head = await handle.head();
if (head.exists) {
  const etag: string | undefined = head.etag;
  const cacheControl: string | undefined = head.cacheControl;
}
// @ts-expect-error Unchecked generic JSON reads are intentionally unsupported.
handle.stream<{id: string}>();
// @ts-expect-error Producer admission has no delivery Promise.
const delivery: Promise<void> = asyncProducer.append('1');
// @ts-expect-error Ordinary append cannot send producer tuples.
handle.append('1', {producerId: 'wrong'});
const typed = DurableStream.withSchema({url: 'https://example.com', schema: Schema.Struct({id: Schema.String})});
typed.appendJson({id: 'ok'});
typed.append('[1,2]');
const union = DurableStream.withSchema({url: 'https://example.com', schema: Schema.Union([Schema.Struct({type: Schema.Literal('added'), id: Schema.String}), Schema.Struct({type: Schema.Literal('deleted'), count: Schema.Number})])});
union.appendJson({type: 'added', id: 'ok'});
union.appendJson({type: 'deleted', count: 1});
// @ts-expect-error Discriminated schemas retain variant-specific fields.
union.appendJson({type: 'added', count: 1});
// @ts-expect-error Schema inference cannot be replaced with arbitrary values.
typed.appendJson({id: 1});
// @ts-expect-error Typed writable requires the explicit raw handle.
typed.writable();
class Encoder extends Context.Service<Encoder, {suffix: string}>()('Encoder') {}
class Decoder extends Context.Service<Decoder, {prefix: string}>()('Decoder') {}
const codec = Schema.String.pipe(Schema.decodeTo(Schema.Struct({id: Schema.String}), {
  decode: SchemaGetter.transformOrFail((id) => Decoder.pipe(Effect.map((s) => ({id: s.prefix + id})))),
  encode: SchemaGetter.transformOrFail((value) => Encoder.pipe(Effect.map((s) => value.id + s.suffix)))
}));
// @ts-expect-error Serviceful schemas need the advanced Effect boundary.
DurableStream.withSchema({url: 'https://example.com', schema: codec});
const advanced = makeEffectClient({url: 'https://example.com', schema: codec});
// @ts-expect-error Both encoding and decoding services are required.
const missingDecoder: Effect.Effect<unknown, unknown, HttpClient.HttpClient | Scope.Scope | Encoder> = advanced;
// @ts-expect-error Both encoding and decoding services are required.
const missingEncoder: Effect.Effect<unknown, unknown, HttpClient.HttpClient | Scope.Scope | Decoder> = advanced;
// @ts-expect-error Advanced transport must be ambient, not a competing Fetch override.
makeEffectClient({url: 'https://example.com', schema: Schema.Json, fetch});
Effect.gen(function* () {
  const client = yield* advanced;
  // @ts-expect-error Advanced reads cannot override ambient HTTP with Fetch.
  client.stream({fetch});
  // @ts-expect-error Dynamic auth belongs on ambient HTTP in advanced mode.
  client.stream({headers: {Authorization: () => 'token'}});
});
const configured = DurableStreamsClient.make({url: 'https://example.com', schema: Schema.Struct({id: Schema.String})});
Effect.gen(function* () {
  const client = yield* configured;
  const read: Stream.Stream<{readonly id: string}, ReadError, HttpClient.HttpClient> = client.json;
  const producer: Effect.Effect<IdempotentProducer<{readonly id: string}>, unknown, HttpClient.HttpClient | Scope.Scope> = client.producer({producerId: 'writer'});
  // @ts-expect-error Custom schema write input must not widen to arbitrary JSON.
  client.append({value: {id: 1}});
  // @ts-expect-error No public transport constructor argument.
  DurableStreamsClient.make({url: 'https://example.com', fetch});
  // @ts-expect-error Built-in contextual service cannot specialize its schema.
  DurableStreamsClient.layer({url: 'https://example.com', schema: Schema.String});
  // @ts-expect-error HTTP requirement remains ambient.
  const runnable: Effect.Effect<unknown, unknown> = Stream.runCollect(read);
  return {producer, runnable};
});
// @ts-expect-error Internal modules are not package exports.
import '@humanlayer/effect-durable-streams-client/transport';
// @ts-expect-error The superseded facade name is deliberately not exported.
import '@humanlayer/effect-durable-streams-client/client';
TS
"$root/node_modules/.bin/tsc" --ignoreConfig --noEmit --strict --skipLibCheck --noUncheckedSideEffectImports --target es2023 --module nodenext --moduleResolution nodenext readme.mts contracts.mts

"$root/node_modules/.bin/tsc" --ignoreConfig --noEmit --strict --skipLibCheck --noUncheckedSideEffectImports --target es2023 --module esnext --moduleResolution bundler readme.mts contracts.mts

# No Node ambient types and no skipLibCheck: validate the shipped declaration graph.
cat > tsconfig.dom.json <<'JSON'
{"compilerOptions":{"strict":true,"noEmit":true,"types":[],"lib":["ES2023","ESNext.Disposable","DOM","DOM.Iterable","DOM.AsyncIterable"],"target":"ES2023","module":"NodeNext","moduleResolution":"NodeNext","noUncheckedSideEffectImports":true},"files":["contracts.mts"]}
JSON
"$root/node_modules/.bin/tsc" -p tsconfig.dom.json

cat > runtime.mjs <<'JS'
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { Context, Effect, Layer, Schema, SchemaGetter, Stream } from 'effect';
import { FetchHttpClient } from 'effect/unstable/http';
import * as sdk from '@humanlayer/effect-durable-streams-client';
const { DurableStreamsClient } = sdk;
const manifest = JSON.parse(await readFile(new URL('./node_modules/@humanlayer/effect-durable-streams-client/package.json', import.meta.url)));
assert.equal(manifest.name, '@humanlayer/effect-durable-streams-client');
assert.equal(manifest.version, '0.1.0-rc.1');
assert.equal(manifest.private, false);
assert.deepEqual(Object.keys(manifest.exports).sort(), ['.', './async-await', './package.json']);
assert.equal(manifest.peerDependencies.effect, '4.0.0-rc.112');
assert.equal(manifest.dependencies?.effect, undefined);
for (const internal of ['acquireProducer', 'inspectStream', 'allocateRead', 'requestRetrySchedule']) assert.equal(internal in sdk, false);
for (const path of ['transport', 'src/index.ts', 'dist/index.mjs', 'client']) {
  await assert.rejects(import(`${manifest.name}/${path}`), {code: process.versions.bun ? 'ERR_MODULE_NOT_FOUND' : 'ERR_PACKAGE_PATH_NOT_EXPORTED'});
}
const base = `${process.env.STREAM_URL}/${process.env.RUNTIME_ID}`;
const { DurableStream, IdempotentProducer: AsyncProducer } = await import('@humanlayer/effect-durable-streams-client/async-await');
class MissingService extends Context.Service()('missing-schema-service') {}
const erasedSchema = Schema.String.pipe(Schema.decodeTo(Schema.String, {
  decode: SchemaGetter.transformOrFail((value) => MissingService.pipe(Effect.as(value))),
  encode: SchemaGetter.transformOrFail((value) => MissingService.pipe(Effect.as(value)))
}));
const erasedClient = DurableStream.withSchema({url: `${base}/erased-schema`, schema: erasedSchema});
await assert.rejects(erasedClient.appendJson('value'), {name: 'ClientInternalError', code: 'INTERNAL_ERROR'});
const handle = await DurableStream.create({url: `${base}/packed-async`, contentType: 'application/json'});
await handle.append('{"id":"ordinary"}');
const asyncProducer = new AsyncProducer(handle, 'packed-writer');
assert.equal(asyncProducer.append('{"id":"producer"}'), undefined);
await asyncProducer.close();
const response = await handle.stream({live: false});
assert.deepEqual(await response.json(), [{id: 'ordinary'}, {id: 'producer'}]);
await response.closed;
await Effect.runPromise(Effect.gen(function* () {
  const client = yield* DurableStreamsClient.make({url: `${base}/package-json`, contentType: 'application/json', schema: Schema.Struct({id: Schema.String})});
  yield* client.create({});
  const producer = yield* client.producer({producerId: 'package-writer'});
  yield* Stream.make({id: 'one'}, {id: 'two'}).pipe(Stream.run(producer.sink));
  yield* producer.detach;
  assert.equal((yield* client.head).closed, false);
  assert.deepEqual(yield* client.json.pipe(Stream.runCollect), [{id: 'one'}, {id: 'two'}]);
  assert.equal(yield* client.json.pipe(Stream.runCollect, Effect.catchTag('AlreadyConsumedError', () => Effect.succeed('consumed'))), 'consumed');
  for (const view of ['bytes', 'text']) {
    const c = yield* DurableStreamsClient.make({url: `${base}/package-${view}`, contentType: 'text/plain'});
    yield* c.create({value: 'hello', closed: true});
    const chunks = yield* c[view].pipe(Stream.runCollect);
    assert.equal(view === 'text' ? chunks.join('') : new TextDecoder().decode(chunks[0]), 'hello');
  }
}).pipe(Effect.scoped, Effect.provide(FetchHttpClient.layer)));
await Effect.runPromise(Effect.gen(function* () {
  const c = yield* DurableStreamsClient;
  assert.deepEqual(yield* c.json.pipe(Stream.runCollect), [{id: 'one'}, {id: 'two'}]);
}).pipe(Effect.provide(Layer.merge(DurableStreamsClient.layer({url: `${base}/package-json`}), FetchHttpClient.layer))));
JS
STREAM_URL="$1" RUNTIME_ID=node node runtime.mjs
STREAM_URL="$1" RUNTIME_ID=bun bun runtime.mjs

cat > browser.mjs <<'JS'
import { DurableStream } from '@humanlayer/effect-durable-streams-client/async-await';
globalThis.facadeSmoke = async () => {
  let fetchCalled = false;
  const response = await new DurableStream({url: 'https://example.test/browser', fetch: async () => {
    fetchCalled = true;
    return new Response('[1,2]', {headers: {'content-type': 'application/json', 'stream-next-offset': 'tail', 'stream-up-to-date': 'true'}});
  }}).stream();
  const reader = response.jsonStream().getReader();
  if ((await reader.read()).value !== 1) throw new Error('Unexpected browser payload');
  await reader.cancel();
  await response.closed;
  if (!fetchCalled) throw new Error('Injected Fetch was not called');
  if (response.offset !== '-1') throw new Error('Unsafe browser checkpoint');
  document.body.textContent = 'BROWSER_PASS';
};
globalThis.facadeSmoke().catch((error) => { document.body.textContent = `BROWSER_FAIL: ${error?.stack ?? error}`; });
JS
bun build browser.mjs --target=browser --outfile=browser.js
printf '<!doctype html><body><script src="browser.js"></script></body>' > browser.html
chrome=${CHROME_BIN:-/Applications/Google Chrome.app/Contents/MacOS/Google Chrome}
if [[ -x "$chrome" ]]; then
  CHROME_EXEC="$chrome" CHROME_PROFILE="$temp/chrome" node --input-type=module <<'JS'
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
const child = spawn(process.env.CHROME_EXEC, ['--headless=new', '--no-first-run', '--no-default-browser-check', '--disable-background-networking', '--disable-component-update', '--disable-dev-shm-usage', '--no-sandbox', '--disable-gpu', '--remote-debugging-port=0', '--remote-allow-origins=*', `--user-data-dir=${process.env.CHROME_PROFILE}`, '--allow-file-access-from-files', 'about:blank'], {detached: true, stdio: ['ignore', 'ignore', 'pipe']});
let diagnostics = '';
const stop = () => { try { process.kill(-child.pid, 'SIGTERM'); } catch (error) { if (error.code !== 'ESRCH') throw error; } };
const endpoint = new Promise((resolve, reject) => {
  child.once('error', reject);
  child.once('close', (code) => reject(new Error(`Chrome exited before CDP was ready (${code})\n${diagnostics}`)));
  child.stderr.on('data', (chunk) => {
    diagnostics += chunk;
    const match = diagnostics.match(/DevTools listening on (ws:\/\/[^\s]+)/);
    if (match) resolve(match[1]);
  });
});
const deadline = Date.now() + 15000;
const timeout = setTimeout(stop, 15000);
try {
  const webSocket = new WebSocket(await endpoint);
  await new Promise((resolve, reject) => {
    webSocket.addEventListener('open', resolve, {once: true});
    webSocket.addEventListener('error', () => reject(new Error(`Could not connect to Chrome CDP\n${diagnostics}`)), {once: true});
    webSocket.addEventListener('close', () => reject(new Error(`Chrome CDP closed before connecting\n${diagnostics}`)), {once: true});
  });
  let nextId = 0;
  const pending = new Map();
  webSocket.addEventListener('message', ({data}) => {
    const message = JSON.parse(data);
    if (message.id === undefined) return;
    const waiter = pending.get(message.id);
    if (!waiter) return;
    pending.delete(message.id);
    if (message.error) waiter.reject(new Error(JSON.stringify(message.error)));
    else waiter.resolve(message.result);
  });
  webSocket.addEventListener('close', () => {
    for (const waiter of pending.values()) waiter.reject(new Error(`Chrome CDP closed before the browser smoke completed\n${diagnostics}`));
    pending.clear();
  }, {once: true});
  const send = (method, params = {}, sessionId) => new Promise((resolve, reject) => {
    const id = ++nextId;
    pending.set(id, {resolve, reject});
    webSocket.send(JSON.stringify({id, method, params, ...(sessionId ? {sessionId} : {})}));
  });
  const {targetId} = await send('Target.createTarget', {url: `file://${process.cwd()}/browser.html`});
  const {sessionId} = await send('Target.attachToTarget', {targetId, flatten: true});
  await send('Runtime.enable', {}, sessionId);
  let body = '';
  while (Date.now() < deadline) {
    const evaluation = await send('Runtime.evaluate', {expression: 'document.body.textContent', returnByValue: true}, sessionId);
    body = evaluation.result.value ?? '';
    if (body.includes('BROWSER_PASS')) break;
    if (body.includes('BROWSER_FAIL')) throw new Error(`${body}\n${diagnostics}`);
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  assert.ok(body.includes('BROWSER_PASS'), `${body}\n${diagnostics}`);
  await send('Browser.close').catch(() => {});
  webSocket.close();
  console.log('Browser injected Fetch/Web cancellation passed');
} finally {
  clearTimeout(timeout);
  stop();
}
JS
else
  echo 'Browser execution unavailable; browser bundle only' >&2
fi

mkdir "$temp/auto-peer"
(
  cd "$temp/auto-peer"
  printf '{"private":true,"type":"module"}\n' > package.json
  npm install --ignore-scripts --no-audit --no-fund --package-lock=false "$temp"/*.tgz
  node --input-type=module -e "import assert from 'node:assert/strict'; import manifest from 'effect/package.json' with {type:'json'}; assert.equal(manifest.version, '4.0.0-rc.112'); await import('@humanlayer/effect-durable-streams-client/async-await');"
)
mkdir "$temp/missing-peer"
(
  cd "$temp/missing-peer"
  printf '{"private":true,"type":"module"}\n' > package.json
  npm install --legacy-peer-deps --ignore-scripts --no-audit --no-fund --package-lock=false "$temp"/*.tgz
  node --input-type=module -e "import assert from 'node:assert/strict'; await assert.rejects(import('@humanlayer/effect-durable-streams-client/async-await'), {code:'ERR_MODULE_NOT_FOUND'});"
)
mkdir "$temp/incompatible-peer"
(
  cd "$temp/incompatible-peer"
  printf '{"private":true,"type":"module"}\n' > package.json
  if npm install --strict-peer-deps --ignore-scripts --no-audit --no-fund --package-lock=false "$temp"/*.tgz effect@3.21.0 > "$temp/incompatible.log" 2>&1; then
    echo 'Incompatible Effect peer was accepted' >&2
    exit 1
  fi
  grep -q ERESOLVE "$temp/incompatible.log"
)

# Only distribution files and npm's mandatory package documents may ship.
tar -tzf "$temp"/*.tgz > "$temp/contents"
if grep -Ev '^package/(dist/[^/]+|package.json|README.md|LICENSE)$' "$temp/contents"; then
  echo 'Unexpected packed file' >&2
  exit 1
fi
echo 'Package consumption passed'
