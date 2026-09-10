#!/usr/bin/env bash
set -euo pipefail
root=$PWD
temp=$(mktemp -d)
trap 'rm -rf "$temp"' EXIT

vp run build
npm pack --ignore-scripts --pack-destination "$temp"
mkdir "$temp/consumer"
cd "$temp/consumer"
printf '{"private":true,"type":"module"}\n' > package.json
npm install --ignore-scripts --no-audit --no-fund --package-lock=false "$temp"/*.tgz

# Compile the actual README examples, not a separately maintained approximation.
awk '/^```ts$/{code=1;next} /^```$/{code=0;next} code{print}' "$root/README.md" > readme.mts
cat > contracts.mts <<'TS'
import { DurableStreamsClient, type ReadError, type IdempotentProducer } from '@humanlayer/effect-durable-streams-client';
import { Effect, Schema, Stream, type Scope } from 'effect';
import type { HttpClient } from 'effect/unstable/http';
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
TS
"$root/node_modules/.bin/tsc" --ignoreConfig --noEmit --strict --skipLibCheck --noUncheckedSideEffectImports --target es2023 --module nodenext --moduleResolution nodenext readme.mts contracts.mts

STREAM_URL="$1" node --input-type=module <<'JS'
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { Effect, Layer, Schema, Stream } from 'effect';
import { FetchHttpClient } from 'effect/unstable/http';
import * as sdk from '@humanlayer/effect-durable-streams-client';
const { DurableStreamsClient } = sdk;
const manifest = JSON.parse(await readFile(new URL('./node_modules/@humanlayer/effect-durable-streams-client/package.json', import.meta.url)));
assert.equal(manifest.name, '@humanlayer/effect-durable-streams-client');
assert.deepEqual(Object.keys(manifest.exports).sort(), ['.', './package.json']);
assert.equal(manifest.dependencies.effect, '4.0.0-rc.112');
for (const internal of ['acquireProducer', 'inspectStream', 'allocateRead', 'requestRetrySchedule']) assert.equal(internal in sdk, false);
for (const path of ['transport', 'src/index.ts', 'dist/index.mjs', 'client']) {
  await assert.rejects(import(`${manifest.name}/${path}`), {code: 'ERR_PACKAGE_PATH_NOT_EXPORTED'});
}
const base = process.env.STREAM_URL;
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

# Only distribution files and npm's mandatory package documents may ship.
tar -tzf "$temp"/*.tgz > "$temp/contents"
if grep -Ev '^package/(dist/[^/]+|package.json|README.md|LICENSE)$' "$temp/contents"; then
  echo 'Unexpected packed file' >&2
  exit 1
fi
echo 'Package consumption passed'
