# @humanlayer/effect-durable-streams-client

An Effect v4-native Durable Streams client: typed payload streams, bidirectional schemas, ordinary append batching, and scoped idempotent producers. This is the native release candidate, not a drop-in Promise replacement for `@durable-streams/client`.

## Install

```bash
npm install @humanlayer/effect-durable-streams-client@rc effect@4.0.0-rc.112
```

ESM only, with generated TypeScript declarations. Effect `4.0.0-rc.112` is currently an exact runtime dependency (not bundled). Use the same version in your application; other Effect releases, especially Effect 3, are not claimed compatible. The future async facade and Effect peer policy are separate design work. No Node platform package is required by this library; provide a suitable Effect HTTP layer at your application's runtime boundary.

## Construct, write, and collect JSON

All TypeScript blocks below are checked against the packed package. They share these imports:

```ts
import { DurableStreamsClient } from "@humanlayer/effect-durable-streams-client";
import { Effect, Layer, Schema, Stream } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
```

```ts
const Order = Schema.Struct({ id: Schema.String, quantity: Schema.Int });

const ordersProgram = Effect.gen(function* () {
  const client = yield* DurableStreamsClient.make({
    url: "https://streams.example.com/orders",
    contentType: "application/json",
    schema: Order,
  });
  yield* client.create({});
  yield* client.append({ value: { id: "order-1", quantity: 2 } });
  const orders = yield* client.json.pipe(Stream.runCollect);
  const offset = yield* client.offset;
  return { orders, offset };
});

const ordersMain = ordersProgram.pipe(Effect.provide(FetchHttpClient.layer));
// Execute ordersMain at your application's runtime boundary.
```

`make` parses configuration and allocates local state without acquiring HTTP or starting a request. Custom schemas specialize reads and writes; encoding/decoding service requirements stay in `R`. With no schema, JSON values use `Schema.Json`. Pass decoded values, not pre-stringified objects. JSON reads emit each message from the outer protocol array; nested arrays remain individual messages. `create({ values: [...] })` atomically creates multiple messages; `value` and `values` are mutually exclusive. `create` also supports `contentType`, `closed`, and the exported `StreamLifetime` TTL/absolute-expiry variants.

## Contextual raw-JSON client

```ts
const appLive = Layer.merge(
  DurableStreamsClient.layer({
    url: "https://streams.example.com/events",
    contentType: "application/json",
  }),
  FetchHttpClient.layer,
);

const contextualMain = Effect.gen(function* () {
  const client = yield* DurableStreamsClient;
  return yield* client.json.pipe(Stream.runCollect);
}).pipe(Effect.provide(appLive));
```

The built-in service and `layer` are raw JSON only: `layer` rejects `schema`. Use `make` for a custom schema, optionally wrapped in your own application service. HTTP remains a sibling capability, not a constructor argument. Apply dynamic authentication with an effectful ambient `HttpClient` transform; transforms run on each attempt. Static extension `headers` and `params` are supported, but cannot override protocol-owned fields.

## Text, bytes, live reads, and resumption

```ts
const viewsProgram = Effect.gen(function* () {
  const textClient = yield* DurableStreamsClient.make({
    url: "https://streams.example.com/logs",
  });
  const textChunks = yield* textClient.text.pipe(Stream.runCollect);

  const byteClient = yield* DurableStreamsClient.make({
    url: "https://streams.example.com/binary",
  });
  const byteChunks = yield* byteClient.bytes.pipe(Stream.runCollect);
  return { textChunks, byteChunks };
});

const liveProgram = Effect.gen(function* () {
  const client = yield* DurableStreamsClient.make({
    url: "https://streams.example.com/events",
    offset: "now",
    live: "sse",
  });
  yield* client.json.pipe(Stream.runForEach((event) => Effect.log(event)));
});
```

Provide `FetchHttpClient.layer` (or your application HTTP layer) when running these programs. Omitted `live` means finite catch-up. Explicit `"long-poll"` or `"sse"` catches up first, then continues until remote closure, failure, or Effect interruption. Do not collect an unbounded live stream into memory. Omitted offset or `"-1"` reads history; `"now"` skips history; a saved server-issued offset resumes exactly. Offsets are opaque.

Each client permits **one read consumption total**, shared by its bytes/text/JSON views—even after completion, failure, or interruption. Make another client for another read. `client.offset` returns `Option<Offset>` and advances only after full response/control-boundary delivery; an early stop can leave the preceding checkpoint, so resumption may replay messages. Text decoding is strict UTF-8. JSON requires an array envelope; empty/malformed/singleton bodies are not silently normalized. NDJSON is bytes/text, not JSON mode.

Read consumption owns response resources and reconnect fibers. Interrupt and join the consuming Effect to cancel; there is no SDK `AbortSignal`, Web Stream, subscription callback, or runtime-disposal method.

## Tagged recovery and lifecycle

```ts
const recoveryProgram = Effect.gen(function* () {
  const client = yield* DurableStreamsClient.make({
    url: "https://streams.example.com/events",
    contentType: "application/json",
    backoffOptions: { maxRetries: 3 },
  });
  return yield* client.append({ value: { type: "updated" } }).pipe(
    Effect.catchTags({
      StreamNotFoundError: () => Effect.succeed("create the stream first" as const),
      StreamClosedError: () => Effect.succeed("choose an open stream" as const),
      AppendOutcomeUnknownError: () => Effect.succeed("reconcile before resubmitting" as const),
    }),
  );
});
```

Unhandled tags remain in the typed error channel. `head` and `connect` return `StreamMetadata` (`Existing` or `Missing`); 404 is successful `Missing` for those operations. `connect` remembers the discovered content type; `head` only inspects. `delete` deletes remotely, and `close({})` closes remotely; `close({ value })` appends a final value atomically. HTTP errors expose bounded, normalized `ErrorResponse` snapshots, not raw responses. Snapshot bodies/headers remain diagnostic data and should not be logged indiscriminately.

Ordinary buffered writes follow reference transient retries, including ambiguous transport failures: **they can duplicate accepted bytes**. Defaults are unlimited retries, 100 ms initial delay, factor 1.3, 60 s client cap, full jitter, and a Retry-After floor. Configure finite `backoffOptions.maxRetries` or impose application interruption/deadlines. HEAD/connect do not retry. Ordinary batching sends immediately when idle and coalesces only overlapping, contiguous submissions with identical execution-context bindings; different HTTP/auth contexts do not share a batch. Cancelling a participating ordinary append cancels its shared physical request.

`appendStream({ source })` accepts `Stream<Uint8Array | string, E, R>`, preserves source E/R, and sends wire chunks without schema encoding or adding a JSON envelope. A one-shot upload may retry only before body consumption starts; it cannot replay consumed input and fails rather than silently transmitting its tail.

## Scoped idempotent producer and Sink

```ts
const producerProgram = Effect.gen(function* () {
  const client = yield* DurableStreamsClient.make({
    url: "https://streams.example.com/orders",
    contentType: "application/json",
    schema: Order,
  });
  yield* client.create({});
  const producer = yield* client.producer({ producerId: "orders-writer", maxInFlight: 1 });
  yield* Stream.make({ id: "order-2", quantity: 3 }, { id: "order-3", quantity: 4 }).pipe(
    Stream.run(producer.sink),
  );
  yield* producer.flush;
  yield* producer.detach;
}).pipe(Effect.scoped);

const producerMain = producerProgram.pipe(Effect.provide(FetchHttpClient.layer));
```

Acquisition requires `Scope` and HTTP and captures that HTTP context for the producer lifetime; schema encoding services remain requirements of its writes/Sink. Defaults: epoch 0, autoClaim false, 1 MiB byte threshold, 5 ms linger, and 5 in-flight batches. The threshold is checked after insertion and can be exceeded by one unsplit value. Linger is not extended by arrivals.

`producer.append({ value })` waits for accepted/deduplicated delivery. Sequentially awaiting every append limits batching; the Sink provides batched, backpressured ingestion over the same coordinator. `flush` waits through its admission watermark and reports failures. `detach` drains without remote closure; `close({})` drains and closes remotely; `restart` drains and advances the epoch. Scope finalization cancels unresolved work and joins cleanup—it does not initiate an unbounded flush or remote close. Interrupting an append receipt wait does not cancel already-admitted producer-owned delivery.

Producer retries are **protocol recovery only** (auto-claim and local sequence-gap coordination), not ordinary transport/429/5xx backoff. Deduplication uses physical batches and retained server producer state, not logical event IDs. Do not assume a new producer with the same ID/epoch and different batching safely resumes an old writer.

## Scope and verification

No async facade, automatic live-mode selection, fork creation, server-side multi-stream protocol subscriptions, browser visibility policy, or client-managed ETag/304 entity cache is exposed. Protocol subscriptions are unrelated to consuming these payload streams. An uncached 304 is not an empty read.

```bash
vp install
vp run check
vp run diff:check
vp test
vp run test:integration
vp run test:conformance
vp run build
vp run test:package
```

Conformance uses pinned `@durable-streams/client-conformance-tests@0.2.12`: 269 cases, with 10 expected skips (six unsupported auto cases, two unsupported batch-item-count validation cases, two unconditional upstream SSE skips). Skips are not passes. Deterministic Effect tests separately cover resource ownership, service requirements, batching and interruption. See [publishing](https://github.com/humanlayer/effect-durable-streams-client/blob/main/docs/publishing.md) for first publication and subsequent tag-based OIDC releases.
