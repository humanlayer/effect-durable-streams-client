# @humanlayer/effect-durable-streams-client

An Effect v4-native Durable Streams client with a Promise facade over the same protocol engines. The root exports native Effects, Streams and Sinks; `/async-await` exports JavaScript handles, callbacks and Web Streams. This release candidate is not a blanket drop-in replacement for every upstream feature.

## Install

```bash
npm install @humanlayer/effect-durable-streams-client@rc effect@4.0.0-rc.112
```

ESM only, with generated TypeScript declarations. Effect `4.0.0-rc.112` is a required exact peer dependency, external to both entries. Install that version explicitly, including in Promise-only applications. Other Effect releases, especially Effect 3, are not claimed compatible. No upstream SDK or Node platform runtime package is required. Ordinary Promise callers use Fetch by default; native callers provide a suitable Effect HTTP layer.

## Plain async/await

```ts
import {
  DurableStream,
  IdempotentProducer as AsyncProducer,
} from "@humanlayer/effect-durable-streams-client/async-await";

const asyncOrders = await DurableStream.create({
  url: "https://streams.example.com/async-orders",
  contentType: "application/json",
  body: '[{"id":"initial"}]',
});
await asyncOrders.append('{"id":"ordinary"}');
const asyncWriter = new AsyncProducer(asyncOrders, "orders-writer", {
  onError(error) {
    console.error(error.code);
  },
});
asyncWriter.append('{"id":"buffered"}'); // synchronous void admission, not delivery
await asyncWriter.flush(); // drained, not proof that every batch succeeded
await asyncWriter.close('{"id":"final"}'); // remote EOF
const asyncRead = await asyncOrders.stream({ live: false });
const asyncMessages = await asyncRead.json();
await asyncRead.closed;
```

`new DurableStream(options)` is cold. Static `create`, `connect`, `head` and `delete` perform their named operations; missing `head` returns `{ exists: false }`, and missing `connect` still returns a handle. Ordinary append accepts serialized strings/bytes (or their Promises), copies direct bytes before admission, and preserves serialized JSON lexemes. Each JSON append is one message, including a nested array; create's body is a complete initial request. Raw methods do not stringify objects or apply a configured schema.

Every `.stream()` owns an independent read session and resolves after initial headers are validated, without consuming the body. The default live mode is long-poll; `true` means long-poll, not automatic negotiation. Promise collectors stop at the first tail or closure even on live sessions. Choose exactly one of `body/text/json`, `bodyStream/textStream/jsonStream`, or `subscribeBytes/subscribeText/subscribeJson`. JSON is inferred as `JsonValue` or from a schema; there is no unchecked `json<T>()`. Callback batches include empty/control boundaries and safe offset/cursor metadata. Callbacks run sequentially and are awaited; rejection fails `closed` without acknowledging that boundary. Web streams support async iteration and acknowledge only fully delivered boundaries.

Call `cancel()`, unsubscribe, break iteration, or cancel the Web reader to stop a read. `closed` joins SDK cleanup, resolves for deliberate cancellation, and rejects for read/decode/callback failures. An ignored response still owns resources: consume or cancel it. A noncooperative user callback or custom fetch cannot be forcibly stopped, but late completion cannot acknowledge or restart work. Response headers are defensive snapshots; `statusText` is empty because the native HTTP abstraction does not expose a reason phrase.

Producer `append` throws invalid/closed admission errors synchronously and returns `undefined`. Delivery failures go to optional `onError`, once per failed physical batch. `flush` drains and does not replay those failures; `detach` stops admission and drains without EOF, suppressing drain failures. Repeated detach returns immediately. Close-request failure rejects independently. Restart advances epoch/sequence but does not reopen a detached/closed producer. Raw producer admission is not whole-input backpressure: `maxInFlight` bounds HTTP concurrency, not retained input memory. Use native Sink for backpressured ingestion.

`handle.writable()` is admission-oriented: writes do not wait for delivery, close attempts remote EOF then reports the first observed batch failure, and abort launches observed background detach/drain without promising a cleanup join. Ordinary buffered appends retain reference transient retries and can duplicate accepted bytes; producer sends use protocol recovery only, not general 429/5xx retries. There is no mandatory producer receipt, appendBatch, or dispose API.

Headers/params may contain async callbacks on ordinary handles and read options; they run per attempt. Protocol-owned keys cannot be overridden. Custom `fetch` receives the request signal. `AbortSignal` belongs at Promise operation boundaries, not the native root API.

## Optional typed facade

These examples share the Effect imports in the native section below; ordinary schema users need only import `Schema`.

```ts
const TypedEvent = Schema.Struct({ id: Schema.String });
const typedEvents = await DurableStream.createWithSchema({
  url: "https://streams.example.com/typed-events",
  schema: TypedEvent,
});
await typedEvents.appendJson({ id: "one" });
await typedEvents.appendJsonBatch([{ id: "two" }, { id: "three" }]);
const typedRead = await typedEvents.stream({ live: false });
const typedMessages = await typedRead.json(); // Array<{ readonly id: string }>
```

`DurableStream.withSchema(options)` constructs without HTTP; instance `.withSchema(schema)` specializes an existing raw handle. `appendJson` encodes one value; `appendJsonBatch` submits values in order and may have an accepted prefix if a later value fails. Raw append/create/close bypass the schema. Typed upload/writable and synchronous typed producers are deliberately omitted; use `.raw` for serialized APIs. Serviceful schemas require the advanced acquisition below rather than erasing their requirements.

```ts
import { makeEffectClient } from "@humanlayer/effect-durable-streams-client/async-await";

const advancedProgram = Effect.gen(function* () {
  const client = yield* makeEffectClient({
    url: "https://streams.example.com/advanced-events",
    schema: TypedEvent,
  });
  yield* Effect.promise(async () => {
    await client.create();
    await client.appendJson({ id: "owned-by-parent" });
    const response = await client.stream({ live: false });
    return response.json();
  });
}).pipe(Effect.scoped, Effect.provide(FetchHttpClient.layer));
```

`makeEffectClient` captures ambient HTTP and both schema encoding/decoding services. Its parent Scope must enclose all Promise use; closing the parent cancels children without disposing borrowed services. Advanced options exclude custom fetch and dynamic metadata callbacks: customize ambient HTTP instead. Ordinary constructors never accept an Effect Context, Layer or runtime.

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

Native read consumption owns response resources and reconnect fibers. Interrupt and join the consuming Effect to cancel; the root does not expose the facade's `AbortSignal`, Web Stream or callback APIs.

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

Automatic live-mode selection, fork creation, server-side multi-stream protocol subscriptions, browser visibility policy, and client-managed ETag/304 entity caching are not exposed. Protocol subscriptions are unrelated to callback consumption. An uncached 304 is not an empty read. `/client` and internal paths are intentionally not exports.

The packed-consumer harness exercises Node 24.21.0 and Vite+-managed Bun 1.4.0. It also creates a browser-target bundle and, when Chrome is installed (or `CHROME_BIN` is set), executes a headless Fetch/Web Stream cancellation smoke test. This is not a claim of cross-browser coverage or upstream Node 18 support.

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

Release versions come from Git tags, not manual package version bumps: source stays private at `0.0.0`, while `v0.1.0-rc.2` publishes a staged `0.1.0-rc.2` tarball to `rc` (`v0.1.0` uses `latest`). For a local publication preview, run `vp run release:publish --version 0.1.0-rc.1 --dry-run`; the same command without `--dry-run` is the owner-only initial publication path. Neither path changes the source manifest or lockfile.
