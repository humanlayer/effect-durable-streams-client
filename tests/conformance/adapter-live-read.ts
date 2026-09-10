import {
  Array as Arr,
  Deferred,
  Duration,
  Effect,
  Option,
  Predicate,
  Record,
  Schema,
  Stream,
} from "effect";
import { HttpClient } from "effect/unstable/http";
import type { TestResult } from "@durable-streams/client-conformance-tests/protocol";
import { DurableStreamsClient, StreamMetadata } from "../../src/index.js";
import { AdapterState } from "./adapter-state.js";
import type { AdapterCommand } from "./adapter.js";
import { observeSse } from "./adapter-sse.js";

export const readLive = (input: Extract<AdapterCommand, { readonly type: "read" }>) =>
  Effect.gen(function* () {
    const state = yield* AdapterState;
    const acquired = yield* Deferred.make<void>();
    const stopped = yield* Deferred.make<void>();
    const collection = {
      chunks: new Array<{ data: string; offset: string }>(),
      items: new Array<string>(),
      bytes: new Array<Uint8Array>(),
      pending: false,
      checkpointBefore: Option.none<string>(),
      readOffset: Effect.succeed(Option.none<string>()),
      headers: { offset: input.offset ?? "-1", upToDate: false, streamClosed: false },
      offset: input.offset ?? "-1",
      upToDate: false,
      streamClosed: false,
      timedOut: false,
    };
    const flush = Effect.sync(() => {
      if (!collection.pending) return;
      const bytes = new Uint8Array(collection.bytes.reduce((size, part) => size + part.length, 0));
      const position = { offset: 0 };
      for (const part of collection.bytes) {
        bytes.set(part, position.offset);
        position.offset += part.length;
      }
      const data = Arr.isArrayNonEmpty(collection.items)
        ? `[${collection.items.join(",")}]`
        : new TextDecoder().decode(bytes);
      collection.offset = collection.headers.offset;
      collection.upToDate = collection.headers.upToDate;
      collection.streamClosed = collection.headers.streamClosed;
      if (data.length > 0) collection.chunks.push({ data, offset: collection.offset });
      collection.items = [];
      collection.bytes = [];
      collection.pending = false;
    });
    const deadline = Deferred.await(acquired).pipe(
      Effect.timeoutOption(Duration.millis(input.timeoutMs ?? 5000)),
      Effect.flatMap((initial) =>
        Option.isSome(initial)
          ? Effect.sleep(Duration.millis(input.timeoutMs ?? 5000))
          : Effect.void,
      ),
      Effect.tap(() =>
        Effect.sync(() => {
          collection.timedOut = true;
        }),
      ),
    );
    const stopAtLimit = Effect.suspend(() =>
      collection.chunks.length >= (input.maxChunks ?? 100) ||
      (input.waitForUpToDate === true && collection.upToDate)
        ? Deferred.succeed(stopped, undefined).pipe(Effect.andThen(Effect.never))
        : Effect.void,
    );
    yield* Effect.gen(function* () {
      const url = yield* state.location(input);
      const contentType = yield* state.contentType(input);
      const client = yield* DurableStreamsClient.make({
        url,
        live: input.live === "sse" ? "sse" : "long-poll",
        ...Record.filter(
          { offset: input.offset, headers: input.headers },
          Predicate.isNotUndefined,
        ),
      });
      collection.readOffset = client.offset;
      const metadata = contentType === undefined ? yield* client.connect : undefined;
      const discovered =
        metadata !== undefined && StreamMetadata.guards.Existing(metadata)
          ? metadata.contentType
          : contentType;
      if (discovered?.split(";")[0]?.trim().toLowerCase() === "application/json") {
        yield* client.json.pipe(
          Stream.runForEach((item) =>
            Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(item).pipe(
              Effect.tap((encoded) =>
                Effect.sync(() => {
                  collection.items.push(encoded);
                }),
              ),
            ),
          ),
        );
      } else {
        yield* client.bytes.pipe(
          Stream.runForEach((bytes) =>
            Effect.sync(() => {
              collection.bytes.push(bytes);
            }),
          ),
        );
      }
      yield* flush;
    }).pipe(
      Effect.provideServiceEffect(
        HttpClient.HttpClient,
        Effect.map(HttpClient.HttpClient, (http) =>
          http.pipe(
            HttpClient.mapRequestInputEffect((request) =>
              request.method !== "GET"
                ? Effect.succeed(request)
                : Effect.gen(function* () {
                    yield* flush;
                    yield* stopAtLimit;
                    collection.checkpointBefore = yield* collection.readOffset;
                    return request;
                  }),
            ),
            HttpClient.tap((response) => {
              if (
                response.request.method !== "GET" ||
                (response.status !== 200 && response.status !== 204)
              )
                return Effect.void;
              return Effect.sync(() => {
                if (
                  response.headers["content-type"]?.split(";")[0]?.trim().toLowerCase() ===
                  "text/event-stream"
                ) {
                  collection.pending = false;
                  const source = observeSse({
                    source: response.stream,
                    before: (control) =>
                      Effect.gen(function* () {
                        collection.checkpointBefore = yield* collection.readOffset;
                        collection.pending = true;
                        collection.headers = {
                          offset: control.streamNextOffset,
                          upToDate: control.streamClosed === true || control.upToDate === true,
                          streamClosed: control.streamClosed === true,
                        };
                      }),
                    after: Effect.gen(function* () {
                      const committed = yield* collection.readOffset;
                      if (
                        collection.pending &&
                        Option.isSome(committed) &&
                        committed.value === collection.headers.offset &&
                        committed !== collection.checkpointBefore
                      ) {
                        yield* flush;
                        yield* stopAtLimit;
                      }
                    }),
                  });
                  Object.defineProperty(response, "stream", { get: () => source });
                  return;
                }
                collection.pending = true;
                collection.headers = {
                  offset: response.headers["stream-next-offset"] ?? collection.offset,
                  upToDate: response.headers["stream-up-to-date"] === "true",
                  streamClosed: response.headers["stream-closed"] === "true",
                };
              }).pipe(Effect.andThen(Deferred.succeed(acquired, undefined)));
            }),
          ),
        ),
      ),
      Effect.raceFirst(Deferred.await(stopped)),
      Effect.raceFirst(deadline),
    );
    const committed = yield* collection.readOffset;
    if (
      collection.timedOut &&
      collection.pending &&
      Option.isSome(committed) &&
      committed.value === collection.headers.offset &&
      committed !== collection.checkpointBefore
    )
      yield* flush;
    return {
      type: "read",
      success: true,
      status:
        input.live === "long-poll" &&
        collection.timedOut &&
        !Arr.isArrayNonEmpty(collection.chunks) &&
        (yield* Deferred.isDone(acquired))
          ? 204
          : 200,
      chunks: collection.chunks,
      offset: collection.offset,
      upToDate: collection.timedOut || collection.upToDate,
      ...Record.filter(
        { streamClosed: (yield* Deferred.isDone(acquired)) ? collection.streamClosed : undefined },
        Predicate.isNotUndefined,
      ),
      ...(yield* state.sent),
    } satisfies TestResult;
  });
