import { describe, expect, it } from "@effect/vitest";
import { Data, Deferred, Effect, Exit, Fiber, Layer, Ref, Schema, Stream } from "effect";
import { FetchHttpClient, HttpClient } from "effect/unstable/http";
import { DurableStreamsClient } from "../../src/index.ts";
import { AdapterState } from "../conformance/adapter-state.ts";
import { processLine } from "../conformance/adapter.ts";
import { acquireDurableStreamServer } from "../support/server.ts";

class UploadFailed extends Data.TaggedError("UploadFailed") {}

describe("reference server batched and streaming writes", () => {
  it.effect(
    "round trips concurrent JSON messages and raw JSON streaming without extra flattening",
    () =>
      Effect.gen(function* () {
        const { baseUrl } = yield* acquireDurableStreamServer;
        const http = yield* HttpClient.HttpClient;
        const client = yield* DurableStreamsClient.make({
          url: baseUrl + "/json",
          contentType: "application/json",
        });
        yield* client.create({});
        const receipts = yield* Effect.forEach(
          [1, [2, 3], { four: 4 }],
          (value) => client.append({ value }),
          { concurrency: "unbounded" },
        );
        expect(receipts).toHaveLength(3);
        const result = yield* client.appendStream({
          source: Stream.make("[5,", new TextEncoder().encode("[6,7]]")),
        });
        expect(yield* (yield* http.get(baseUrl + "/json")).json).toEqual([
          1,
          [2, 3],
          { four: 4 },
          5,
          [6, 7],
        ]);
        expect(yield* client.head).toMatchObject({ offset: result.offset, closed: false });
      }).pipe(Effect.provide(FetchHttpClient.layer)),
  );

  it.effect("concatenates text and binary ordinary appends and streamed wire chunks exactly", () =>
    Effect.gen(function* () {
      const { baseUrl } = yield* acquireDurableStreamServer;
      const http = yield* HttpClient.HttpClient;
      for (const contentType of ["text/plain", "application/octet-stream"]) {
        const url = baseUrl + (contentType === "text/plain" ? "/text" : "/bytes");
        const client = yield* DurableStreamsClient.make({ url, contentType });
        yield* client.create({});
        yield* Effect.forEach(
          ["hé", new Uint8Array([0, 255]), "!"],
          (value) => client.append({ value }),
          { concurrency: "unbounded" },
        );
        const result = yield* client.appendStream({
          source: Stream.make("more", new Uint8Array([13, 10])),
        });
        expect(new Uint8Array(yield* (yield* http.get(url)).arrayBuffer)).toEqual(
          new Uint8Array([104, 195, 169, 0, 255, 33, 109, 111, 114, 101, 13, 10]),
        );
        expect(yield* client.head).toMatchObject({ offset: result.offset });
      }
    }).pipe(Effect.provide(FetchHttpClient.layer)),
  );

  it.effect(
    "Fetch streaming source failure retains E and cancellation finalizes a blocked pull",
    () =>
      Effect.gen(function* () {
        const { baseUrl } = yield* acquireDurableStreamServer;
        const client = yield* DurableStreamsClient.make({
          url: baseUrl + "/cancel",
          contentType: "application/octet-stream",
        });
        yield* client.create({});
        const failed = new UploadFailed();
        expect(
          yield* client
            .appendStream({ source: Stream.make("first").pipe(Stream.concat(Stream.fail(failed))) })
            .pipe(Effect.flip),
        ).toBe(failed);
        const pulling = yield* Deferred.make<void>();
        const released = yield* Ref.make(false);
        const source = Stream.make("partial").pipe(
          Stream.concat(
            Stream.fromEffect(
              Deferred.succeed(pulling, undefined).pipe(Effect.andThen(Effect.never)),
            ),
          ),
          Stream.ensuring(Ref.set(released, true)),
        );
        const upload = yield* client.appendStream({ source }).pipe(Effect.forkChild);
        yield* Deferred.await(pulling);
        yield* Fiber.interrupt(upload);
        expect(Exit.hasInterrupts(yield* Fiber.await(upload))).toBe(true);
        expect(yield* Ref.get(released)).toBe(true);
        yield* client.append({ value: "after" });
        expect(yield* client.head).toMatchObject({ closed: false });
      }).pipe(Effect.provide(FetchHttpClient.layer)),
  );

  it.effect(
    "local append-batch extension uses SDK writes and does not alias producer commands",
    () =>
      Effect.gen(function* () {
        const { baseUrl } = yield* acquireDurableStreamServer;
        const encode = Schema.encodeEffect(Schema.fromJsonString(Schema.Json));
        const decode = Schema.decodeEffect(Schema.fromJsonString(Schema.Json));
        expect(
          yield* decode(yield* processLine(yield* encode({ type: "init", serverUrl: baseUrl }))),
        ).toMatchObject({ features: { batching: true, streaming: true } });
        yield* processLine('{"type":"create","path":"/local","contentType":"application/json"}');
        expect(
          yield* decode(
            yield* processLine(
              '{"type":"append-batch","path":"/local","items":["1","[2,3]","null"]}',
            ),
          ),
        ).toMatchObject({ type: "append-batch", success: true, status: 200 });
        const http = yield* HttpClient.HttpClient;
        expect(yield* (yield* http.get(baseUrl + "/local")).json).toEqual([1, [2, 3], null]);
        expect(
          yield* decode(
            yield* processLine(
              '{"type":"idempotent-append-batch","path":"/local","items":["4"],"producerId":"p","epoch":0,"autoClaim":false}',
            ),
          ),
        ).toMatchObject({ type: "idempotent-append-batch", success: true });
        expect(yield* (yield* http.get(baseUrl + "/local")).json).toEqual([1, [2, 3], null, 4]);
      }).pipe(
        Effect.provide(
          Layer.merge(FetchHttpClient.layer, Layer.effect(AdapterState, AdapterState.make)),
        ),
      ),
  );
});
