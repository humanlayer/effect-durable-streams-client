import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Option, Schema, SchemaGetter, Stream } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import { DurableStreamsClient } from "../../src/index.ts";
import { acquireDurableStreamServer } from "../support/server.ts";
import { AdapterState } from "../conformance/adapter-state.ts";
import { handleCommand } from "../conformance/adapter.ts";

describe("reference server finite catch-up", () => {
  it.effect(
    "reads bytes/text, skips history with now and resumes from exact returned offsets",
    () =>
      Effect.gen(function* () {
        const { baseUrl } = yield* acquireDurableStreamServer;
        const writer = yield* DurableStreamsClient.make({
          url: baseUrl + "/bytes",
          contentType: "application/octet-stream",
        });
        yield* writer.create({ value: new Uint8Array([0, 255, 1]) });
        const first = yield* DurableStreamsClient.make({ url: baseUrl + "/bytes" });
        expect(
          (yield* first.bytes.pipe(Stream.runCollect)).flatMap((chunk) => Array.from(chunk)),
        ).toEqual([0, 255, 1]);
        const offset = Option.getOrThrow(yield* first.offset);
        const tail = yield* DurableStreamsClient.make({ url: baseUrl + "/bytes", offset: "now" });
        expect(yield* tail.bytes.pipe(Stream.runCollect)).toEqual([]);
        expect(yield* tail.offset).toEqual(Option.some(offset));
        yield* writer.close({ value: new Uint8Array([2, 128]) });
        const resumed = yield* DurableStreamsClient.make({ url: baseUrl + "/bytes", offset });
        expect(
          (yield* resumed.bytes.pipe(Stream.runCollect)).flatMap((chunk) => Array.from(chunk)),
        ).toEqual([2, 128]);
        const text = yield* DurableStreamsClient.make({
          url: baseUrl + "/text",
          contentType: "text/plain; charset=utf-8",
        });
        yield* text.create({ value: "こんにちは 🎉", closed: true });
        expect((yield* text.text.pipe(Stream.runCollect)).join("")).toBe("こんにちは 🎉");
        expect(yield* text.text.pipe(Stream.runDrain, Effect.flip)).toHaveProperty(
          "_tag",
          "AlreadyConsumedError",
        );
        const closedNow = yield* DurableStreamsClient.make({
          url: baseUrl + "/text",
          offset: "now",
        });
        expect(yield* closedNow.text.pipe(Stream.runCollect)).toEqual([]);
      }).pipe(Effect.provide(FetchHttpClient.layer)),
  );

  it.effect(
    "round trips raw and transformed schema JSON, including empty now and saved resumption",
    () =>
      Effect.gen(function* () {
        const { baseUrl } = yield* acquireDurableStreamServer;
        const raw = yield* DurableStreamsClient.make({
          url: baseUrl + "/json",
          contentType: "application/json",
        });
        yield* raw.create({ values: [1, [2, 3], null, { id: "four" }] });
        expect(yield* raw.json.pipe(Stream.runCollect)).toEqual([1, [2, 3], null, { id: "four" }]);
        const offset = Option.getOrThrow(yield* raw.offset);
        yield* raw.append({ value: false });
        const resumed = yield* DurableStreamsClient.make({ url: baseUrl + "/json", offset });
        expect(yield* resumed.json.pipe(Stream.runCollect)).toEqual([false]);
        const now = yield* DurableStreamsClient.make({ url: baseUrl + "/json", offset: "now" });
        expect(yield* now.json.pipe(Stream.runCollect)).toEqual([]);
        const schema = Schema.String.pipe(
          Schema.decodeTo(Schema.Struct({ id: Schema.String }), {
            decode: SchemaGetter.transform((id) => ({ id })),
            encode: SchemaGetter.transform((value) => value.id),
          }),
        );
        const typed = yield* DurableStreamsClient.make({ url: baseUrl + "/typed", schema });
        yield* typed.create({ values: [{ id: "a" }, { id: "b" }], closed: true });
        expect(yield* typed.json.pipe(Stream.runCollect)).toEqual([{ id: "a" }, { id: "b" }]);
        const invalid = yield* DurableStreamsClient.make({
          url: baseUrl + "/typed",
          schema: Schema.Struct({ wrong: Schema.Int }),
        });
        expect(yield* invalid.json.pipe(Stream.runDrain, Effect.flip)).toHaveProperty(
          "_tag",
          "PayloadDecodeError",
        );
        expect(yield* invalid.offset).toEqual(Option.none());
        const missing = yield* DurableStreamsClient.make({ url: baseUrl + "/missing" });
        expect(yield* missing.head).toHaveProperty("_tag", "Missing");
        expect(yield* missing.json.pipe(Stream.runDrain, Effect.flip)).toHaveProperty(
          "_tag",
          "StreamNotFoundError",
        );
      }).pipe(Effect.provide(FetchHttpClient.layer)),
  );

  it.effect("runs finite adapter commands over the real SDK without advertising live reads", () =>
    Effect.gen(function* () {
      const { baseUrl } = yield* acquireDurableStreamServer;
      yield* handleCommand({ type: "init", serverUrl: baseUrl });
      yield* handleCommand({
        type: "create",
        path: "/adapter",
        contentType: "application/json",
        data: "[1,[2,3]]",
      });
      const read = yield* handleCommand({
        type: "read",
        path: "/adapter",
        live: false,
        maxChunks: 1,
      });
      expect(read).toMatchObject({
        type: "read",
        success: true,
        upToDate: true,
        chunks: [{ data: "[1,[2,3]]" }],
      });
      expect(
        yield* handleCommand({ type: "read", path: "/adapter", offset: "now", live: false }),
      ).toMatchObject({ type: "read", chunks: [] });
      expect(
        yield* handleCommand({ type: "read", path: "/adapter", live: "long-poll" }),
      ).toMatchObject({ type: "error", errorCode: "NOT_SUPPORTED" });
      expect(yield* handleCommand({ type: "read", path: "/missing", live: false })).toMatchObject({
        type: "error",
        errorCode: "NOT_FOUND",
      });
    }).pipe(
      Effect.provide(
        Layer.merge(FetchHttpClient.layer, Layer.effect(AdapterState, AdapterState.make)),
      ),
    ),
  );
});
