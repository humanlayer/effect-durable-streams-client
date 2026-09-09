import { describe, expect, expectTypeOf, it } from "@effect/vitest";
import {
  Context,
  Deferred,
  Effect,
  Fiber,
  Layer,
  Logger,
  Option,
  Predicate,
  Queue,
  Schema,
  SchemaGetter,
  Stream,
} from "effect";
import { HttpClient, HttpClientError, HttpClientRequest } from "effect/unstable/http";
import { DurableStreamsClient, ErrorResponseBody, type ReadError } from "../src/index.ts";
import { makeReadHttp, readReply } from "./support/read-http.ts";

describe("catch-up codecs and errors", () => {
  it.effect(
    "classifies status-filtered clients and rejects uncached 304 without default retry",
    () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        for (const [status, tag] of [
          [404, "StreamNotFoundError"],
          [304, "ProtocolViolationError"],
        ] as const) {
          const client = yield* DurableStreamsClient.make({ url: "https://streams.test/data" });
          yield* Queue.offer(http.replies, { status, headers: {}, body: Stream.empty });
          expect(
            yield* client.bytes.pipe(
              Stream.runDrain,
              Effect.flip,
              Effect.provideServiceEffect(
                HttpClient.HttpClient,
                Effect.map(HttpClient.HttpClient, HttpClient.filterStatusOk),
              ),
              Effect.provide(http.layer),
            ),
          ).toHaveProperty("_tag", tag);
          expect((yield* Queue.take(http.requests)).signal.aborted).toBe(true);
          expect(yield* Queue.size(http.requests)).toBe(0);
        }
      }),
  );

  it.effect("keeps arbitrary binary bytes undecoded even with a JSON schema configured", () =>
    Effect.gen(function* () {
      const http = yield* makeReadHttp;
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/data",
        schema: Schema.String,
      });
      const wire = new Uint8Array([0, 255, 128]);
      yield* Queue.offer(http.replies, {
        ...readReply({ offset: "tail", text: "", contentType: "application/json", upToDate: true }),
        body: Stream.make(wire),
      });
      expect(yield* client.bytes.pipe(Stream.runCollect, Effect.provide(http.layer))).toEqual([
        wire,
      ]);
      expect(yield* client.offset).toEqual(Option.some("tail"));
    }),
  );
  it.effect("carries split UTF-8 between responses without committing an undecoded suffix", () =>
    Effect.gen(function* () {
      const http = yield* makeReadHttp;
      const client = yield* DurableStreamsClient.make({ url: "https://streams.test/data" });
      const run = yield* client.text.pipe(
        Stream.runCollect,
        Effect.provide(http.layer),
        Effect.forkChild,
      );
      yield* Queue.take(http.requests);
      yield* Queue.offer(http.replies, {
        ...readReply({ offset: "split", text: "" }),
        body: Stream.make(new Uint8Array([65, 0xf0, 0x9f])),
      });
      const second = yield* Queue.take(http.requests);
      expect(new URL(second.request.url).searchParams.get("offset")).toBe("split");
      expect(yield* client.offset).toEqual(Option.none());
      yield* Queue.offer(http.replies, {
        ...readReply({ offset: "tail", text: "", upToDate: true }),
        body: Stream.make(new Uint8Array([0x8e, 0x89, 66])),
      });
      expect((yield* Fiber.join(run)).join("")).toBe("A🎉B");
      expect(yield* client.offset).toEqual(Option.some("tail"));
    }),
  );

  it.effect(
    "strict text flush rejects truncated terminal characters and preserves the prior checkpoint",
    () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        for (const invalid of [new Uint8Array([0xff]), new Uint8Array([0xe2, 0x82])]) {
          const client = yield* DurableStreamsClient.make({ url: "https://streams.test/data" });
          yield* Queue.offer(http.replies, readReply({ offset: "previous", text: "valid" }));
          yield* Queue.offer(http.replies, {
            ...readReply({ offset: "broken", text: "", upToDate: true }),
            body: Stream.make(invalid),
          });
          expect(
            yield* client.text.pipe(Stream.runDrain, Effect.flip, Effect.provide(http.layer)),
          ).toHaveProperty("_tag", "PayloadDecodeError");
          expect(yield* client.offset).toEqual(Option.some("previous"));
        }
      }),
  );
  it.effect("preserves bytes and strictly decodes UTF-8 across arbitrary body chunks", () =>
    Effect.gen(function* () {
      const http = yield* makeReadHttp;
      const wire = new TextEncoder().encode("日本語 🎉 café");
      for (const mode of ["bytes", "text"] as const) {
        const client = yield* DurableStreamsClient.make({ url: "https://streams.test/data" });
        yield* Queue.offer(http.replies, {
          ...readReply({ offset: "tail", text: "", upToDate: true }),
          body: Stream.fromIterable(Array.from(wire, (byte) => new Uint8Array([byte]))),
        });
        if (mode === "bytes") {
          const chunks = yield* client.bytes.pipe(Stream.runCollect, Effect.provide(http.layer));
          expect(chunks.flatMap((chunk) => Array.from(chunk))).toEqual(Array.from(wire));
        } else {
          expect(
            (yield* client.text.pipe(Stream.runCollect, Effect.provide(http.layer))).join(""),
          ).toBe("日本語 🎉 café");
        }
        expect(yield* client.offset).toEqual(Option.some("tail"));
      }
    }),
  );

  it.effect("emits one level of JSON messages including null, scalars and nested arrays", () =>
    Effect.gen(function* () {
      const http = yield* makeReadHttp;
      const client = yield* DurableStreamsClient.make({ url: "https://streams.test/data" });
      yield* Queue.offer(
        http.replies,
        readReply({ offset: "empty", text: "[]", contentType: "application/json" }),
      );
      yield* Queue.offer(
        http.replies,
        readReply({
          offset: "tail",
          text: '[null,false,0,"",[1,[2]],{"x":1}]',
          contentType: "Application/JSON; charset=utf-8",
          upToDate: true,
        }),
      );
      expect(yield* client.json.pipe(Stream.runCollect, Effect.provide(http.layer))).toEqual([
        null,
        false,
        0,
        "",
        [1, [2]],
        { x: 1 },
      ]);
    }),
  );

  it.effect(
    "retains schema decoding services and does not commit while item decoding is blocked",
    () =>
      Effect.gen(function* () {
        class Decode extends Context.Service<Decode, { readonly prefix: string }>()(
          "read/Decode",
        ) {}
        class Encode extends Context.Service<Encode, { readonly suffix: string }>()(
          "read/Encode",
        ) {}
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const schema = Schema.String.pipe(
          Schema.decodeTo(Schema.Struct({ id: Schema.String }), {
            decode: SchemaGetter.transformOrFail((value) =>
              Effect.gen(function* () {
                const service = yield* Decode;
                yield* Deferred.succeed(entered, undefined);
                yield* Deferred.await(release);
                return { id: service.prefix + value };
              }),
            ),
            encode: SchemaGetter.transformOrFail((value) =>
              Encode.pipe(Effect.map((service) => value.id + service.suffix)),
            ),
          }),
        );
        const http = yield* makeReadHttp;
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/data",
          schema,
        });
        expectTypeOf<Stream.Services<typeof client.json>>().toEqualTypeOf<
          HttpClient.HttpClient | Decode
        >();
        expectTypeOf<Stream.Error<typeof client.json>>().toEqualTypeOf<ReadError>();
        expectTypeOf<Stream.Success<typeof client.json>>().toEqualTypeOf<{ readonly id: string }>();
        yield* Queue.offer(
          http.replies,
          readReply({
            offset: "tail",
            text: '["1","2"]',
            contentType: "application/json",
            upToDate: true,
          }),
        );
        const run = yield* client.json.pipe(
          Stream.runCollect,
          Effect.provide(Layer.merge(http.layer, Layer.succeed(Decode, { prefix: "id-" }))),
          Effect.forkChild,
        );
        yield* Deferred.await(entered);
        expect(yield* client.offset).toEqual(Option.none());
        yield* Deferred.succeed(release, undefined);
        expect(yield* Fiber.join(run)).toEqual([{ id: "id-1" }, { id: "id-2" }]);
        expect(yield* client.offset).toEqual(Option.some("tail"));
      }),
  );

  it.effect(
    "rejects invalid and truncated UTF-8, malformed envelopes and schema mismatch without payload logs",
    () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        const logs: Array<string> = [];
        for (const body of [
          new Uint8Array([255]),
          new Uint8Array([0xf0, 0x9f]),
          new TextEncoder().encode('{"secret":"private-payload"}'),
          new TextEncoder().encode('["private-payload"]'),
          new TextEncoder().encode("["),
          new Uint8Array(0),
        ]) {
          const client = yield* DurableStreamsClient.make({
            url: "https://streams.test/data",
            schema: Schema.Struct({ id: Schema.Int }),
          });
          yield* Queue.offer(http.replies, {
            ...readReply({
              offset: "tail",
              text: "",
              contentType: "application/json",
              upToDate: true,
            }),
            body: Stream.make(body),
          });
          expect(
            yield* client.json.pipe(
              Stream.runCollect,
              Effect.flip,
              Effect.provide(
                Layer.merge(
                  http.layer,
                  Logger.layer([
                    Logger.make(({ message }) => {
                      logs.push(String(message));
                    }),
                  ]),
                ),
              ),
            ),
          ).toHaveProperty("_tag", "PayloadDecodeError");
          expect(yield* client.offset).toEqual(Option.none());
          expect((yield* Queue.take(http.requests)).signal.aborted).toBe(true);
        }
        expect(logs.length).toBeGreaterThan(0);
        expect(logs.join("\n")).not.toContain("private-payload");
      }),
  );

  it.effect("keeps ndjson in text/bytes and rejects JSON views of other content types", () =>
    Effect.gen(function* () {
      const http = yield* makeReadHttp;
      for (const json of [false, true]) {
        const client = yield* DurableStreamsClient.make({ url: "https://streams.test/data" });
        yield* Queue.offer(
          http.replies,
          readReply({
            offset: "tail",
            text: '{"id":1}\n',
            contentType: "application/ndjson",
            upToDate: true,
          }),
        );
        if (json)
          expect(
            yield* client.json.pipe(Stream.runDrain, Effect.flip, Effect.provide(http.layer)),
          ).toHaveProperty("component", "read content-type");
        else
          expect(yield* client.text.pipe(Stream.runCollect, Effect.provide(http.layer))).toEqual([
            '{"id":1}\n',
          ]);
      }
    }),
  );

  it.effect(
    "classifies HTTP failures separately from missing HEAD and freezes diagnostic snapshots",
    () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        for (const [status, tag] of [
          [400, "InvalidRequestError"],
          [401, "UnauthorizedError"],
          [403, "ForbiddenError"],
          [404, "StreamNotFoundError"],
          [410, "StreamGoneError"],
          [429, "RateLimitedError"],
          [503, "StreamUnavailableError"],
          [501, "StreamUnavailableError"],
          [304, "ProtocolViolationError"],
          [204, "ProtocolViolationError"],
          [409, "ProtocolViolationError"],
        ] as const) {
          const client = yield* DurableStreamsClient.make({
            url: "https://streams.test/data?token=secret",
            backoffOptions: { maxRetries: 0 },
          });
          yield* Queue.offer(http.replies, {
            status,
            headers: { "set-cookie": "private", "retry-after": "3" },
            body: Stream.make(new TextEncoder().encode("diagnostic")),
          });
          const error = yield* client.bytes.pipe(
            Stream.runDrain,
            Effect.flip,
            Effect.provide(http.layer),
          );
          expect(error).toHaveProperty("_tag", tag);
          if (Predicate.hasProperty(error, "response") && error.response !== undefined) {
            expect(Object.isFrozen(error.response)).toBe(true);
            expect(error.response.url).toBe("https://streams.test/data");
            expect(error.response.headers["set-cookie"]).toBe("[REDACTED]");
            if (ErrorResponseBody.guards.Bytes(error.response.body)) {
              error.response.body.value.fill(0);
              expect(new TextDecoder().decode(error.response.body.value)).toBe("diagnostic");
            }
          }
          expect(yield* client.offset).toEqual(Option.none());
          expect((yield* Queue.take(http.requests)).signal.aborted).toBe(true);
        }
        const head = yield* DurableStreamsClient.make({ url: "https://streams.test/data" });
        yield* Queue.offer(http.replies, { status: 404, headers: {}, body: Stream.empty });
        expect(yield* head.head.pipe(Effect.provide(http.layer))).toHaveProperty("_tag", "Missing");
      }),
  );

  it.effect("rejects missing/sentinel offsets and malformed read metadata without committing", () =>
    Effect.gen(function* () {
      const http = yield* makeReadHttp;
      const cases: ReadonlyArray<Readonly<Record<string, string>>> = [
        { "content-type": "text/plain" },
        { "stream-next-offset": "now", "content-type": "text/plain" },
        { "stream-next-offset": "-1", "content-type": "text/plain" },
        { "stream-next-offset": "tail", "content-type": "text/plain", "stream-closed": "yes" },
        {
          "stream-next-offset": "tail",
          "content-type": "text/plain",
          "stream-up-to-date": "false",
        },
      ];
      for (const headers of cases) {
        const client = yield* DurableStreamsClient.make({ url: "https://streams.test/data" });
        yield* Queue.offer(http.replies, {
          status: 200,
          headers,
          body: Stream.empty,
        });
        expect(
          yield* client.bytes.pipe(Stream.runDrain, Effect.flip, Effect.provide(http.layer)),
        ).toHaveProperty("_tag", "ProtocolViolationError");
        expect(yield* client.offset).toEqual(Option.none());
      }
    }),
  );

  it.effect("does not replay a failed body after partial delivery", () =>
    Effect.gen(function* () {
      const http = yield* makeReadHttp;
      const client = yield* DurableStreamsClient.make({ url: "https://streams.test/data" });
      const released = yield* Deferred.make<void>();
      const body = Stream.make(new Uint8Array([1])).pipe(
        Stream.concat(
          Stream.fail(
            new HttpClientError.HttpClientError({
              reason: new HttpClientError.TransportError({
                request: HttpClientRequest.get("https://streams.test/data"),
                description: "broken body",
              }),
            }),
          ),
        ),
        Stream.ensuring(Deferred.succeed(released, undefined)),
      );
      yield* Queue.offer(http.replies, {
        ...readReply({ offset: "tail", text: "", upToDate: true }),
        body,
      });
      const values: Array<number> = [];
      expect(
        yield* client.bytes.pipe(
          Stream.runForEach((chunk) =>
            Effect.sync(() => {
              values.push(...chunk);
            }),
          ),
          Effect.flip,
          Effect.provide(http.layer),
        ),
      ).toHaveProperty("_tag", "StreamUnavailableError");
      expect(values).toEqual([1]);
      expect(yield* Deferred.isDone(released)).toBe(true);
      expect(yield* client.offset).toEqual(Option.none());
      expect((yield* Queue.take(http.requests)).signal.aborted).toBe(true);
      expect(yield* Queue.size(http.requests)).toBe(0);
    }),
  );
});
