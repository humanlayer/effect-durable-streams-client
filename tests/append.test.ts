import { describe, expect, expectTypeOf, it } from "@effect/vitest";
import { Context, Effect, Layer, Match, Queue, Schema, SchemaGetter, Stream } from "effect";
import type { HttpClient } from "effect/unstable/http";
import { DurableStreamsClient, type DurableStreamsClientConfig } from "../src/index";
import { makeScriptedHttpClient, ScriptedResponse } from "./support/http-client";

describe("unbatched encoding", () => {
  it.effect("preserves declared configs and HTTP-only contextual writes", () =>
    Effect.gen(function* () {
      const Event = Schema.Struct({ id: Schema.String });
      const config: DurableStreamsClientConfig<typeof Event> = {
        url: "https://streams.test/orders",
        schema: Event,
      };
      const custom = yield* DurableStreamsClient.make(config);
      expectTypeOf<Parameters<typeof custom.append>[0]["value"]>().toEqualTypeOf<
        typeof Event.Type
      >();
      const http = yield* makeScriptedHttpClient;
      yield* http.respond(
        ScriptedResponse.Response({ status: 204, headers: { "stream-next-offset": "tail" } }),
      );
      const program = Effect.gen(function* () {
        const client = yield* DurableStreamsClient;
        const append = client.append({ value: { id: "raw" } });
        expectTypeOf<Effect.Services<typeof append>>().toEqualTypeOf<HttpClient.HttpClient>();
        return yield* append;
      });
      const provided = program.pipe(
        Effect.provide(
          Layer.merge(
            http.layer,
            DurableStreamsClient.layer({
              url: "https://streams.test/orders",
              contentType: "application/json",
            }),
          ),
        ),
      );
      expectTypeOf<Effect.Services<typeof provided>>().toEqualTypeOf<never>();
      expect(yield* provided).toEqual({ offset: "tail", closed: false });
    }),
  );
  it.effect("encodes exact bytes, UTF-8, raw JSON and nested logical arrays", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const cases = [
        {
          contentType: "application/octet-stream",
          value: new Uint8Array([0, 255, 10]),
          bytes: new Uint8Array([0, 255, 10]),
        },
        { contentType: "text/plain", value: "héllo", bytes: new TextEncoder().encode("héllo") },
        {
          contentType: "application/json",
          value: [1, { id: 2 }],
          bytes: new TextEncoder().encode('[[1,{"id":2}]]'),
        },
        {
          contentType: "application/json; charset=utf-8",
          value: null,
          bytes: new TextEncoder().encode("[null]"),
        },
      ];
      for (const entry of cases) {
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/orders",
          contentType: entry.contentType,
          batching: false,
        });
        yield* http.respond(
          ScriptedResponse.Response({
            status: 204,
            headers: { "stream-next-offset": "opaque_ABC" },
          }),
        );
        expect(
          yield* client.append({ value: entry.value, seq: "009" }).pipe(Effect.provide(http.layer)),
        ).toEqual({ offset: "opaque_ABC", closed: false });
        const request = yield* Queue.take(http.requests);
        expect(request.method).toBe("POST");
        expect(request.headers).toMatchObject({
          "content-type": entry.contentType,
          "stream-seq": "009",
        });
        expect(
          Match.value(request.body).pipe(
            Match.tag("Uint8Array", (body) => body.body),
            Match.orElse(() => new Uint8Array()),
          ),
        ).toEqual(entry.bytes);
      }
    }),
  );

  it.effect("retains distinct schema types and encoding services in every write", () =>
    Effect.gen(function* () {
      class Encoder extends Context.Service<Encoder, { readonly suffix: string }>()(
        "write/Encoder",
      ) {}
      class Decoder extends Context.Service<Decoder, { readonly prefix: string }>()(
        "write/Decoder",
      ) {}
      const codec = Schema.String.pipe(
        Schema.decodeTo(Schema.Struct({ id: Schema.String }), {
          decode: SchemaGetter.transformOrFail((id) =>
            Decoder.pipe(Effect.map((s) => ({ id: s.prefix + id }))),
          ),
          encode: SchemaGetter.transformOrFail((value) =>
            Encoder.pipe(Effect.map((s) => value.id + s.suffix)),
          ),
        }),
      );
      const construction = DurableStreamsClient.make({
        url: "https://streams.test/orders",
        schema: codec,
      });
      expectTypeOf<Effect.Services<typeof construction>>().toEqualTypeOf<never>();
      const client = yield* construction;
      expectTypeOf<Parameters<typeof client.append>[0]["value"]>().toEqualTypeOf<{
        readonly id: string;
      }>();
      expectTypeOf<Stream.Services<typeof client.json>>().toEqualTypeOf<
        HttpClient.HttpClient | Decoder
      >();
      const append = client.append({ value: { id: "a" } });
      const create = client.create({ value: { id: "b" } });
      const createMany = client.create({ values: [{ id: "d" }, { id: "e" }] });
      const close = client.close({ value: { id: "c" } });
      expectTypeOf<Effect.Services<typeof append>>().toEqualTypeOf<
        HttpClient.HttpClient | Encoder
      >();
      expectTypeOf<Effect.Services<typeof create>>().toEqualTypeOf<
        HttpClient.HttpClient | Encoder
      >();
      expectTypeOf<Effect.Services<typeof createMany>>().toEqualTypeOf<
        HttpClient.HttpClient | Encoder
      >();
      expectTypeOf<Effect.Services<typeof close>>().toEqualTypeOf<
        HttpClient.HttpClient | Encoder
      >();
      const http = yield* makeScriptedHttpClient;
      const operations: ReadonlyArray<
        readonly [
          Effect.Effect<
            void,
            Effect.Error<typeof create | typeof append | typeof close>,
            HttpClient.HttpClient | Encoder
          >,
          number,
          string,
        ]
      > = [
        [create.pipe(Effect.as(undefined)), 201, '["b!"]'],
        [createMany.pipe(Effect.as(undefined)), 201, '["d!","e!"]'],
        [append.pipe(Effect.as(undefined)), 204, '["a!"]'],
        [close.pipe(Effect.as(undefined)), 204, '["c!"]'],
      ];
      for (const [operation, status, expected] of operations) {
        yield* http.respond(
          ScriptedResponse.Response({
            status,
            headers: {
              "content-type": "application/json",
              "stream-next-offset": "tail",
              "stream-closed": "true",
            },
          }),
        );
        const provided = operation.pipe(
          Effect.provide(Layer.merge(http.layer, Layer.succeed(Encoder, { suffix: "!" }))),
        );
        expectTypeOf<Effect.Services<typeof provided>>().toEqualTypeOf<never>();
        yield* provided;
        const request = yield* Queue.take(http.requests);
        expect(
          Match.value(request.body).pipe(
            Match.tag("Uint8Array", (body) => new TextDecoder().decode(body.body)),
            Match.orElse(() => "missing"),
          ),
        ).toBe(expected);
      }
      const raw = yield* DurableStreamsClient.make({
        url: "https://streams.test/orders",
        schema: Schema.Json,
      });
      expectTypeOf<Parameters<typeof raw.append>[0]["value"]>().toEqualTypeOf<Schema.Json>();
    }),
  );

  it.effect(
    "fails encoding before HTTP for non-JSON bytes, schema checks and invalid JSON numbers",
    () =>
      Effect.gen(function* () {
        const http = yield* makeScriptedHttpClient;
        const json = yield* DurableStreamsClient.make({
          url: "https://streams.test/orders",
          contentType: "application/json",
        });
        expect(
          yield* json.append({ value: NaN }).pipe(Effect.flip, Effect.provide(http.layer)),
        ).toHaveProperty("_tag", "PayloadEncodeError");
        const typed = yield* DurableStreamsClient.make({
          url: "https://streams.test/orders",
          schema: Schema.String.check(Schema.isMinLength(3)),
        });
        expect(
          yield* typed.append({ value: "x" }).pipe(Effect.flip, Effect.provide(http.layer)),
        ).toHaveProperty("_tag", "PayloadEncodeError");
        expect(
          yield* typed
            .create({ contentType: "text/plain" })
            .pipe(Effect.flip, Effect.provide(http.layer)),
        ).toHaveProperty("_tag", "PayloadEncodeError");
        expect(yield* Queue.size(http.requests)).toBe(0);
      }),
  );
});
