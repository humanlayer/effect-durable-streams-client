import { describe, expect, expectTypeOf, it } from "@effect/vitest";
import { Array, Effect, Match, Queue, Schema, SchemaGetter } from "effect";
import { DurableStreamsClient, type CreateInput } from "../src/index.ts";
import { makeScriptedHttpClient, ScriptedResponse } from "./support/http-client.ts";

describe("atomic initial values", () => {
  it.effect("frames raw JSON once and distinguishes empty messages from no messages", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/initial",
        contentType: "application/json",
      });
      expectTypeOf<{ value: number; values: number[] }>().not.toExtend<CreateInput<number>>();
      expectTypeOf<{ values: string[] }>().not.toExtend<CreateInput<number>>();
      const cases: ReadonlyArray<readonly [CreateInput<Schema.Json>, string | undefined]> = [
        [{}, undefined],
        [{ value: [1, 2] }, "[[1,2]]"],
        [{ values: [1, 2] }, "[1,2]"],
        [{ value: [] }, "[[]]"],
        [{ values: [] }, "[]"],
        [{ values: [[1, 2], [], [3]] }, "[[1,2],[],[3]]"],
      ];
      for (const [input, expected] of cases) {
        yield* http.respond(
          ScriptedResponse.Response({
            status: 201,
            headers: {
              "content-type": "application/json",
              "stream-next-offset": "tail",
              "stream-closed": "true",
            },
          }),
        );
        yield* client.create({ ...input, closed: true }).pipe(Effect.provide(http.layer));
        const request = yield* Queue.take(http.requests);
        expect(request.method).toBe("PUT");
        expect(request.headers["stream-closed"]).toBe("true");
        expect(
          Match.value(request.body).pipe(
            Match.tag("Uint8Array", (body) => new TextDecoder().decode(body.body)),
            Match.orElse(() => undefined),
          ),
        ).toBe(expected);
        expect(yield* Queue.size(http.requests)).toBe(0);
      }
    }),
  );

  it.effect("concatenates text and binary inputs without delimiters, including empty inputs", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      for (const contentType of ["text/plain", "application/octet-stream"]) {
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/initial",
          contentType,
        });
        for (const values of [[], ["hé", new Uint8Array([0, 255]), "!"]]) {
          yield* http.respond(
            ScriptedResponse.Response({
              status: 201,
              headers: { "content-type": contentType, "stream-next-offset": "tail" },
            }),
          );
          yield* client.create({ values }).pipe(Effect.provide(http.layer));
          const request = yield* Queue.take(http.requests);
          expect(request.method).toBe("PUT");
          expect(
            Match.value(request.body).pipe(
              Match.tag("Uint8Array", (body) => [...body.body]),
              Match.orElse(() => undefined),
            ),
          ).toEqual(Array.isReadonlyArrayNonEmpty(values) ? [104, 195, 169, 0, 255, 33] : []);
          expect(yield* Queue.size(http.requests)).toBe(0);
        }
      }
    }),
  );

  it.effect("rejects conflicting property presence and invalid lists before dispatch", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/initial",
        contentType: "application/json",
      });
      for (const invalid of [
        { value: 1, values: [2] },
        { value: undefined, values: [2] },
        { value: 1, values: undefined },
        { value: undefined, values: undefined },
        { values: undefined },
        { values: null },
        { values: "not-an-array" },
      ]) {
        const erased: {} = invalid;
        expect(
          yield* client.create(erased).pipe(Effect.flip, Effect.provide(http.layer)),
        ).toHaveProperty("_tag", "PayloadEncodeError");
      }
      expect(yield* Queue.size(http.requests)).toBe(0);
    }),
  );

  it.effect("encodes undefined-valued schema items and preserves transformed nested arrays", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const schema = Schema.Json.pipe(
        Schema.decodeTo(Schema.Undefined, {
          decode: SchemaGetter.transform(() => undefined),
          encode: SchemaGetter.transform(() => [null, []]),
        }),
      );
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/initial",
        schema,
      });
      expectTypeOf<{ values: number[] }>().not.toExtend<Parameters<typeof client.create>[0]>();
      for (const values of [[], [undefined, undefined]]) {
        yield* http.respond(
          ScriptedResponse.Response({
            status: 201,
            headers: { "content-type": "application/json", "stream-next-offset": "tail" },
          }),
        );
        yield* client.create({ values }).pipe(Effect.provide(http.layer));
        const request = yield* Queue.take(http.requests);
        expect(
          Match.value(request.body).pipe(
            Match.tag("Uint8Array", (body) => new TextDecoder().decode(body.body)),
            Match.orElse(() => "missing"),
          ),
        ).toBe(Array.isReadonlyArrayNonEmpty(values) ? "[[null,[]],[null,[]]]" : "[]");
        expect(yield* Queue.size(http.requests)).toBe(0);
      }
    }),
  );

  it.effect("fails the entire creation when any schema or raw JSON item cannot encode", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const typed = yield* DurableStreamsClient.make({
        url: "https://streams.test/initial",
        schema: Schema.String.check(Schema.isMinLength(3)),
      });
      const raw = yield* DurableStreamsClient.make({
        url: "https://streams.test/initial",
        contentType: "application/json",
      });
      for (const operation of [
        typed.create({ values: ["valid", "x"], closed: true }),
        raw.create({ values: [1, NaN], closed: true }),
      ]) {
        expect(yield* operation.pipe(Effect.flip, Effect.provide(http.layer))).toHaveProperty(
          "_tag",
          "PayloadEncodeError",
        );
      }
      expect(yield* Queue.size(http.requests)).toBe(0);
    }),
  );
});
