import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Logger, Match, Queue, Schema, SchemaGetter, Stream } from "effect";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";
import { DurableStreamsClient, ErrorResponseBody } from "../src/index.ts";
import { makeScriptedHttpClient, ScriptedResponse } from "./support/http-client.ts";

const UndefinedFromNull = Schema.Null.pipe(
  Schema.decodeTo(Schema.Undefined, {
    decode: SchemaGetter.transform(() => undefined),
    encode: SchemaGetter.transform(() => null),
  }),
);

describe("Phase 2 review regressions", () => {
  it.effect("encodes present undefined values for create, append and final append", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/orders",
        schema: UndefinedFromNull,
      });
      yield* http.respond(
        ScriptedResponse.Response({
          status: 201,
          headers: { "content-type": "application/json", "stream-next-offset": "start" },
        }),
      );
      yield* client.create({ value: undefined }).pipe(Effect.provide(http.layer));
      yield* http.respond(
        ScriptedResponse.Response({ status: 204, headers: { "stream-next-offset": "next" } }),
      );
      yield* client.append({ value: undefined }).pipe(Effect.provide(http.layer));
      yield* http.respond(
        ScriptedResponse.Response({
          status: 204,
          headers: { "stream-next-offset": "final", "stream-closed": "true" },
        }),
      );
      yield* client.close({ value: undefined }).pipe(Effect.provide(http.layer));
      for (const method of ["PUT", "POST", "POST"]) {
        const request = yield* Queue.take(http.requests);
        expect(request.method).toBe(method);
        expect(request.headers["content-type"]).toBe("application/json");
        expect(
          Match.value(request.body).pipe(
            Match.tag("Uint8Array", (body) => new TextDecoder().decode(body.body)),
            Match.orElse(() => "missing"),
          ),
        ).toBe("[null]");
      }
    }),
  );

  it.effect(
    "keeps absent create/close values bodyless but never retries a present undefined final value",
    () =>
      Effect.gen(function* () {
        const http = yield* makeScriptedHttpClient;
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/orders",
          schema: UndefinedFromNull,
        });
        yield* http.respond(
          ScriptedResponse.Response({
            status: 201,
            headers: { "content-type": "application/json", "stream-next-offset": "start" },
          }),
        );
        yield* client.create({}).pipe(Effect.provide(http.layer));
        expect((yield* Queue.take(http.requests)).body).toHaveProperty("_tag", "Empty");
        yield* http.respond(
          ScriptedResponse.Response({
            status: 204,
            headers: { "stream-next-offset": "start", "stream-closed": "true" },
          }),
        );
        yield* client.close({}).pipe(Effect.provide(http.layer));
        expect((yield* Queue.take(http.requests)).body).toHaveProperty("_tag", "Empty");
        yield* http.respond(ScriptedResponse.TransportFailure());
        expect(
          yield* client.close({ value: undefined }).pipe(Effect.flip, Effect.provide(http.layer)),
        ).toHaveProperty("_tag", "AppendOutcomeUnknownError");
        yield* Queue.take(http.requests);
        expect(yield* Queue.size(http.requests)).toBe(0);
      }),
  );

  it.effect("does not touch diagnostic streams or log warnings for ordinary mutation success", () =>
    Effect.gen(function* () {
      let reads = 0;
      let creates = 0;
      const logs: Array<string> = [];
      const signals: Array<AbortSignal> = [];
      const http = HttpClient.make((request, _url, signal) =>
        Effect.sync(() => {
          signals.push(signal);
          const status = request.method === "PUT" ? (++creates === 1 ? 201 : 200) : 204;
          const response = HttpClientResponse.fromWeb(
            request,
            new Response(null, {
              status,
              headers: {
                "content-type": "text/plain",
                "stream-next-offset": "tail",
                "stream-closed": "true",
              },
            }),
          );
          Object.defineProperty(response, "stream", {
            get: () => {
              reads++;
              return Stream.die("Successful lifecycle response body must not be read");
            },
          });
          return response;
        }),
      );
      yield* Effect.gen(function* () {
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/orders",
          contentType: "text/plain",
        });
        expect((yield* client.create({})).status).toBe(201);
        expect((yield* client.create({})).status).toBe(200);
        yield* client.append({ value: "data" });
        yield* client.close({});
        yield* client.delete;
      }).pipe(
        Effect.provide(
          Layer.merge(
            Layer.succeed(HttpClient.HttpClient, http),
            Logger.layer([
              Logger.make(({ message }) => {
                logs.push(String(message));
              }),
            ]),
          ),
        ),
      );
      expect(reads).toBe(0);
      expect(logs).toEqual([]);
      expect(signals).toHaveLength(5);
      expect(signals.every((signal) => signal.aborted)).toBe(true);
    }),
  );

  it.effect("captures null error bodies as Empty without body-read warnings", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const logs: Array<string> = [];
      yield* Effect.gen(function* () {
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/orders",
          contentType: "text/plain",
        });
        yield* http.respond(ScriptedResponse.Response({ status: 403, headers: {} }));
        yield* client.append({ value: "data" }).pipe(
          Effect.tap(() => Effect.sync(() => expect.fail("Expected ForbiddenError"))),
          Effect.catchTag("ForbiddenError", (error) =>
            Effect.sync(() => {
              expect(error.response.body).toEqual(ErrorResponseBody.cases.Empty.make({}));
            }),
          ),
        );
        yield* http.respond(ScriptedResponse.Response({ status: 403, headers: {} }));
        yield* client.head.pipe(
          Effect.tap(() => Effect.sync(() => expect.fail("Expected ForbiddenError"))),
          Effect.catchTag("ForbiddenError", (error) =>
            Effect.sync(() => {
              expect(error.response.body).toEqual(ErrorResponseBody.cases.Empty.make({}));
            }),
          ),
        );
        yield* http.respond(ScriptedResponse.Response({ status: 204, headers: {} }));
        yield* client.close({}).pipe(
          Effect.tap(() => Effect.sync(() => expect.fail("Expected ProtocolViolationError"))),
          Effect.catchTag("ProtocolViolationError", (error) =>
            Effect.sync(() => {
              expect(error.response?.body).toEqual(ErrorResponseBody.cases.Empty.make({}));
            }),
          ),
        );
      }).pipe(
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
      );
      expect(logs.some((line) => line.includes("Unable to finish error response snapshot"))).toBe(
        false,
      );
    }),
  );

  it.effect("still captures bounded diagnostic bodies for malformed successful responses", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/orders?secret=private",
      });
      yield* http.respond(
        ScriptedResponse.Response({
          status: 201,
          headers: { "set-cookie": "secret" },
          body: "diagnostic".repeat(10000),
        }),
      );
      yield* client.create({}).pipe(
        Effect.tap(() => Effect.sync(() => expect.fail("Expected ProtocolViolationError"))),
        Effect.catchTag("ProtocolViolationError", (error) =>
          Effect.sync(() => {
            expect(error.response?.url).toBe("https://streams.test/orders");
            expect(error.response?.headers["set-cookie"]).toBe("[REDACTED]");
            expect(Object.isFrozen(error.response)).toBe(true);
            expect(error.response).toBeDefined();
            if (
              error.response !== undefined &&
              ErrorResponseBody.guards.Bytes(error.response.body)
            ) {
              expect(error.response.body.value.length).toBe(65536);
              expect(error.response.body.truncated).toBe(true);
              expect(new TextDecoder().decode(error.response.body.value)).toMatch(/^diagnostic/);
            } else {
              expect.fail("Expected diagnostic bytes");
            }
          }),
        ),
        Effect.provide(http.layer),
      );
    }),
  );

  it.effect("keeps malformed success alive for diagnostics then releases the response", () =>
    Effect.gen(function* () {
      const signals: Array<AbortSignal> = [];
      const http = HttpClient.make((request, _url, signal) =>
        Effect.sync(() => {
          signals.push(signal);
          const response = HttpClientResponse.fromWeb(request, new Response(null, { status: 201 }));
          Object.defineProperty(response, "stream", {
            get: () =>
              Stream.fromEffect(
                Effect.sync(() => {
                  expect(signal.aborted).toBe(false);
                  return new TextEncoder().encode("still readable");
                }),
              ),
          });
          return response;
        }),
      );
      const client = yield* DurableStreamsClient.make({ url: "https://streams.test/orders" });
      yield* client.create({}).pipe(
        Effect.tap(() => Effect.sync(() => expect.fail("Expected ProtocolViolationError"))),
        Effect.catchTag("ProtocolViolationError", (error) =>
          Effect.sync(() => {
            expect(error.response?.body).toEqual(
              ErrorResponseBody.cases.Bytes.make({
                value: new TextEncoder().encode("still readable"),
                truncated: false,
              }),
            );
          }),
        ),
        Effect.provideService(HttpClient.HttpClient, http),
      );
      expect(signals).toHaveLength(1);
      expect(signals.every((signal) => signal.aborted)).toBe(true);
    }),
  );

  it.effect("rejects unsupported field values and sequences before HTTP or retry", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/orders",
        contentType: "text/plain",
      });
      for (const invalid of [
        "nul\0",
        "control\x01",
        "delete\x7f",
        "line\r\n",
        "wide\u0100",
        "emoji😀",
        "surrogate\ud800",
      ]) {
        expect(
          yield* DurableStreamsClient.make({
            url: "https://streams.test/orders",
            headers: { "X-Extension": invalid },
          }).pipe(Effect.flip),
        ).toHaveProperty("_tag", "InvalidDurableStreamsConfigError");
        expect(
          yield* DurableStreamsClient.make({
            url: "https://streams.test/orders",
            contentType: `text/plain; extension=${invalid}`,
          }).pipe(Effect.flip),
        ).toHaveProperty("_tag", "InvalidDurableStreamsConfigError");
        expect(
          yield* client
            .create({ contentType: `text/plain; extension=${invalid}` })
            .pipe(Effect.flip, Effect.provide(http.layer)),
        ).toHaveProperty("_tag", "PayloadEncodeError");
        expect(
          yield* client
            .append({ value: "data", seq: invalid })
            .pipe(Effect.flip, Effect.provide(http.layer)),
        ).toHaveProperty("_tag", "PayloadEncodeError");
        expect(
          yield* client.close({ seq: invalid }).pipe(Effect.flip, Effect.provide(http.layer)),
        ).toHaveProperty("_tag", "PayloadEncodeError");
      }
      expect(yield* Queue.size(http.requests)).toBe(0);
    }),
  );

  it.effect("accepts supported visible ASCII, tabs and byte-range obs-text", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const value = "ascii\tspace é\xff";
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/orders",
        headers: { "X-Extension": value },
        contentType: "text/plain",
      });
      yield* http.respond(
        ScriptedResponse.Response({ status: 204, headers: { "stream-next-offset": "tail" } }),
      );
      yield* client.append({ value: "data", seq: value }).pipe(Effect.provide(http.layer));
      const request = yield* Queue.take(http.requests);
      expect(request.headers["x-extension"]).toBe(value);
      expect(request.headers["stream-seq"]).toBe(value);
    }),
  );
});
