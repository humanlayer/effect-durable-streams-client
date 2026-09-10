import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Match, Queue } from "effect";
import { AdapterState } from "./adapter-state";
import { handleCommand } from "./adapter";
import { makeScriptedHttpClient, ScriptedResponse } from "../support/http-client";

describe("Phase 2 conformance adapter", () => {
  it.effect("sends serialized initial JSON envelopes unchanged in one closed PUT", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      yield* Effect.gen(function* () {
        yield* handleCommand({ type: "init", serverUrl: "https://streams.test" });
        for (const data of ["[1,2]", "[]", "[[1,2],[],[3]]"]) {
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
          expect(
            yield* handleCommand({
              type: "create",
              path: "/initial",
              contentType: "application/json",
              data,
              closed: true,
            }),
          ).toMatchObject({ success: true, status: 201 });
          const request = yield* Queue.take(http.requests);
          expect(request.method).toBe("PUT");
          expect(request.headers["stream-closed"]).toBe("true");
          expect(
            Match.value(request.body).pipe(
              Match.tag("Uint8Array", (body) => new TextDecoder().decode(body.body)),
              Match.orElse(() => "missing"),
            ),
          ).toBe(data);
          expect(yield* Queue.size(http.requests)).toBe(0);
        }
      }).pipe(
        Effect.provide(Layer.merge(http.layer, Layer.effect(AdapterState, AdapterState.make))),
      );
    }),
  );

  it.effect(
    "routes lifecycle commands through SDK and reports actual dynamic request metadata",
    () =>
      Effect.gen(function* () {
        const http = yield* makeScriptedHttpClient;
        yield* Effect.gen(function* () {
          const init = yield* handleCommand({ type: "init", serverUrl: "https://streams.test" });
          expect(init).toMatchObject({
            features: {
              dynamicHeaders: true,
              retryOptions: true,
              batching: true,
              sse: true,
              longPoll: true,
              streaming: true,
              auto: false,
              batchItems: false,
              strictZeroValidation: true,
            },
          });
          yield* http.respond(
            ScriptedResponse.Response({
              status: 201,
              headers: { "content-type": "text/plain", "stream-next-offset": "start" },
            }),
          );
          expect(
            yield* handleCommand({ type: "create", path: "/orders", contentType: "text/plain" }),
          ).toMatchObject({ type: "create", status: 201 });
          expect((yield* Queue.take(http.requests)).method).toBe("PUT");
          yield* handleCommand({
            type: "set-dynamic-header",
            name: "X-Counter",
            valueType: "counter",
          });
          yield* handleCommand({
            type: "set-dynamic-header",
            name: "Authorization",
            valueType: "token",
            initialValue: "Bearer token",
          });
          yield* handleCommand({
            type: "set-dynamic-param",
            name: "attempt",
            valueType: "counter",
          });
          for (const n of [1, 2]) {
            yield* http.respond(
              ScriptedResponse.Response({
                status: 204,
                headers: { "stream-next-offset": `tail${n}` },
              }),
            );
            expect(
              yield* handleCommand({ type: "append", path: "/orders", data: "value" }),
            ).toMatchObject({
              type: "append",
              status: 200,
              headersSent: { "X-Counter": String(n), Authorization: "Bearer token" },
              paramsSent: { attempt: String(n) },
            });
            const request = yield* Queue.take(http.requests);
            expect(request.method).toBe("POST");
            expect(request.headers).toMatchObject({
              "x-counter": String(n),
              authorization: "Bearer token",
            });
            expect(request.urlParams).toContainEqual(["attempt", String(n)]);
          }
          yield* handleCommand({ type: "clear-dynamic" });
          yield* http.respond(
            ScriptedResponse.Response({
              status: 204,
              headers: { "stream-next-offset": "tail2", "stream-closed": "true" },
            }),
          );
          expect(yield* handleCommand({ type: "close", path: "/orders" })).toEqual({
            type: "close",
            success: true,
            finalOffset: "tail2",
          });
          expect((yield* Queue.take(http.requests)).headers.authorization).toBeUndefined();
          yield* http.respond(ScriptedResponse.Response({ status: 204, headers: {} }));
          expect(yield* handleCommand({ type: "delete", path: "/orders" })).toEqual({
            type: "delete",
            success: true,
            status: 200,
          });
          expect((yield* Queue.take(http.requests)).method).toBe("DELETE");
        }).pipe(
          Effect.provide(Layer.merge(http.layer, Layer.effect(AdapterState, AdapterState.make))),
        );
      }),
  );
});
