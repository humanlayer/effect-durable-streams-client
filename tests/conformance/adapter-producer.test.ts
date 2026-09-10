import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Fiber, Layer, Queue } from "effect";
import { AdapterState } from "./adapter-state.ts";
import { handleCommand } from "./adapter.ts";
import { makeProducerHttp, producerReply } from "../support/producer-http.ts";
import { ScriptedResponse } from "../support/http-client.ts";

describe("conformance producer adapter", () => {
  it.effect("retains cached producer sequences, JSON bodies and idempotent close", () =>
    Effect.gen(function* () {
      const http = yield* makeProducerHttp;
      yield* Effect.gen(function* () {
        yield* handleCommand({ type: "init", serverUrl: "http://localhost" });
        const state = yield* AdapterState;
        yield* state.remember({ path: "/json", contentType: "application/json" });
        for (const seq of [0, 1]) {
          const run = yield* handleCommand({
            type: "idempotent-append",
            path: "/json",
            producerId: "p",
            epoch: 0,
            autoClaim: false,
            data: String(seq),
            headers: { "x-static": "kept" },
          }).pipe(Effect.forkChild);
          const request = yield* Queue.take(http.requests);
          expect(new TextDecoder().decode(request.body)).toBe(`[${seq}]`);
          expect(request.headers).toMatchObject({
            "producer-seq": String(seq),
            "x-static": "kept",
          });
          yield* Deferred.succeed(request.reply, producerReply({ seq }));
          expect(yield* Fiber.join(run)).toMatchObject({
            type: "idempotent-append",
            success: true,
            producerSeq: seq,
          });
        }
        const close = yield* handleCommand({
          type: "idempotent-close",
          path: "/json",
          producerId: "p",
          epoch: 0,
          autoClaim: false,
          data: "[2,3]",
        }).pipe(Effect.forkChild);
        const request = yield* Queue.take(http.requests);
        expect(new TextDecoder().decode(request.body)).toBe("[[2,3]]");
        expect(request.headers["producer-seq"]).toBe("2");
        yield* Deferred.succeed(request.reply, producerReply({ seq: 2, closed: true }));
        const result = yield* Fiber.join(close);
        expect(
          yield* handleCommand({
            type: "idempotent-close",
            path: "/json",
            producerId: "p",
            epoch: 0,
            autoClaim: false,
          }),
        ).toEqual(result);
        yield* handleCommand({ type: "init", serverUrl: "http://localhost" });
        expect(state.producers.size).toBe(0);
        expect(yield* Queue.size(http.requests)).toBe(0);
      }).pipe(
        Effect.provide(Layer.merge(http.layer, Layer.effect(AdapterState, AdapterState.make))),
      );
    }),
  );

  it.effect("maps producer failures rather than reporting a successful queue drain", () =>
    Effect.gen(function* () {
      const http = yield* makeProducerHttp;
      yield* Effect.gen(function* () {
        yield* handleCommand({ type: "init", serverUrl: "http://localhost" });
        const run = yield* handleCommand({
          type: "idempotent-append-batch",
          path: "/text",
          producerId: "p",
          epoch: 0,
          autoClaim: false,
          items: ["a", "b"],
        }).pipe(Effect.forkChild);
        const request = yield* Queue.take(http.requests);
        expect(new TextDecoder().decode(request.body)).toBe("ab");
        yield* Deferred.succeed(
          request.reply,
          ScriptedResponse.Response({ status: 403, headers: { "producer-epoch": "2" } }),
        );
        expect(yield* Fiber.join(run)).toMatchObject({
          type: "error",
          errorCode: "STALE_EPOCH",
          status: 403,
        });
        expect(request.signal.aborted).toBe(true);
      }).pipe(
        Effect.provide(Layer.merge(http.layer, Layer.effect(AdapterState, AdapterState.make))),
      );
    }),
  );

  it.effect("runs constructor validation through SDK and advertises only supported features", () =>
    Effect.gen(function* () {
      const http = yield* makeProducerHttp;
      yield* Effect.gen(function* () {
        expect(yield* handleCommand({ type: "init", serverUrl: "http://localhost" })).toMatchObject(
          {
            features: {
              retryOptions: true,
              strictZeroValidation: true,
              batchItems: false,
              auto: false,
            },
          },
        );
        expect(
          yield* handleCommand({
            type: "validate",
            target: { target: "retry-options", initialDelayMs: 1000, maxDelayMs: 500 },
          }),
        ).toMatchObject({ type: "error", errorCode: "INVALID_ARGUMENT" });
        expect(
          yield* handleCommand({
            type: "validate",
            target: { target: "idempotent-producer", producerId: "", maxBatchBytes: 0 },
          }),
        ).toMatchObject({ type: "error", errorCode: "INVALID_ARGUMENT" });
        expect(
          yield* handleCommand({
            type: "validate",
            target: { target: "idempotent-producer", producerId: "ok" },
          }),
        ).toEqual({ type: "validate", success: true });
        expect(yield* Queue.size(http.requests)).toBe(0);
      }).pipe(
        Effect.provide(Layer.merge(http.layer, Layer.effect(AdapterState, AdapterState.make))),
      );
    }),
  );
});
