import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Fiber, Layer, Queue, Stream } from "effect";
import { TestClock } from "effect/testing";
import { AdapterState } from "./adapter-state.ts";
import { handleCommand } from "./adapter.ts";
import { makeReadHttp, readReply } from "../support/read-http.ts";

describe("SSE adapter deadline ownership", () => {
  for (const acknowledged of [false, true]) {
    it.effect(`joins delayed cleanup and retains only acknowledged data: ${acknowledged}`, () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        yield* Effect.gen(function* () {
          yield* handleCommand({ type: "init", serverUrl: "https://streams.test" });
          const state = yield* AdapterState;
          yield* state.remember({ path: "/s", contentType: "text/plain" });
          const releasing = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const complete = yield* Deferred.make<void>();
          const entered = yield* Deferred.make<void>();
          const run = yield* handleCommand({
            type: "read",
            path: "/s",
            live: "sse",
            timeoutMs: 100,
          }).pipe(
            Effect.tap(() => Deferred.succeed(complete, undefined)),
            Effect.forkChild,
          );
          yield* Queue.take(http.requests);
          yield* Queue.offer(http.replies, readReply({ offset: "safe", text: "", upToDate: true }));
          const request = yield* Queue.take(http.requests);
          const wire =
            "event: data\ndata: payload\n\n" +
            (acknowledged
              ? 'event: control\ndata: {"streamNextOffset":"new","streamCursor":"c","upToDate":true}\n\n'
              : "");
          yield* Queue.offer(http.replies, {
            status: 200,
            headers: { "content-type": "text/event-stream" },
            body: Stream.make(new TextEncoder().encode(wire)).pipe(
              Stream.concat(
                Stream.fromEffectDrain(
                  Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
                ),
              ),
              Stream.ensuring(
                Deferred.succeed(releasing, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                ),
              ),
            ),
          });
          yield* Deferred.await(entered);
          const clock = yield* TestClock.adjust("100 millis").pipe(Effect.forkChild);
          yield* Deferred.await(releasing);
          expect(yield* Deferred.isDone(complete)).toBe(false);
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(clock);
          expect(yield* Fiber.join(run)).toMatchObject({
            type: "read",
            chunks: acknowledged ? [{ data: "payload", offset: "new" }] : [],
            offset: acknowledged ? "new" : "safe",
            streamClosed: false,
          });
          expect(request.signal.aborted).toBe(true);
          yield* TestClock.adjust("1 hour");
          expect(yield* Queue.size(http.requests)).toBe(0);
        }).pipe(
          Effect.provide(Layer.merge(http.layer, Layer.effect(AdapterState, AdapterState.make))),
        );
      }),
    );
  }
});
