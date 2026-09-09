import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Layer, Queue, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { HttpClient } from "effect/unstable/http";
import { makeReadHttp, readReply } from "../support/read-http.ts";
import { AdapterState } from "./adapter-state.ts";
import { handleCommand, processLine } from "./adapter.ts";

const _initialize = Effect.gen(function* () {
  yield* handleCommand({ type: "init", serverUrl: "https://streams.test" });
  const state = yield* AdapterState;
  yield* state.remember({ path: "/stream", contentType: "application/json" });
});

describe("finite adapter initial acquisition deadline", () => {
  for (const contentType of ["application/json", "text/plain"]) {
    for (const scenario of ["closed nonempty", "closed empty", "open caught up"]) {
      it.effect(`reports GET closure for ${scenario} (${contentType})`, () =>
        Effect.gen(function* () {
          const http = yield* makeReadHttp;
          yield* Effect.gen(function* () {
            yield* _initialize;
            const state = yield* AdapterState;
            yield* state.remember({ path: "/stream", contentType });
            const closed = scenario !== "open caught up";
            const empty = scenario === "closed empty";
            const data =
              contentType === "application/json" ? (empty ? "[]" : "[1]") : empty ? "" : "hello";
            yield* Queue.offer(
              http.replies,
              readReply({ offset: "tail", text: data, contentType, closed, upToDate: true }),
            );
            expect(
              yield* handleCommand({ type: "read", path: "/stream", live: false }),
            ).toMatchObject({
              type: "read",
              success: true,
              chunks: empty ? [] : [{ data, offset: "tail" }],
              offset: "tail",
              upToDate: true,
              streamClosed: closed,
            });
            const request = yield* Queue.take(http.requests);
            expect(request.request.method).toBe("GET");
            expect(request.signal.aborted).toBe(true);
            expect(yield* Queue.size(http.requests)).toBe(0);
          }).pipe(
            Effect.provide(Layer.merge(http.layer, Layer.effect(AdapterState, AdapterState.make))),
          );
        }),
      );
    }
  }

  it.effect(
    "defaults to five seconds, aborts a blocked GET and permits the next JSONL command",
    () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        yield* Effect.gen(function* () {
          yield* _initialize;
          yield* handleCommand({
            type: "set-dynamic-header",
            name: "X-Request",
            valueType: "counter",
          });
          const results = yield* Queue.unbounded<string>();
          const run = yield* Stream.make(
            '{"type":"read","path":"/stream","live":false}',
            '{"type":"head","path":"/stream"}',
          ).pipe(
            Stream.mapEffect(processLine),
            Stream.runForEach((line) => Queue.offer(results, line)),
            Effect.forkChild,
          );
          const blocked = yield* Queue.take(http.requests);
          expect(blocked.request.method).toBe("GET");
          yield* TestClock.adjust("4999 millis");
          expect(blocked.signal.aborted).toBe(false);
          expect(yield* Queue.size(results)).toBe(0);
          yield* TestClock.adjust("1 millis");
          const first = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(
            yield* Queue.take(results),
          );
          expect(first).toMatchObject({
            type: "read",
            success: true,
            status: 200,
            chunks: [],
            offset: "-1",
            upToDate: true,
            headersSent: { "X-Request": "1" },
          });
          expect(blocked.signal.aborted).toBe(true);
          expect(first).not.toHaveProperty("streamClosed");
          const following = yield* Queue.take(http.requests);
          expect(following.request.method).toBe("HEAD");
          yield* Queue.offer(
            http.replies,
            readReply({ offset: "tail", text: "", contentType: "application/json" }),
          );
          expect(
            yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(
              yield* Queue.take(results),
            ),
          ).toMatchObject({ type: "head", success: true, offset: "tail" });
          yield* Fiber.join(run);
          expect(following.signal.aborted).toBe(true);
          expect(yield* Queue.size(http.requests)).toBe(0);
        }).pipe(
          Effect.provide(Layer.merge(http.layer, Layer.effect(AdapterState, AdapterState.make))),
        );
      }),
  );

  it.effect("honors a longer explicit deadline and preserves the requested opaque offset", () =>
    Effect.gen(function* () {
      const http = yield* makeReadHttp;
      yield* Effect.gen(function* () {
        yield* _initialize;
        const completed = yield* Deferred.make<void>();
        const run = yield* handleCommand({
          type: "read",
          path: "/stream",
          offset: "Opaque+token",
          timeoutMs: 7500,
          live: false,
        }).pipe(
          Effect.tap(() => Deferred.succeed(completed, undefined)),
          Effect.forkChild,
        );
        const blocked = yield* Queue.take(http.requests);
        yield* TestClock.adjust("7499 millis");
        expect(yield* Deferred.isDone(completed)).toBe(false);
        expect(blocked.signal.aborted).toBe(false);
        yield* TestClock.adjust("1 millis");
        expect(yield* Fiber.join(run)).toMatchObject({
          type: "read",
          success: true,
          status: 200,
          chunks: [],
          offset: "Opaque+token",
          upToDate: true,
        });
        expect(blocked.signal.aborted).toBe(true);
      }).pipe(
        Effect.provide(Layer.merge(http.layer, Layer.effect(AdapterState, AdapterState.make))),
      );
    }),
  );

  it.effect(
    "applies one deadline across retry waits and waits for response cleanup before returning",
    () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        yield* Effect.gen(function* () {
          yield* _initialize;
          const releaseStarted = yield* Deferred.make<void>();
          const releaseAllowed = yield* Deferred.make<void>();
          const completed = yield* Deferred.make<void>();
          const run = yield* handleCommand({
            type: "read",
            path: "/stream",
            timeoutMs: 100,
            live: false,
          }).pipe(
            Effect.tap(() => Deferred.succeed(completed, undefined)),
            Effect.forkChild,
          );
          const request = yield* Queue.take(http.requests);
          yield* Queue.offer(http.replies, {
            status: 429,
            headers: { "retry-after": "60" },
            body: Stream.never.pipe(
              Stream.ensuring(
                Deferred.succeed(releaseStarted, undefined).pipe(
                  Effect.andThen(Deferred.await(releaseAllowed)),
                ),
              ),
            ),
          });
          yield* TestClock.adjust("100 millis");
          yield* Deferred.await(releaseStarted);
          expect(yield* Deferred.isDone(completed)).toBe(false);
          yield* Deferred.succeed(releaseAllowed, undefined);
          expect(yield* Fiber.join(run)).toMatchObject({ type: "read", chunks: [], success: true });
          expect(request.signal.aborted).toBe(true);
          yield* TestClock.adjust("2 minutes");
          expect(yield* Queue.size(http.requests)).toBe(0);
        }).pipe(
          Effect.provide(Layer.merge(http.layer, Layer.effect(AdapterState, AdapterState.make))),
        );
      }),
  );

  it.effect("does not reset or disarm the initial deadline for retryable responses", () =>
    Effect.gen(function* () {
      const http = yield* makeReadHttp;
      yield* Effect.gen(function* () {
        yield* _initialize;
        const run = yield* handleCommand({
          type: "read",
          path: "/stream",
          timeoutMs: 1500,
          live: false,
        }).pipe(Effect.forkChild);
        const first = yield* Queue.take(http.requests);
        yield* Queue.offer(http.replies, {
          status: 503,
          headers: { "retry-after": "1" },
          body: Stream.empty,
        });
        yield* TestClock.adjust("1 second");
        const second = yield* Queue.take(http.requests);
        expect(first.signal.aborted).toBe(true);
        yield* Queue.offer(http.replies, {
          status: 429,
          headers: { "retry-after": "60" },
          body: Stream.empty,
        });
        yield* TestClock.adjust("500 millis");
        expect(yield* Fiber.join(run)).toMatchObject({ type: "read", success: true, chunks: [] });
        expect(second.signal.aborted).toBe(true);
        yield* TestClock.adjust("2 minutes");
        expect(yield* Queue.size(http.requests)).toBe(0);
      }).pipe(
        Effect.provide(Layer.merge(http.layer, Layer.effect(AdapterState, AdapterState.make))),
      );
    }),
  );

  it.effect(
    "disarms at initial headers, not after body collection or later catch-up requests",
    () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        yield* Effect.gen(function* () {
          yield* _initialize;
          const bodyEntered = yield* Deferred.make<void>();
          const bodyAllowed = yield* Deferred.make<void>();
          const completed = yield* Deferred.make<void>();
          const run = yield* handleCommand({
            type: "read",
            path: "/stream",
            timeoutMs: 100,
            live: false,
          }).pipe(
            Effect.tap(() => Deferred.succeed(completed, undefined)),
            Effect.forkChild,
          );
          const first = yield* Queue.take(http.requests);
          yield* Queue.offer(http.replies, {
            ...readReply({ offset: "middle", text: "", contentType: "application/json" }),
            body: Stream.fromEffect(
              Deferred.succeed(bodyEntered, undefined).pipe(
                Effect.andThen(Deferred.await(bodyAllowed)),
                Effect.as(new TextEncoder().encode("[1]")),
              ),
            ),
          });
          yield* Deferred.await(bodyEntered);
          yield* TestClock.adjust("10 seconds");
          expect(yield* Deferred.isDone(completed)).toBe(false);
          expect(first.signal.aborted).toBe(false);
          yield* Deferred.succeed(bodyAllowed, undefined);
          const second = yield* Queue.take(http.requests);
          expect(first.signal.aborted).toBe(true);
          yield* TestClock.adjust("10 seconds");
          expect(yield* Deferred.isDone(completed)).toBe(false);
          expect(second.signal.aborted).toBe(false);
          yield* Queue.offer(
            http.replies,
            readReply({
              offset: "tail",
              text: "[[2,3],null]",
              contentType: "application/json",
              upToDate: true,
              closed: true,
            }),
          );
          expect(yield* Fiber.join(run)).toMatchObject({
            type: "read",
            success: true,
            chunks: [{ data: "[1,[2,3],null]" }],
            offset: "tail",
            streamClosed: true,
          });
          expect(second.signal.aborted).toBe(true);
          expect(yield* Queue.size(http.requests)).toBe(0);
        }).pipe(
          Effect.provide(Layer.merge(http.layer, Layer.effect(AdapterState, AdapterState.make))),
        );
      }),
  );

  it.effect(
    "bounds optional discovery HEAD without mistaking its headers for GET acquisition",
    () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        yield* Effect.gen(function* () {
          yield* handleCommand({ type: "init", serverUrl: "https://streams.test" });
          const run = yield* handleCommand({
            type: "read",
            path: "/unknown",
            live: false,
            timeoutMs: 100,
          }).pipe(Effect.forkChild);
          const head = yield* Queue.take(http.requests);
          expect(head.request.method).toBe("HEAD");
          yield* Queue.offer(http.replies, readReply({ offset: "tail", text: "", closed: true }));
          const get = yield* Queue.take(http.requests);
          expect(get.request.method).toBe("GET");
          yield* TestClock.adjust("100 millis");
          const result = yield* Fiber.join(run);
          expect(result).toMatchObject({ type: "read", success: true, chunks: [] });
          expect(result).not.toHaveProperty("streamClosed");
          expect(head.signal.aborted && get.signal.aborted).toBe(true);
        }).pipe(
          Effect.provide(Layer.merge(http.layer, Layer.effect(AdapterState, AdapterState.make))),
        );
      }),
  );

  it.effect("preserves pre-deadline error normalization and caller interruption", () =>
    Effect.gen(function* () {
      const http = yield* makeReadHttp;
      yield* Effect.gen(function* () {
        yield* _initialize;
        yield* Queue.offer(http.replies, { status: 404, headers: {}, body: Stream.empty });
        expect(
          yield* handleCommand({ type: "read", path: "/stream", live: false }).pipe(
            Effect.provideServiceEffect(
              HttpClient.HttpClient,
              Effect.map(HttpClient.HttpClient, HttpClient.filterStatusOk),
            ),
          ),
        ).toMatchObject({ type: "error", errorCode: "NOT_FOUND", status: 404 });
        yield* Queue.take(http.requests);
        const run = yield* handleCommand({ type: "read", path: "/stream", live: false }).pipe(
          Effect.forkChild,
        );
        const blocked = yield* Queue.take(http.requests);
        yield* Fiber.interrupt(run);
        expect(Exit.hasInterrupts(yield* Fiber.await(run))).toBe(true);
        expect(blocked.signal.aborted).toBe(true);
        yield* TestClock.adjust("1 minute");
        expect(yield* Queue.size(http.requests)).toBe(0);
        expect(yield* handleCommand({ type: "shutdown" })).toEqual({
          type: "shutdown",
          success: true,
        });
      }).pipe(
        Effect.provide(Layer.merge(http.layer, Layer.effect(AdapterState, AdapterState.make))),
      );
    }),
  );
});
