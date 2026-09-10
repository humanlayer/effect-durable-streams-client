import { describe, expect, it } from "@effect/vitest";
import {
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Queue,
  Schema,
  Scope,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import { HttpClient } from "effect/unstable/http";
import { AdapterState } from "./adapter-state.ts";
import { handleCommand, processLine } from "./adapter.ts";
import { makeReadHttp, readReply } from "../support/read-http.ts";

const _initialize = Effect.gen(function* () {
  yield* handleCommand({ type: "init", serverUrl: "https://streams.test" });
  const state = yield* AdapterState;
  yield* state.remember({ path: "/stream", contentType: "application/json" });
});

describe("pinned runner live read contract", () => {
  it.effect(
    "counts response batches, not JSON items, and stops maxChunks without an extra dispatch",
    () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        yield* Effect.gen(function* () {
          yield* _initialize;
          yield* handleCommand({
            type: "set-dynamic-header",
            name: "X-Attempt",
            valueType: "counter",
          });
          const run = yield* processLine(
            '{"type":"read","path":"/stream","live":"long-poll","maxChunks":2}',
          ).pipe(Effect.forkChild);
          yield* Queue.take(http.requests);
          yield* Queue.offer(
            http.replies,
            readReply({
              offset: "first",
              text: "[1,[2,3],null]",
              contentType: "application/json",
              upToDate: true,
            }),
          );
          const poll = yield* Queue.take(http.requests);
          expect(poll.request.headers["x-attempt"]).toBe("2");
          yield* Queue.offer(http.replies, {
            status: 204,
            headers: {
              "stream-next-offset": "first",
              "stream-up-to-date": "true",
              "stream-cursor": "c1",
            },
            body: Stream.empty,
          });
          const next = yield* Queue.take(http.requests);
          expect(next.request.headers["x-attempt"]).toBe("3");
          const page = readReply({
            offset: "last",
            text: "[false,4]",
            contentType: "application/json",
            upToDate: true,
          });
          yield* Queue.offer(http.replies, {
            ...page,
            headers: { ...page.headers, "stream-cursor": "c2" },
          });
          const result = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(
            yield* Fiber.join(run),
          );
          expect(result).toMatchObject({
            type: "read",
            success: true,
            chunks: [
              { data: "[1,[2,3],null]", offset: "first" },
              { data: "[false,4]", offset: "last" },
            ],
            offset: "last",
            upToDate: true,
            streamClosed: false,
            headersSent: { "X-Attempt": "3" },
          });
          expect(next.signal.aborted).toBe(true);
          expect(yield* Queue.size(http.requests)).toBe(0);
        }).pipe(
          Effect.provide(Layer.merge(http.layer, Layer.effect(AdapterState, AdapterState.make))),
        );
      }),
  );

  for (const contentType of ["application/json", "text/plain"]) {
    for (const empty of [false, true]) {
      it.effect(`waitForUpToDate stops after full ${contentType} catch-up, empty=${empty}`, () =>
        Effect.gen(function* () {
          const http = yield* makeReadHttp;
          yield* Effect.gen(function* () {
            yield* _initialize;
            const state = yield* AdapterState;
            yield* state.remember({ path: "/stream", contentType });
            const data =
              contentType === "application/json" ? (empty ? "[]" : "[1,2]") : empty ? "" : "hello";
            yield* Queue.offer(
              http.replies,
              readReply({ offset: "tail", text: data, contentType, upToDate: true }),
            );
            expect(
              yield* handleCommand({
                type: "read",
                path: "/stream",
                live: "long-poll",
                waitForUpToDate: true,
              }),
            ).toMatchObject({
              type: "read",
              chunks: empty ? [] : [{ data, offset: "tail" }],
              offset: "tail",
              upToDate: true,
              streamClosed: false,
            });
            expect((yield* Queue.take(http.requests)).signal.aborted).toBe(true);
            expect(yield* Queue.size(http.requests)).toBe(0);
          }).pipe(
            Effect.provide(Layer.merge(http.layer, Layer.effect(AdapterState, AdapterState.make))),
          );
        }),
      );
    }
  }

  it.effect(
    "uses separate initial and live deadlines and aborts retry waits before the next command",
    () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        yield* Effect.gen(function* () {
          yield* _initialize;
          const results = yield* Queue.unbounded<string>();
          const run = yield* Stream.make(
            '{"type":"read","path":"/stream","live":"long-poll","timeoutMs":1000}',
            '{"type":"head","path":"/stream"}',
          ).pipe(
            Stream.mapEffect(processLine),
            Stream.runForEach((line) => Queue.offer(results, line)),
            Effect.forkChild,
          );
          const first = yield* Queue.take(http.requests);
          yield* TestClock.adjust("900 millis");
          yield* Queue.offer(
            http.replies,
            readReply({
              offset: "tail",
              text: "[1]",
              contentType: "application/json",
              upToDate: true,
            }),
          );
          const poll = yield* Queue.take(http.requests);
          yield* Queue.offer(http.replies, {
            status: 503,
            headers: { "retry-after": "60" },
            body: Stream.empty,
          });
          yield* TestClock.adjust("999 millis");
          expect(yield* Queue.size(results)).toBe(0);
          yield* TestClock.adjust("1 millis");
          const result = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(
            yield* Queue.take(results),
          );
          expect(result).toMatchObject({
            type: "read",
            success: true,
            chunks: [{ data: "[1]" }],
            offset: "tail",
            upToDate: true,
            streamClosed: false,
          });
          expect(first.signal.aborted && poll.signal.aborted).toBe(true);
          const head = yield* Queue.take(http.requests);
          expect(head.request.method).toBe("HEAD");
          yield* Queue.offer(http.replies, readReply({ offset: "tail", text: "" }));
          yield* Fiber.join(run);
          yield* TestClock.adjust("2 minutes");
          expect(yield* Queue.size(http.requests)).toBe(0);
        }).pipe(
          Effect.provide(Layer.merge(http.layer, Layer.effect(AdapterState, AdapterState.make))),
        );
      }),
  );

  it.effect("initial timeout preserves requested position and does not fabricate closure", () =>
    Effect.gen(function* () {
      const http = yield* makeReadHttp;
      yield* Effect.gen(function* () {
        yield* _initialize;
        const run = yield* handleCommand({
          type: "read",
          path: "/stream",
          live: "long-poll",
          offset: "Opaque+",
          timeoutMs: 200,
        }).pipe(Effect.forkChild);
        const request = yield* Queue.take(http.requests);
        yield* TestClock.adjust("200 millis");
        const result = yield* Fiber.join(run);
        expect(result).toMatchObject({
          type: "read",
          success: true,
          chunks: [],
          offset: "Opaque+",
          upToDate: true,
        });
        expect(result).not.toHaveProperty("streamClosed");
        expect(request.signal.aborted).toBe(true);
      }).pipe(
        Effect.provide(Layer.merge(http.layer, Layer.effect(AdapterState, AdapterState.make))),
      );
    }),
  );

  for (const contentType of ["application/json", "text/plain"]) {
    for (const closed of [false, true]) {
      it.effect(
        `retains committed ${contentType} through deadline during HTTP cleanup, closed=${closed}`,
        () =>
          Effect.gen(function* () {
            const http = yield* makeReadHttp;
            yield* Effect.gen(function* () {
              yield* _initialize;
              const state = yield* AdapterState;
              yield* state.remember({ path: "/stream", contentType });
              const releasing = yield* Deferred.make<void>();
              const release = yield* Deferred.make<void>();
              const completed = yield* Deferred.make<void>();
              const requests = { count: 0 };
              const run = yield* handleCommand({
                type: "read",
                path: "/stream",
                live: "long-poll",
                offset: closed ? "tail" : undefined,
                maxChunks: 1,
                timeoutMs: 100,
              }).pipe(
                Effect.provideServiceEffect(
                  HttpClient.HttpClient,
                  Effect.map(HttpClient.HttpClient, (client) =>
                    client.pipe(
                      HttpClient.mapRequestInputEffect((request) =>
                        Effect.withFiber((fiber) =>
                          ++requests.count === (closed ? 2 : 1)
                            ? Scope.addFinalizer(
                                Context.getUnsafe(fiber.context, Scope.Scope),
                                Deferred.succeed(releasing, undefined).pipe(
                                  Effect.andThen(Deferred.await(release)),
                                  Effect.uninterruptible,
                                ),
                              )
                            : Effect.void,
                        ).pipe(Effect.as(request)),
                      ),
                    ),
                  ),
                ),
                Effect.tap(() => Deferred.succeed(completed, undefined)),
                Effect.forkChild,
              );
              if (closed) {
                const initial = yield* Queue.take(http.requests);
                yield* Queue.offer(
                  http.replies,
                  readReply({
                    offset: "tail",
                    text: contentType === "application/json" ? "[]" : "",
                    contentType,
                    upToDate: true,
                  }),
                );
                expect(initial.request.method).toBe("GET");
              }
              const request = yield* Queue.take(http.requests);
              const data = contentType === "application/json" ? "[1,2]" : "hello";
              yield* Queue.offer(
                http.replies,
                readReply({ offset: "tail", text: data, contentType, upToDate: true, closed }),
              );
              yield* Deferred.await(releasing);
              yield* TestClock.adjust("100 millis");
              expect(yield* Deferred.isDone(completed)).toBe(false);
              expect(yield* Queue.size(http.requests)).toBe(0);
              yield* Deferred.succeed(release, undefined);
              expect(yield* Fiber.join(run)).toMatchObject({
                type: "read",
                success: true,
                chunks: [{ data, offset: "tail" }],
                offset: "tail",
                upToDate: true,
                streamClosed: closed,
              });
              expect(request.signal.aborted).toBe(true);
              yield* TestClock.adjust("1 minute");
              expect(yield* Queue.size(http.requests)).toBe(0);
            }).pipe(
              Effect.provide(
                Layer.merge(http.layer, Layer.effect(AdapterState, AdapterState.make)),
              ),
            );
          }),
      );
    }
  }

  for (const contentType of ["application/json", "text/plain"]) {
    for (const offset of ["unsafe", "safe"]) {
      it.effect(
        `discards incomplete ${contentType} body at ${offset} on timeout and joins delayed cleanup`,
        () =>
          Effect.gen(function* () {
            const http = yield* makeReadHttp;
            yield* Effect.gen(function* () {
              yield* _initialize;
              const state = yield* AdapterState;
              yield* state.remember({ path: "/stream", contentType });
              const entered = yield* Deferred.make<void>();
              const releasing = yield* Deferred.make<void>();
              const release = yield* Deferred.make<void>();
              const completed = yield* Deferred.make<void>();
              const run = yield* handleCommand({
                type: "read",
                path: "/stream",
                live: "long-poll",
                timeoutMs: 100,
                maxChunks: 1,
              }).pipe(
                Effect.tap(() => Deferred.succeed(completed, undefined)),
                Effect.forkChild,
              );
              yield* Queue.take(http.requests);
              yield* Queue.offer(
                http.replies,
                readReply({
                  offset: "safe",
                  text: contentType === "application/json" ? "[]" : "",
                  contentType,
                  upToDate: true,
                }),
              );
              const request = yield* Queue.take(http.requests);
              const page = readReply({
                offset,
                text: "",
                contentType,
                closed: true,
              });
              yield* Queue.offer(http.replies, {
                ...page,
                headers: { ...page.headers, "stream-cursor": "c" },
                body: Stream.fromEffect(
                  Deferred.succeed(entered, undefined).pipe(
                    Effect.as(new TextEncoder().encode("[1,")),
                  ),
                ).pipe(
                  Stream.concat(Stream.never),
                  Stream.ensuring(
                    Deferred.succeed(releasing, undefined).pipe(
                      Effect.andThen(Deferred.await(release)),
                    ),
                  ),
                ),
              });
              yield* Deferred.await(entered);
              yield* TestClock.adjust("100 millis");
              yield* Deferred.await(releasing);
              expect(yield* Deferred.isDone(completed)).toBe(false);
              yield* Deferred.succeed(release, undefined);
              expect(yield* Fiber.join(run)).toMatchObject({
                type: "read",
                chunks: [],
                offset: "safe",
                streamClosed: false,
              });
              expect(request.signal.aborted).toBe(true);
              yield* TestClock.adjust("1 minute");
              expect(yield* Queue.size(http.requests)).toBe(0);
            }).pipe(
              Effect.provide(
                Layer.merge(http.layer, Layer.effect(AdapterState, AdapterState.make)),
              ),
            );
          }),
      );
    }
  }

  it.effect(
    "emits final decoded data with closure and preserves errors and external interruption",
    () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        yield* Effect.gen(function* () {
          yield* _initialize;
          for (const text of ["[1]", "[]", "invalid"]) {
            const run = yield* handleCommand({
              type: "read",
              path: "/stream",
              live: "long-poll",
            }).pipe(Effect.forkChild);
            yield* Queue.take(http.requests);
            yield* Queue.offer(
              http.replies,
              readReply({
                offset: "tail",
                text: "[]",
                contentType: "application/json",
                upToDate: true,
              }),
            );
            const request = yield* Queue.take(http.requests);
            const page = readReply({
              offset: "final",
              text,
              contentType: "application/json",
              closed: true,
            });
            yield* Queue.offer(http.replies, {
              ...page,
              headers: { ...page.headers, "stream-cursor": "c" },
            });
            expect(yield* Fiber.join(run)).toMatchObject(
              text === "invalid"
                ? { type: "error", errorCode: "PARSE_ERROR" }
                : {
                    type: "read",
                    streamClosed: true,
                    offset: "final",
                    chunks: text === "[]" ? [] : [{ data: "[1]" }],
                  },
            );
            expect(request.signal.aborted).toBe(true);
          }
          const run = yield* handleCommand({
            type: "read",
            path: "/stream",
            live: "long-poll",
          }).pipe(Effect.forkChild);
          const request = yield* Queue.take(http.requests);
          yield* Fiber.interrupt(run);
          expect(Exit.hasInterrupts(yield* Fiber.await(run))).toBe(true);
          expect(request.signal.aborted).toBe(true);
          expect(yield* Queue.size(http.requests)).toBe(0);
          expect(yield* handleCommand({ type: "read", path: "/stream", live: true })).toMatchObject(
            { errorCode: "NOT_SUPPORTED" },
          );
        }).pipe(
          Effect.provide(Layer.merge(http.layer, Layer.effect(AdapterState, AdapterState.make))),
        );
      }),
  );
});
