import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Fiber, Layer, Option, Queue, Schema, Stream } from "effect";
import { FetchHttpClient, HttpClient } from "effect/unstable/http";
import { DurableStreamsClient } from "../../src/index.js";
import { acquireDurableStreamServer } from "../support/server.js";
import { AdapterState } from "../conformance/adapter-state.js";
import { handleCommand } from "../conformance/adapter.js";

describe("reference server long-poll", () => {
  it.effect(
    "catches up, receives later append and final append, then completes at the actual tail",
    () =>
      Effect.gen(function* () {
        const { baseUrl } = yield* acquireDurableStreamServer;
        const polls = yield* Queue.unbounded<string>();
        const observed = yield* Queue.unbounded<string>();
        const writer = yield* DurableStreamsClient.make({
          url: baseUrl + "/live",
          contentType: "text/plain",
        });
        yield* writer.create({ value: "history" });
        const reader = yield* DurableStreamsClient.make({
          url: baseUrl + "/live",
          live: "long-poll",
        });
        const run = yield* reader.text.pipe(
          Stream.runForEach((text) => Queue.offer(observed, text)),
          Effect.provideServiceEffect(
            HttpClient.HttpClient,
            Effect.map(HttpClient.HttpClient, (http) =>
              http.pipe(
                HttpClient.mapRequestEffect((request) =>
                  request.url.includes("live=long-poll")
                    ? Queue.offer(polls, request.url).pipe(Effect.as(request))
                    : Effect.succeed(request),
                ),
              ),
            ),
          ),
          Effect.forkChild,
        );
        expect(yield* Queue.take(observed)).toBe("history");
        yield* Queue.take(polls);
        yield* writer.append({ value: "later" });
        expect(yield* Queue.take(observed)).toBe("later");
        yield* Queue.take(polls);
        const closed = yield* writer.close({ value: "final" });
        expect(yield* Queue.take(observed)).toBe("final");
        yield* Fiber.join(run);
        expect(yield* reader.offset).toEqual(Option.some(closed.finalOffset));
        expect(yield* writer.head).toMatchObject({ closed: true, offset: closed.finalOffset });
      }).pipe(Effect.provide(FetchHttpClient.layer)),
  );

  it.effect("now skips history and schema JSON delivers future data before bodyless closure", () =>
    Effect.gen(function* () {
      const { baseUrl } = yield* acquireDurableStreamServer;
      const entered = yield* Queue.unbounded<void>();
      const delivered = yield* Deferred.make<void>();
      const writer = yield* DurableStreamsClient.make({
        url: baseUrl + "/json",
        contentType: "application/json",
      });
      yield* writer.create({ value: { id: 0 } });
      const reader = yield* DurableStreamsClient.make({
        url: baseUrl + "/json",
        live: "long-poll",
        offset: "now",
        schema: Schema.Struct({ id: Schema.Int }),
      });
      const run = yield* reader.json.pipe(
        Stream.tap(() => Deferred.succeed(delivered, undefined)),
        Stream.runCollect,
        Effect.provideServiceEffect(
          HttpClient.HttpClient,
          Effect.map(HttpClient.HttpClient, (http) =>
            http.pipe(
              HttpClient.mapRequestEffect((request) =>
                request.url.includes("live=long-poll")
                  ? Queue.offer(entered, undefined).pipe(Effect.as(request))
                  : Effect.succeed(request),
              ),
            ),
          ),
        ),
        Effect.forkChild,
      );
      yield* Queue.take(entered);
      yield* writer.append({ value: { id: 1 } });
      yield* Deferred.await(delivered);
      yield* Queue.take(entered);
      const closed = yield* writer.close({});
      expect(yield* Fiber.join(run)).toEqual([{ id: 1 }]);
      expect(yield* reader.offset).toEqual(Option.some(closed.finalOffset));
    }).pipe(Effect.provide(FetchHttpClient.layer)),
  );

  it.effect(
    "uses the pinned runner's ordinary read as a background operation with external writes",
    () =>
      Effect.gen(function* () {
        const { baseUrl } = yield* acquireDurableStreamServer;
        const entered = yield* Deferred.make<void>();
        expect(yield* handleCommand({ type: "init", serverUrl: baseUrl })).toMatchObject({
          features: { longPoll: true, sse: true, auto: false },
        });
        yield* handleCommand({
          type: "create",
          path: "/adapter",
          contentType: "application/json",
          data: "[0]",
        });
        const read = yield* handleCommand({
          type: "read",
          path: "/adapter",
          live: "long-poll",
          offset: "now",
          maxChunks: 1,
        }).pipe(
          Effect.provideServiceEffect(
            HttpClient.HttpClient,
            Effect.map(HttpClient.HttpClient, (http) =>
              http.pipe(
                HttpClient.mapRequestEffect((request) =>
                  request.url.includes("live=long-poll")
                    ? Deferred.succeed(entered, undefined).pipe(Effect.as(request))
                    : Effect.succeed(request),
                ),
              ),
            ),
          ),
          Effect.forkChild,
        );
        yield* Deferred.await(entered);
        const writer = yield* DurableStreamsClient.make({
          url: baseUrl + "/adapter",
          contentType: "application/json",
        });
        const receipt = yield* writer.append({ value: [1, 2] });
        expect(yield* Fiber.join(read)).toMatchObject({
          type: "read",
          success: true,
          chunks: [{ data: "[[1,2]]", offset: receipt.offset }],
          offset: receipt.offset,
          streamClosed: false,
        });
        expect(yield* handleCommand({ type: "head", path: "/adapter" })).toMatchObject({
          offset: receipt.offset,
        });
        expect(yield* handleCommand({ type: "shutdown" })).toEqual({
          type: "shutdown",
          success: true,
        });
      }).pipe(
        Effect.provide(
          Layer.merge(FetchHttpClient.layer, Layer.effect(AdapterState, AdapterState.make)),
        ),
      ),
  );
});
