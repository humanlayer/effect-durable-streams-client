import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Fiber, Layer, Match, Option, Queue, Stream } from "effect";
import { FetchHttpClient, HttpClient } from "effect/unstable/http";
import { DurableStreamsClient } from "../../src/index.js";
import { acquireDurableStreamServer } from "../support/server.js";
import { AdapterState } from "../conformance/adapter-state.js";
import { handleCommand } from "../conformance/adapter.js";

describe("reference server SSE", () => {
  for (const contentType of ["text/plain", "application/json", "application/octet-stream"]) {
    it.effect(`delivers live ${contentType}, resumes and closes without reconnect`, () =>
      Effect.gen(function* () {
        const { baseUrl } = yield* acquireDurableStreamServer;
        const entered = yield* Deferred.make<void>();
        const requests = yield* Queue.unbounded<string>();
        const writer = yield* DurableStreamsClient.make({ url: baseUrl + "/sse", contentType });
        yield* writer.create({});
        const reader = yield* DurableStreamsClient.make({
          url: baseUrl + "/sse",
          live: "sse",
          offset: "now",
        });
        const source =
          contentType === "application/json"
            ? reader.json.pipe(Stream.map((value) => value))
            : reader.bytes.pipe(Stream.map((bytes) => Array.from(bytes)));
        const run = yield* source.pipe(
          Stream.runCollect,
          Effect.provideServiceEffect(
            HttpClient.HttpClient,
            Effect.map(HttpClient.HttpClient, (http) =>
              http.pipe(
                HttpClient.tap((response) => {
                  if (!response.request.url.includes("live=sse")) return Effect.void;
                  if (contentType === "application/octet-stream")
                    expect(response.headers["stream-sse-data-encoding"]).toBe("base64");
                  return Queue.offer(requests, response.request.url).pipe(
                    Effect.andThen(Deferred.succeed(entered, undefined)),
                  );
                }),
              ),
            ),
          ),
          Effect.forkChild,
        );
        yield* Deferred.await(entered);
        const value = Match.value(contentType).pipe(
          Match.when("application/json", () => [1, [2, 3], null]),
          Match.when("text/plain", () => "hé\nthere"),
          Match.orElse(() => new Uint8Array([0, 255, 128, 13, 10])),
        );
        const receipt = yield* writer.close({ value });
        const values = yield* Fiber.join(run);
        expect(values).toEqual(
          contentType === "application/json"
            ? [[1, [2, 3], null]]
            : [
                Array.from(
                  contentType === "text/plain"
                    ? new TextEncoder().encode("hé\nthere")
                    : new Uint8Array([0, 255, 128, 13, 10]),
                ),
              ],
        );
        expect(yield* reader.offset).toEqual(Option.some(receipt.finalOffset));
        yield* Queue.take(requests);
        expect(yield* Queue.size(requests)).toBe(0);
        const resumed = yield* DurableStreamsClient.make({
          url: baseUrl + "/sse",
          offset: receipt.finalOffset,
          live: "sse",
        });
        expect(yield* resumed.bytes.pipe(Stream.runCollect)).toEqual(
          contentType === "application/json" ? [new TextEncoder().encode("[]")] : [],
        );
      }).pipe(Effect.provide(FetchHttpClient.layer)),
    );
  }
  it.effect("adapter stops at an SSE control batch without waiting for another event", () =>
    Effect.gen(function* () {
      const { baseUrl } = yield* acquireDurableStreamServer;
      const entered = yield* Deferred.make<void>();
      yield* handleCommand({ type: "init", serverUrl: baseUrl });
      yield* handleCommand({ type: "create", path: "/adapter", contentType: "application/json" });
      const run = yield* handleCommand({
        type: "read",
        path: "/adapter",
        live: "sse",
        maxChunks: 1,
      }).pipe(
        Effect.provideServiceEffect(
          HttpClient.HttpClient,
          Effect.map(HttpClient.HttpClient, (http) =>
            http.pipe(
              HttpClient.tap((response) =>
                response.request.url.includes("live=sse")
                  ? Deferred.succeed(entered, undefined)
                  : Effect.void,
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
      expect(yield* Fiber.join(run)).toMatchObject({
        type: "read",
        chunks: [{ data: "[[1,2]]", offset: receipt.offset }],
        streamClosed: false,
      });
    }).pipe(
      Effect.provide(
        Layer.merge(FetchHttpClient.layer, Layer.effect(AdapterState, AdapterState.make)),
      ),
    ),
  );
});
