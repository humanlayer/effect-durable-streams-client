import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Fiber, Option, Queue, Stream } from "effect";
import { HttpClient } from "effect/unstable/http";
import { TestClock } from "effect/testing";
import { DurableStreamsClient } from "../src/index";
import { makeReadHttp, readReply } from "./support/read-http";
import { makeScriptedHttpClient, ScriptedResponse } from "./support/http-client";

describe("finite catch-up ownership and progression", () => {
  it.effect(
    "keeps unlimited GET retries and reevaluates the ambient HTTP transform on every attempt",
    () =>
      Effect.gen(function* () {
        const http = yield* makeScriptedHttpClient;
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/data",
          offset: "opaque",
          backoffOptions: { initialDelay: 1, multiplier: 1, maxDelay: 1 },
        });
        const transforms = { count: 0 };
        const run = yield* client.bytes.pipe(
          Stream.runDrain,
          Effect.provideServiceEffect(
            HttpClient.HttpClient,
            Effect.map(HttpClient.HttpClient, (http) =>
              http.pipe(
                HttpClient.mapRequestEffect((request) =>
                  Effect.sync(() => {
                    transforms.count++;
                    return request;
                  }),
                ),
              ),
            ),
          ),
          Effect.provide(http.layer),
          Effect.forkChild,
        );
        for (const attempt of Array.from({ length: 9 }, (_, index) => index + 1)) {
          const request = yield* Queue.take(http.requests);
          expect(new URL(request.url).searchParams.get("offset")).toBe("opaque");
          expect(transforms.count).toBe(attempt);
          yield* http.respond(ScriptedResponse.TransportFailure());
          yield* TestClock.adjust("1 millis");
        }
        yield* Queue.take(http.requests);
        yield* http.respond(
          ScriptedResponse.Response({
            status: 200,
            headers: {
              "content-type": "text/plain",
              "stream-next-offset": "tail",
              "stream-up-to-date": "true",
            },
          }),
        );
        yield* Fiber.join(run);
        expect(transforms.count).toBe(10);
        expect(yield* client.offset).toEqual(Option.some("tail"));
      }),
  );

  it.effect("exhausts explicit finite transport retries without claiming consumption success", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/data",
        backoffOptions: { maxRetries: 1, initialDelay: 1 },
      });
      yield* http.respond(ScriptedResponse.TransportFailure());
      yield* http.respond(ScriptedResponse.TransportFailure());
      const run = yield* client.bytes.pipe(
        Stream.runDrain,
        Effect.flip,
        Effect.provide(http.layer),
        Effect.forkChild,
      );
      yield* Queue.take(http.requests);
      yield* TestClock.adjust("1 millis");
      expect(yield* Fiber.join(run)).toHaveProperty("_tag", "StreamUnavailableError");
      yield* Queue.take(http.requests);
      expect(yield* Queue.size(http.requests)).toBe(0);
      expect(yield* client.offset).toEqual(Option.none());
    }),
  );
  it.effect(
    "echoes opaque offsets and cursors, sorts query keys and closes each page before the next",
    () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/data?z=last",
          params: { a: "first" },
          headers: { "x-extension": "yes" },
        });
        const run = yield* client.text.pipe(
          Stream.runCollect,
          Effect.provide(http.layer),
          Effect.forkChild,
        );
        const first = yield* Queue.take(http.requests);
        expect(first.request.url).toBe("https://streams.test/data?a=first&offset=-1&z=last");
        expect(first.request.headers["x-extension"]).toBe("yes");
        yield* Queue.offer(http.replies, {
          ...readReply({ offset: "Opaque+:%#", text: "one" }),
          headers: {
            "content-type": "text/plain",
            "stream-next-offset": "Opaque+:%#",
            "stream-cursor": "cursor +/?",
          },
        });
        const second = yield* Queue.take(http.requests);
        expect(first.signal.aborted).toBe(true);
        expect(yield* client.offset).toEqual(Option.some("Opaque+:%#"));
        expect(second.request.url).toBe(
          "https://streams.test/data?a=first&cursor=cursor+%2B%2F%3F&offset=Opaque%2B%3A%25%23&z=last",
        );
        yield* Queue.offer(http.replies, readReply({ offset: "empty", text: "" }));
        const third = yield* Queue.take(http.requests);
        expect(second.signal.aborted).toBe(true);
        expect(new URL(third.request.url).searchParams.get("offset")).toBe("empty");
        yield* Queue.offer(http.replies, readReply({ offset: "final", text: "two", closed: true }));
        expect(yield* Fiber.join(run)).toEqual(["one", "two"]);
        expect(yield* client.offset).toEqual(Option.some("final"));
        expect(third.signal.aborted).toBe(true);
        expect(yield* Queue.size(http.requests)).toBe(0);
      }),
  );

  it.effect("supports beginning, now and saved offsets with empty up-to-date completion", () =>
    Effect.gen(function* () {
      const http = yield* makeReadHttp;
      for (const offset of ["-1", "now", "saved+Case"]) {
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/data",
          offset,
        });
        yield* Queue.offer(http.replies, readReply({ offset: "tail", text: "", upToDate: true }));
        expect(yield* client.bytes.pipe(Stream.runCollect, Effect.provide(http.layer))).toEqual([]);
        expect(
          new URL((yield* Queue.take(http.requests)).request.url).searchParams.get("offset"),
        ).toBe(offset);
        expect(yield* client.offset).toEqual(Option.some("tail"));
      }
    }),
  );

  it.effect("acquires consumption on execution and rejects concurrent and subsequent views", () =>
    Effect.gen(function* () {
      const http = yield* makeReadHttp;
      const client = yield* DurableStreamsClient.make({ url: "https://streams.test/data" });
      const untouched = client.json;
      expect(untouched).toBe(client.json);
      const owner = yield* client.bytes.pipe(
        Stream.runDrain,
        Effect.provide(http.layer),
        Effect.forkChild,
      );
      const request = yield* Queue.take(http.requests);
      expect(
        yield* client.text.pipe(Stream.runDrain, Effect.flip, Effect.provide(http.layer)),
      ).toHaveProperty("_tag", "AlreadyConsumedError");
      yield* Fiber.interrupt(owner);
      expect(request.signal.aborted).toBe(true);
      expect(
        yield* client.bytes.pipe(Stream.runDrain, Effect.flip, Effect.provide(http.layer)),
      ).toHaveProperty("_tag", "AlreadyConsumedError");
      expect(yield* client.offset).toEqual(Option.none());
      expect(yield* Queue.size(http.requests)).toBe(0);
    }),
  );

  it.effect(
    "interruption during a partial body releases resources without committing its boundary",
    () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        const delivered = yield* Deferred.make<void>();
        const released = yield* Deferred.make<void>();
        const client = yield* DurableStreamsClient.make({ url: "https://streams.test/data" });
        yield* Queue.offer(http.replies, readReply({ offset: "committed", text: "first" }));
        yield* Queue.offer(http.replies, {
          ...readReply({ offset: "uncommitted", text: "", upToDate: true }),
          body: Stream.make(new TextEncoder().encode("partial")).pipe(
            Stream.concat(Stream.never),
            Stream.ensuring(Deferred.succeed(released, undefined)),
          ),
        });
        const run = yield* client.text.pipe(
          Stream.runForEach((text) =>
            text === "partial" ? Deferred.succeed(delivered, undefined) : Effect.void,
          ),
          Effect.provide(http.layer),
          Effect.forkChild,
        );
        yield* Deferred.await(delivered);
        expect(yield* client.offset).toEqual(Option.some("committed"));
        const first = yield* Queue.take(http.requests);
        const second = yield* Queue.take(http.requests);
        yield* Fiber.interrupt(run);
        expect(yield* Deferred.isDone(released)).toBe(true);
        expect(first.signal.aborted && second.signal.aborted).toBe(true);
        expect(yield* client.offset).toEqual(Option.some("committed"));
      }),
  );

  it.effect("early downstream completion cannot advance past undelivered JSON items", () =>
    Effect.gen(function* () {
      const http = yield* makeReadHttp;
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/data",
        offset: "saved",
      });
      yield* Queue.offer(
        http.replies,
        readReply({
          offset: "tail",
          text: "[1,2,3]",
          contentType: "application/json",
          upToDate: true,
        }),
      );
      expect(
        yield* client.json.pipe(Stream.take(1), Stream.runCollect, Effect.provide(http.layer)),
      ).toEqual([1]);
      expect(yield* client.offset).toEqual(Option.none());
      expect((yield* Queue.take(http.requests)).signal.aborted).toBe(true);
    }),
  );

  it.effect("SSE configuration stays cold and closed catch-up never enters SSE", () =>
    Effect.gen(function* () {
      const http = yield* makeReadHttp;
      for (const live of ["sse"] as const) {
        const client = yield* DurableStreamsClient.make({ url: "https://streams.test/data", live });
        expect(yield* Queue.size(http.requests)).toBe(0);
        yield* Queue.offer(http.replies, readReply({ offset: "final", text: "", closed: true }));
        yield* client.bytes.pipe(Stream.runDrain, Effect.provide(http.layer));
        expect((yield* Queue.take(http.requests)).request.url).toBe(
          "https://streams.test/data?offset=-1",
        );
        expect(yield* client.offset).toEqual(Option.some("final"));
      }
      expect(yield* Queue.size(http.requests)).toBe(0);
    }),
  );

  it.effect("retries safe GETs with Retry-After and cancels unlimited waiting", () =>
    Effect.gen(function* () {
      const http = yield* makeReadHttp;
      const client = yield* DurableStreamsClient.make({ url: "https://streams.test/data" });
      const run = yield* client.bytes.pipe(
        Stream.runDrain,
        Effect.provide(http.layer),
        Effect.forkChild,
      );
      const first = yield* Queue.take(http.requests);
      yield* Queue.offer(http.replies, {
        status: 429,
        headers: { "retry-after": "2" },
        body: Stream.empty,
      });
      yield* TestClock.adjust("1 second");
      expect(yield* Queue.size(http.requests)).toBe(0);
      expect(first.signal.aborted).toBe(true);
      yield* TestClock.adjust("1 second");
      const retry = yield* Queue.take(http.requests);
      expect(retry.request.url).toBe(first.request.url);
      yield* Queue.offer(http.replies, {
        status: 503,
        headers: { "retry-after": "3600" },
        body: Stream.empty,
      });
      yield* TestClock.adjust("1 second");
      yield* Fiber.interrupt(run);
      expect(retry.signal.aborted).toBe(true);
      expect(yield* client.offset).toEqual(Option.none());
      yield* TestClock.adjust("2 hours");
      expect(yield* Queue.size(http.requests)).toBe(0);
    }),
  );
});
