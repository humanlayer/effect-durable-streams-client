import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Option, Queue, Random, Schema, Stream } from "effect";
import { HttpClientError, type HttpClient } from "effect/unstable/http";
import type { ReadError } from "../src/errors.ts";
import { TestClock } from "effect/testing";
import { DurableStreamsClient } from "../src/index.ts";
import { makeReadHttp, readReply, type ReadReply } from "./support/read-http.ts";

const _sse = (wire: string) =>
  ({
    status: 200,
    headers: { "content-type": "text/event-stream" },
    body: Stream.make(new TextEncoder().encode(wire)),
  }) satisfies ReadReply;

describe("SSE sessions", () => {
  for (const interrupt of [false, true]) {
    it.effect(`final JSON delivery precedes commit and joins cleanup: interrupt=${interrupt}`, () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        const observed = yield* Queue.unbounded<Schema.Json>();
        const resume = yield* Queue.unbounded<void>();
        const releasing = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const completed = yield* Deferred.make<void>();
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/s",
          live: "sse",
        });
        const run = yield* client.json.pipe(
          Stream.runForEach((value) =>
            Queue.offer(observed, value).pipe(Effect.andThen(Queue.take(resume))),
          ),
          Effect.onExit(() => Deferred.succeed(completed, undefined)),
          Effect.provide(http.layer),
          Effect.forkChild,
        );
        yield* Queue.take(http.requests);
        yield* Queue.offer(
          http.replies,
          readReply({
            offset: "safe",
            text: "[]",
            contentType: "application/json",
            upToDate: true,
          }),
        );
        const request = yield* Queue.take(http.requests);
        const reply = _sse(
          'event: data\ndata: [1,2]\n\nevent: control\ndata: {"streamNextOffset":"final","streamClosed":true}\n\n',
        );
        yield* Queue.offer(http.replies, {
          ...reply,
          body: reply.body.pipe(
            Stream.concat(Stream.never),
            Stream.ensuring(
              Deferred.succeed(releasing, undefined).pipe(Effect.andThen(Deferred.await(release))),
            ),
          ),
        });
        expect(yield* Queue.take(observed)).toBe(1);
        expect(yield* client.offset).toEqual(Option.some("safe"));
        yield* Queue.offer(resume, undefined);
        expect(yield* Queue.take(observed)).toBe(2);
        expect(yield* client.offset).toEqual(Option.some("safe"));
        expect(yield* Deferred.isDone(completed)).toBe(false);
        const stopping = yield* (
          interrupt ? Fiber.interrupt(run) : Queue.offer(resume, undefined)
        ).pipe(Effect.forkChild);
        yield* Deferred.await(releasing);
        expect(yield* Deferred.isDone(completed)).toBe(false);
        expect(yield* client.offset).toEqual(Option.some(interrupt ? "safe" : "final"));
        expect(yield* Queue.size(http.requests)).toBe(0);
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(stopping);
        const result = yield* Fiber.await(run);
        expect(interrupt ? Exit.hasInterrupts(result) : Exit.isSuccess(result)).toBe(true);
        expect(request.signal.aborted).toBe(true);
        expect(yield* Deferred.isDone(completed)).toBe(true);
        yield* TestClock.adjust("1 hour");
        expect(yield* Queue.size(http.requests)).toBe(0);
        expect(yield* Queue.size(observed)).toBe(0);
      }),
    );
  }
  for (const ending of ["broken", "truncated", "invalid"] as const) {
    it.effect(`distinguishes ${ending} UTF-8 framing from transport disconnect`, () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/s",
          live: "sse",
          sseResilience: { minConnectionDuration: 0 },
        });
        const run = yield* client.text.pipe(
          Stream.runCollect,
          Effect.exit,
          Effect.provide(http.layer),
          Effect.forkChild,
        );
        yield* Queue.take(http.requests);
        yield* Queue.offer(http.replies, readReply({ offset: "safe", text: "", upToDate: true }));
        const first = yield* Queue.take(http.requests);
        const body = Stream.make(
          new TextEncoder().encode("event: data\ndata: "),
          new Uint8Array(ending === "invalid" ? [0xe2, 0x28] : [0xe2]),
        );
        yield* Queue.offer(http.replies, {
          ..._sse(""),
          body:
            ending === "broken"
              ? body.pipe(
                  Stream.concat(
                    Stream.fail(
                      new HttpClientError.HttpClientError({
                        reason: new HttpClientError.TransportError({ request: first.request }),
                      }),
                    ),
                  ),
                )
              : body,
        });
        if (ending === "broken") {
          const next = yield* Queue.take(http.requests);
          expect(next.request.url).toBe("https://streams.test/s?live=sse&offset=safe");
          expect(yield* client.offset).toEqual(Option.some("safe"));
          expect(first.signal.aborted).toBe(true);
          yield* Queue.offer(
            http.replies,
            _sse(
              'event: data\ndata: € replay\n\nevent: control\ndata: {"streamNextOffset":"final","streamClosed":true}\n\n',
            ),
          );
          expect(yield* Fiber.join(run)).toEqual(Exit.succeed(["€ replay"]));
          expect(yield* client.offset).toEqual(Option.some("final"));
          expect(next.signal.aborted).toBe(true);
        } else {
          const result = yield* Fiber.join(run);
          expect(Exit.isFailure(result)).toBe(true);
          const failure = yield* result.pipe(Effect.flip);
          expect(failure).toHaveProperty("_tag", "ProtocolViolationError");
          expect(failure).toHaveProperty("component", "SSE UTF-8 framing");
          expect(yield* client.offset).toEqual(Option.some("safe"));
        }
        expect(first.signal.aborted).toBe(true);
        expect(yield* Queue.size(http.requests)).toBe(0);
      }),
    );
  }
  it.effect("reconnects after a broken SSE body and discards unacknowledged data", () =>
    Effect.gen(function* () {
      const http = yield* makeReadHttp;
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/s",
        live: "sse",
        sseResilience: { minConnectionDuration: 0 },
      });
      const run = yield* client.text.pipe(
        Stream.runCollect,
        Effect.provide(http.layer),
        Effect.forkChild,
      );
      yield* Queue.take(http.requests);
      yield* Queue.offer(http.replies, readReply({ offset: "safe", text: "", upToDate: true }));
      const first = yield* Queue.take(http.requests);
      const reply = _sse("event: data\ndata: replay\n\n");
      yield* Queue.offer(http.replies, {
        ...reply,
        body: reply.body.pipe(
          Stream.concat(
            Stream.fail(
              new HttpClientError.HttpClientError({
                reason: new HttpClientError.TransportError({ request: first.request }),
              }),
            ),
          ),
        ),
      });
      const next = yield* Queue.take(http.requests);
      expect(first.signal.aborted).toBe(true);
      expect(next.request.url).toBe(first.request.url);
      yield* Queue.offer(
        http.replies,
        _sse(
          'event: data\ndata: replay\n\nevent: control\ndata: {"streamNextOffset":"final","streamClosed":true}\n\n',
        ),
      );
      expect(yield* Fiber.join(run)).toEqual(["replay"]);
      expect(yield* client.offset).toEqual(Option.some("final"));
      expect(next.signal.aborted).toBe(true);
      expect(yield* Queue.size(http.requests)).toBe(0);
    }),
  );
  it.effect(
    "resets short-connection jitter after healthy activity and cancels a reconnect wait",
    () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/s",
          live: "sse",
        });
        const run = yield* client.text.pipe(
          Stream.runDrain,
          Effect.provide(http.layer),
          Effect.forkChild,
        );
        yield* Queue.take(http.requests);
        yield* Queue.offer(http.replies, readReply({ offset: "safe", text: "", upToDate: true }));
        const first = yield* Queue.take(http.requests);
        yield* Queue.offer(http.replies, _sse(""));
        yield* TestClock.adjust("99 millis");
        expect(first.signal.aborted).toBe(true);
        expect(yield* Queue.size(http.requests)).toBe(0);
        yield* TestClock.adjust("1 millis");
        yield* Queue.take(http.requests);
        const release = yield* Deferred.make<void>();
        yield* Queue.offer(http.replies, {
          ..._sse(""),
          body: Stream.fromEffectDrain(Deferred.await(release)),
        });
        yield* TestClock.adjust("1 second");
        yield* Deferred.succeed(release, undefined);
        yield* Queue.take(http.requests);
        yield* Queue.offer(http.replies, _sse(""));
        yield* TestClock.adjust("99 millis");
        expect(yield* Queue.size(http.requests)).toBe(0);
        yield* TestClock.adjust("1 millis");
        const last = yield* Queue.take(http.requests);
        expect(last.request.url).toContain("live=sse");
        yield* Queue.offer(http.replies, _sse(""));
        yield* TestClock.adjust("1 millis");
        yield* Fiber.interrupt(run);
        expect(Exit.hasInterrupts(yield* Fiber.await(run))).toBe(true);
        expect(last.signal.aborted).toBe(true);
        expect(yield* client.offset).toEqual(Option.some("safe"));
        yield* TestClock.adjust("1 hour");
        expect(yield* Queue.size(http.requests)).toBe(0);
      }).pipe(
        Effect.provideService(Random.Random, {
          nextDoubleUnsafe: () => 0.5,
          nextIntUnsafe: () => 0,
        }),
      ),
  );

  for (const malformed of ["headers", "control", "payload"]) {
    it.effect(`releases SSE on invalid ${malformed} without advancing or reconnecting`, () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/s",
          live: "sse",
        });
        const run = yield* client.json.pipe(
          Stream.runDrain,
          Effect.flip,
          Effect.provide(http.layer),
          Effect.forkChild,
        );
        yield* Queue.take(http.requests);
        yield* Queue.offer(
          http.replies,
          readReply({
            offset: "safe",
            text: "[]",
            contentType: "application/json",
            upToDate: true,
          }),
        );
        const request = yield* Queue.take(http.requests);
        const reply = _sse(
          malformed === "control"
            ? "event: control\ndata: {}\n\n"
            : 'event: data\ndata: invalid\n\nevent: control\ndata: {"streamNextOffset":"unsafe","streamClosed":true}\n\n',
        );
        yield* Queue.offer(http.replies, {
          ...reply,
          headers: malformed === "headers" ? { "content-type": "text/plain" } : reply.headers,
          body: reply.body.pipe(Stream.concat(Stream.never)),
        });
        expect(yield* Fiber.join(run)).toHaveProperty(
          "_tag",
          malformed === "payload" ? "PayloadDecodeError" : "ProtocolViolationError",
        );
        expect(request.signal.aborted).toBe(true);
        expect(yield* client.offset).toEqual(Option.some("safe"));
        expect(yield* Queue.size(http.requests)).toBe(0);
      }),
    );
  }
  it.effect(
    "buffers until control, delivers before commit, drops uncontrolled data and reconnects from exact checkpoints",
    () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        const delivered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/s",
          live: "sse",
          sseResilience: { minConnectionDuration: 0 },
        });
        const run = yield* client.text.pipe(
          Stream.tap(() =>
            Deferred.succeed(delivered, undefined).pipe(Effect.andThen(Deferred.await(release))),
          ),
          Stream.runCollect,
          Effect.provide(http.layer),
          Effect.forkChild,
        );
        expect((yield* Queue.take(http.requests)).request.url).toBe(
          "https://streams.test/s?offset=-1",
        );
        const catchup = readReply({ offset: "safe", text: "", upToDate: true });
        yield* Queue.offer(http.replies, {
          ...catchup,
          headers: { ...catchup.headers, "stream-cursor": "old +/?" },
        });
        const first = yield* Queue.take(http.requests);
        expect(first.request.url).toBe(
          "https://streams.test/s?cursor=old+%2B%2F%3F&live=sse&offset=safe",
        );
        yield* Queue.offer(http.replies, _sse("event: data\ndata: replay\n\n"));
        const second = yield* Queue.take(http.requests);
        expect(second.request.url).toBe(first.request.url);
        expect(first.signal.aborted).toBe(true);
        expect(yield* Deferred.isDone(delivered)).toBe(false);
        yield* Queue.offer(
          http.replies,
          _sse(
            'event: data\ndata: replay\n\nevent: control\ndata: {"streamNextOffset":"new+offset","streamCursor":"c +/?"}\n\n',
          ),
        );
        yield* Deferred.await(delivered);
        expect(yield* client.offset).toEqual(Option.some("safe"));
        yield* Deferred.succeed(release, undefined);
        const third = yield* Queue.take(http.requests);
        expect(third.request.url).toBe(
          "https://streams.test/s?cursor=c+%2B%2F%3F&live=sse&offset=new%2Boffset",
        );
        expect(yield* client.offset).toEqual(Option.some("new+offset"));
        expect(second.signal.aborted).toBe(true);
        yield* Queue.offer(http.replies, {
          ..._sse(
            'event: data\ndata: final\n\nevent: control\ndata: {"streamNextOffset":"final","streamClosed":true}\n\n',
          ),
          body: _sse(
            'event: data\ndata: final\n\nevent: control\ndata: {"streamNextOffset":"final","streamClosed":true}\n\n',
          ).body.pipe(Stream.concat(Stream.never)),
        });
        expect(yield* Fiber.join(run)).toEqual(["replay", "final"]);
        expect(yield* client.offset).toEqual(Option.some("final"));
        expect(third.signal.aborted).toBe(true);
        expect(yield* Queue.size(http.requests)).toBe(0);
      }),
  );
  it.effect("backs off short connections then actually polls and never returns to SSE", () =>
    Effect.gen(function* () {
      const http = yield* makeReadHttp;
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/s",
        live: "sse",
      });
      const run = yield* client.text.pipe(
        Stream.runCollect,
        Effect.provide(http.layer),
        Effect.forkChild,
      );
      yield* Queue.take(http.requests);
      yield* Queue.offer(http.replies, readReply({ offset: "safe", text: "", upToDate: true }));
      for (const cap of [200, 400, 0]) {
        const request = yield* Queue.take(http.requests);
        expect(new URL(request.request.url).searchParams.get("live")).toBe("sse");
        yield* Queue.offer(http.replies, _sse(""));
        yield* TestClock.adjust(`${cap} millis`);
        expect(request.signal.aborted).toBe(true);
      }
      const poll = yield* Queue.take(http.requests);
      expect(new URL(poll.request.url).searchParams.get("live")).toBe("long-poll");
      yield* Queue.offer(http.replies, readReply({ offset: "final", text: "last", closed: true }));
      expect(yield* Fiber.join(run)).toEqual(["last"]);
      expect(yield* client.offset).toEqual(Option.some("final"));
    }),
  );
  for (const json of [false, true]) {
    it.effect(`decodes independent events before one closed control json=${json}`, () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/s",
          live: "sse",
        });
        const collect: Effect.Effect<
          ReadonlyArray<Schema.Json | Uint8Array>,
          ReadError,
          HttpClient.HttpClient
        > = json ? Stream.runCollect(client.json) : Stream.runCollect(client.bytes);
        const run = yield* collect.pipe(Effect.provide(http.layer), Effect.forkChild);
        yield* Queue.take(http.requests);
        yield* Queue.offer(
          http.replies,
          readReply({
            offset: "safe",
            text: json ? "[]" : "",
            contentType: json ? "application/json" : "application/octet-stream",
            upToDate: true,
          }),
        );
        yield* Queue.take(http.requests);
        const reply = _sse(
          `event: data\ndata: ${json ? "[1,[2,3]]" : "AQ=="}\n\nevent: data\ndata: ${json ? "[null,false]" : "AgM="}\n\nevent: control\ndata: {"streamNextOffset":"final","streamClosed":true}\n\n`,
        );
        yield* Queue.offer(http.replies, {
          ...reply,
          headers: json
            ? reply.headers
            : { ...reply.headers, "stream-sse-data-encoding": "base64" },
        });
        expect(yield* Fiber.join(run)).toEqual(
          json ? [1, [2, 3], null, false] : [new Uint8Array([1]), new Uint8Array([2, 3])],
        );
        expect(yield* client.offset).toEqual(Option.some("final"));
      }),
    );
  }
});
