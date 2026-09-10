import { describe, expect, it } from "@effect/vitest";
import { Array as Arr, Deferred, Effect, Fiber, Queue, Ref, Stream } from "effect";
import { HttpClient, HttpClientRequest } from "effect/unstable/http";
import { DurableStreamsClient } from "../src/index";
import { ScriptedResponse } from "./support/http-client";
import { makeProducerHttp, producerReply } from "./support/producer-http";

describe("producer protocol recovery", () => {
  it.effect(
    "captures ambient filtered HTTP at acquisition and refreshes transforms on recovery",
    () =>
      Effect.gen(function* () {
        const http = yield* makeProducerHttp;
        const other = yield* makeProducerHttp;
        const attempt = yield* Ref.make(0);
        const client = yield* DurableStreamsClient.make({
          url: "http://localhost/text",
          params: { tenant: "a b" },
          headers: { "x-static": "kept" },
        });
        const producer = yield* Effect.gen(function* () {
          const base = yield* HttpClient.HttpClient;
          const transport = base.pipe(
            HttpClient.mapRequestEffect((request) =>
              Ref.updateAndGet(attempt, (n) => n + 1).pipe(
                Effect.map((n) =>
                  request.pipe(HttpClientRequest.setHeader("authorization", `Bearer ${n}`)),
                ),
              ),
            ),
            HttpClient.filterStatusOk,
          );
          return yield* client
            .producer({ producerId: "p", maxBatchBytes: 1, autoClaim: true })
            .pipe(Effect.provideService(HttpClient.HttpClient, transport));
        }).pipe(Effect.provide(http.layer));
        const run = yield* producer
          .append({ value: "unchanged" })
          .pipe(Effect.provide(other.layer), Effect.forkChild);
        const first = yield* Queue.take(http.requests);
        expect(first.headers).toMatchObject({ "x-static": "kept", authorization: "Bearer 1" });
        expect(first.url).toContain("tenant=a+b");
        yield* Deferred.succeed(
          first.reply,
          ScriptedResponse.Response({ status: 403, headers: { "producer-epoch": "3" } }),
        );
        const retry = yield* Queue.take(http.requests);
        expect(retry.headers).toMatchObject({
          "x-static": "kept",
          authorization: "Bearer 2",
          "producer-epoch": "4",
          "producer-seq": "0",
        });
        expect(retry.body).toEqual(first.body);
        expect(first.signal.aborted).toBe(true);
        yield* Deferred.succeed(retry.reply, producerReply({ seq: 0, epoch: 4 }));
        yield* Fiber.join(run);
        expect(yield* Queue.size(other.requests)).toBe(0);
      }),
  );

  it.effect("accepts duplicate 204 without an offset and reports the server's last sequence", () =>
    Effect.gen(function* () {
      const http = yield* makeProducerHttp;
      yield* Effect.gen(function* () {
        const client = yield* DurableStreamsClient.make({ url: "http://localhost/text" });
        const producer = yield* client.producer({ producerId: "p", maxBatchBytes: 1 });
        const run = yield* producer.append({ value: "duplicate" }).pipe(Effect.forkChild);
        const request = yield* Queue.take(http.requests);
        yield* Deferred.succeed(
          request.reply,
          ScriptedResponse.Response({
            status: 204,
            headers: { "producer-epoch": "0", "producer-seq": "4" },
          }),
        );
        expect(yield* Fiber.join(run)).toEqual({ duplicate: true, producerSeq: 4 });
        expect(yield* producer.nextSeq).toBe(1);
      }).pipe(Effect.provide(http.layer));
    }),
  );
  for (const earlierFails of [false, true])
    it.effect(`waits for reordered local gaps; earlier failure=${earlierFails}`, () =>
      Effect.gen(function* () {
        const http = yield* makeProducerHttp;
        yield* Effect.gen(function* () {
          const client = yield* DurableStreamsClient.make({
            url: "http://localhost/text",
            contentType: "text/plain",
          });
          const producer = yield* client.producer({ producerId: "p", maxBatchBytes: 1 });
          const one = yield* producer.append({ value: "first" }).pipe(Effect.forkChild);
          const first = yield* Queue.take(http.requests);
          const two = yield* producer.append({ value: "second" }).pipe(Effect.forkChild);
          const second = yield* Queue.take(http.requests);
          yield* Deferred.succeed(
            second.reply,
            ScriptedResponse.Response({
              status: 409,
              headers: { "producer-expected-seq": "0", "producer-received-seq": "1" },
            }),
          );
          yield* Deferred.succeed(
            first.reply,
            earlierFails
              ? ScriptedResponse.Response({ status: 413, headers: {} })
              : producerReply({ seq: 0 }),
          );
          if (earlierFails) {
            const failure = yield* Fiber.join(one).pipe(Effect.flip);
            expect(failure._tag).toBe("PayloadTooLargeError");
            expect(yield* Fiber.join(two).pipe(Effect.flip)).toBe(failure);
            expect(yield* producer.flush.pipe(Effect.flip)).toBe(failure);
            expect(yield* Queue.size(http.requests)).toBe(0);
          } else {
            yield* Fiber.join(one);
            const retried = yield* Queue.take(http.requests);
            expect(retried.body).toEqual(second.body);
            expect(retried.headers).toEqual(second.headers);
            expect(second.signal.aborted).toBe(true);
            yield* Deferred.succeed(retried.reply, producerReply({ seq: 1 }));
            expect(yield* Fiber.join(two)).toMatchObject({ producerSeq: 1, duplicate: false });
          }
        }).pipe(Effect.provide(http.layer));
      }),
    );

  it.effect("holds later auto-claim batches without reserving sequences", () =>
    Effect.gen(function* () {
      const http = yield* makeProducerHttp;
      yield* Effect.gen(function* () {
        const client = yield* DurableStreamsClient.make({
          url: "http://localhost/text",
          contentType: "text/plain",
        });
        const producer = yield* client.producer({
          producerId: "p",
          maxBatchBytes: 1,
          autoClaim: true,
          maxInFlight: 3,
        });
        const run = yield* Stream.range(0, 9).pipe(
          Stream.map(String),
          Stream.run(producer.sink),
          Effect.forkChild,
        );
        const first = yield* Queue.take(http.requests);
        yield* Effect.forEach(Arr.range(1, 6), () => Queue.take(http.admitted));
        expect(yield* producer.nextSeq).toBe(1);
        expect(yield* producer.inFlightCount).toBe(1);
        expect(yield* Queue.size(http.requests)).toBe(0);
        yield* Deferred.succeed(
          first.reply,
          ScriptedResponse.Response({ status: 403, headers: { "producer-epoch": "7" } }),
        );
        const claim = yield* Queue.take(http.requests);
        expect(claim.body).toEqual(first.body);
        expect(claim.headers).toMatchObject({ "producer-epoch": "8", "producer-seq": "0" });
        expect(yield* producer.nextSeq).toBe(1);
        yield* Deferred.succeed(claim.reply, producerReply({ seq: 0, epoch: 8 }));
        for (const seq of Arr.range(1, 9)) {
          const request = yield* Queue.take(http.requests);
          expect(request.headers).toMatchObject({
            "producer-epoch": "8",
            "producer-seq": String(seq),
          });
          expect(new TextDecoder().decode(request.body)).toBe(String(seq));
          yield* Deferred.succeed(request.reply, producerReply({ seq, epoch: 8 }));
        }
        yield* Fiber.join(run);
        expect(yield* producer.epoch).toBe(8);
      }).pipe(Effect.provide(http.layer));
    }),
  );

  const scenarios: ReadonlyArray<{
    readonly status: number;
    readonly headers: Readonly<Record<string, string>>;
    readonly tag: string;
  }> = [
    { status: 403, headers: { "producer-epoch": "4" }, tag: "ProducerFencedError" },
    { status: 403, headers: { "producer-epoch": "4junk" }, tag: "ForbiddenError" },
    { status: 403, headers: {}, tag: "ForbiddenError" },
    {
      status: 409,
      headers: { "producer-expected-seq": "2", "producer-received-seq": "0" },
      tag: "ProducerSequenceGapError",
    },
    { status: 409, headers: {}, tag: "ProtocolViolationError" },
    {
      status: 409,
      headers: { "stream-closed": "true", "stream-next-offset": "tail" },
      tag: "StreamClosedError",
    },
    { status: 200, headers: {}, tag: "ProtocolViolationError" },
    { status: 503, headers: {}, tag: "StreamUnavailableError" },
    { status: 429, headers: { "retry-after": "10" }, tag: "RateLimitedError" },
  ];
  for (const scenario of scenarios)
    it.effect(`classifies producer ${scenario.status} as ${scenario.tag}`, () =>
      Effect.gen(function* () {
        const http = yield* makeProducerHttp;
        yield* Effect.gen(function* () {
          const client = yield* DurableStreamsClient.make({ url: "http://localhost/text" });
          const producer = yield* client.producer({ producerId: "p", maxBatchBytes: 1 });
          const append = yield* producer.append({ value: "a" }).pipe(Effect.forkChild);
          const request = yield* Queue.take(http.requests);
          yield* Deferred.succeed(
            request.reply,
            ScriptedResponse.Response({
              status: scenario.status,
              headers: scenario.headers,
            }),
          );
          const failure = yield* Fiber.join(append).pipe(Effect.flip);
          expect(failure._tag).toBe(scenario.tag);
          expect(yield* producer.flush.pipe(Effect.flip)).toBe(failure);
          expect(request.signal.aborted).toBe(true);
          expect(yield* Queue.size(http.requests)).toBe(0);
        }).pipe(Effect.provide(http.layer));
      }),
    );

  it.effect("does not retry transport failures even with ordinary unlimited defaults", () =>
    Effect.gen(function* () {
      const http = yield* makeProducerHttp;
      yield* Effect.gen(function* () {
        const client = yield* DurableStreamsClient.make({ url: "http://localhost/text" });
        const producer = yield* client.producer({ producerId: "p", maxBatchBytes: 1 });
        const append = yield* producer.append({ value: "a" }).pipe(Effect.forkChild);
        const request = yield* Queue.take(http.requests);
        yield* Deferred.succeed(request.reply, ScriptedResponse.TransportFailure());
        expect((yield* Fiber.join(append).pipe(Effect.flip))._tag).toBe("StreamUnavailableError");
        expect(yield* Queue.size(http.requests)).toBe(0);
      }).pipe(Effect.provide(http.layer));
    }),
  );
});
