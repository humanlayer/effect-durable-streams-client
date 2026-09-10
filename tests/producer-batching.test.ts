import { describe, expect, it } from "@effect/vitest";
import { Array as Arr, Deferred, Duration, Effect, Fiber, Option, Queue, Stream } from "effect";
import { TestClock } from "effect/testing";
import { DurableStreamsClient } from "../src/index.ts";
import { makeProducerHttp, producerReply } from "./support/producer-http.ts";

describe("producer batching", () => {
  it.effect(
    "checks threshold after insertion without counting JSON framing and completes shared receipts",
    () =>
      Effect.gen(function* () {
        const http = yield* makeProducerHttp;
        yield* Effect.gen(function* () {
          const client = yield* DurableStreamsClient.make({
            url: "http://localhost/json",
            contentType: "application/json",
          });
          const producer = yield* client.producer({
            producerId: "writer",
            maxBatchBytes: 4,
            linger: Duration.seconds(1),
          });
          const first = yield* producer.append({ value: 1 }).pipe(Effect.forkChild);
          yield* Queue.take(http.admitted);
          expect(yield* Queue.size(http.requests)).toBe(0);
          const second = yield* producer.append({ value: [2, 3] }).pipe(Effect.forkChild);
          const request = yield* Queue.take(http.requests);
          expect(new TextDecoder().decode(request.body)).toBe("[1,[2,3]]");
          expect(request.headers).toMatchObject({
            "producer-id": "writer",
            "producer-epoch": "0",
            "producer-seq": "0",
          });
          expect(first.pollUnsafe()).toBeUndefined();
          yield* Deferred.succeed(request.reply, producerReply({ seq: 0 }));
          expect(yield* Fiber.join(first)).toEqual(yield* Fiber.join(second));
          expect(request.signal.aborted).toBe(true);
          expect(yield* producer.nextSeq).toBe(1);
          yield* producer.flush;
        }).pipe(Effect.provide(http.layer));
      }),
  );

  it.effect("does not extend linger and gives the next batch a fresh full timer", () =>
    Effect.gen(function* () {
      const http = yield* makeProducerHttp;
      yield* Effect.gen(function* () {
        const client = yield* DurableStreamsClient.make({
          url: "http://localhost/text",
          contentType: "text/plain",
        });
        const producer = yield* client.producer({
          producerId: "writer",
          maxBatchBytes: 3,
          linger: Duration.millis(10),
        });
        const one = yield* producer.append({ value: "a" }).pipe(Effect.forkChild);
        yield* Queue.take(http.admitted);
        yield* TestClock.adjust("6 millis");
        const two = yield* producer.append({ value: "bc" }).pipe(Effect.forkChild);
        const first = yield* Queue.take(http.requests);
        yield* Deferred.succeed(first.reply, producerReply({ seq: 0 }));
        yield* Fiber.join(one);
        yield* Fiber.join(two);
        const three = yield* producer.append({ value: "d" }).pipe(Effect.forkChild);
        yield* Queue.take(http.admitted);
        yield* Queue.take(http.admitted);
        yield* TestClock.adjust("4 millis");
        expect(yield* Queue.size(http.requests)).toBe(0);
        const four = yield* producer.append({ value: "e" }).pipe(Effect.forkChild);
        yield* Queue.take(http.admitted);
        yield* TestClock.adjust("6 millis");
        const second = yield* Queue.take(http.requests);
        expect(new TextDecoder().decode(second.body)).toBe("de");
        yield* Deferred.succeed(second.reply, producerReply({ seq: 1 }));
        yield* Fiber.join(three);
        yield* Fiber.join(four);
      }).pipe(Effect.provide(http.layer));
    }),
  );

  it.effect("bounds workers and backpressures the Sink without regrouping binary entries", () =>
    Effect.gen(function* () {
      const http = yield* makeProducerHttp;
      yield* Effect.gen(function* () {
        const client = yield* DurableStreamsClient.make({ url: "http://localhost/bytes" });
        const producer = yield* client.producer({
          producerId: "writer",
          maxBatchBytes: 1,
          maxInFlight: 2,
        });
        const consumed = yield* Queue.unbounded<number>();
        const run = yield* Stream.range(0, 9).pipe(
          Stream.tap((value) => Queue.offer(consumed, value)),
          Stream.map((value) => new Uint8Array([value, 255])),
          Stream.run(producer.sink),
          Effect.forkChild,
        );
        const first = yield* Queue.take(http.requests);
        const second = yield* Queue.take(http.requests);
        yield* Effect.forEach(Arr.range(1, 5), () => Queue.take(consumed));
        expect(yield* Queue.size(http.requests)).toBe(0);
        expect(yield* Queue.size(consumed)).toBe(0);
        expect(first.body).toEqual(new Uint8Array([0, 255]));
        expect(second.body).toEqual(new Uint8Array([1, 255]));
        yield* Deferred.succeed(first.reply, producerReply({ seq: 0 }));
        yield* Deferred.succeed(second.reply, producerReply({ seq: 1, duplicate: true }));
        for (const seq of Arr.range(2, 9)) {
          const request = yield* Queue.take(http.requests);
          expect(request.headers["producer-seq"]).toBe(String(seq));
          expect(request.body).toEqual(new Uint8Array([seq, 255]));
          yield* Deferred.succeed(request.reply, producerReply({ seq }));
        }
        yield* Fiber.join(run);
        expect(yield* producer.lastSuccessfulOffset).toEqual(Option.some("offset9"));
      }).pipe(Effect.provide(http.layer));
    }),
  );

  it.effect("Sink end flushes a partial batch immediately", () =>
    Effect.gen(function* () {
      const http = yield* makeProducerHttp;
      yield* Effect.gen(function* () {
        const client = yield* DurableStreamsClient.make({
          url: "http://localhost/json",
          contentType: "application/json",
        });
        const producer = yield* client.producer({
          producerId: "writer",
          linger: Duration.hours(1),
        });
        const run = yield* Stream.make(1, 2, 3).pipe(Stream.run(producer.sink), Effect.forkChild);
        const request = yield* Queue.take(http.requests);
        expect(new TextDecoder().decode(request.body)).toBe("[1,2,3]");
        yield* Deferred.succeed(request.reply, producerReply({ seq: 0 }));
        yield* Fiber.join(run);
      }).pipe(Effect.provide(http.layer));
    }),
  );
});
