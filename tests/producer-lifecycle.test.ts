import { describe, expect, it } from "@effect/vitest";
import { Deferred, Duration, Effect, Exit, Fiber, Queue, Scope, Stream } from "effect";
import { TestClock } from "effect/testing";
import { HttpClient } from "effect/unstable/http";
import { DurableStreamsClient } from "../src/index.ts";
import { ScriptedResponse } from "./support/http-client.ts";
import { makeProducerHttp, producerReply } from "./support/producer-http.ts";

describe("producer lifecycle", () => {
  it.effect("interrupted restart leaves admission open without advancing the epoch", () =>
    Effect.gen(function* () {
      const http = yield* makeProducerHttp;
      yield* Effect.gen(function* () {
        const client = yield* DurableStreamsClient.make({ url: "http://localhost/text" });
        const producer = yield* client.producer({ producerId: "p", maxBatchBytes: 1 });
        const append = yield* producer.append({ value: "one" }).pipe(Effect.forkChild);
        const request = yield* Queue.take(http.requests);
        const restart = yield* producer.restart.pipe(Effect.forkChild);
        yield* Effect.yieldNow;
        expect((yield* producer.append({ value: "blocked" }).pipe(Effect.flip))._tag).toBe(
          "ProducerClosedError",
        );
        yield* Fiber.interrupt(restart);
        expect(yield* producer.epoch).toBe(0);
        yield* Deferred.succeed(request.reply, producerReply({ seq: 0 }));
        yield* Fiber.join(append);
        const next = yield* producer.append({ value: "two" }).pipe(Effect.forkChild);
        const second = yield* Queue.take(http.requests);
        expect(second.headers).toMatchObject({ "producer-epoch": "0", "producer-seq": "1" });
        yield* Deferred.succeed(second.reply, producerReply({ seq: 1 }));
        yield* Fiber.join(next);
      }).pipe(Effect.provide(http.layer));
    }),
  );

  it.effect("retains close payload when interrupted while draining pending appends", () =>
    Effect.gen(function* () {
      const http = yield* makeProducerHttp;
      yield* Effect.gen(function* () {
        const client = yield* DurableStreamsClient.make({ url: "http://localhost/text" });
        const producer = yield* client.producer({ producerId: "p", linger: Duration.hours(1) });
        const append = yield* producer.append({ value: "pending" }).pipe(Effect.forkChild);
        yield* Queue.take(http.admitted);
        const close = yield* producer.close({ value: "retained" }).pipe(Effect.forkChild);
        const request = yield* Queue.take(http.requests);
        expect(request.headers["stream-closed"]).toBeUndefined();
        yield* Fiber.interrupt(close);
        yield* Deferred.succeed(request.reply, producerReply({ seq: 0 }));
        yield* Fiber.join(append);
        const next = yield* producer.close({ value: "ignored" }).pipe(Effect.forkChild);
        const final = yield* Queue.take(http.requests);
        expect(new TextDecoder().decode(final.body)).toBe("retained");
        expect(final.headers).toMatchObject({ "stream-closed": "true", "producer-seq": "1" });
        yield* Deferred.succeed(final.reply, producerReply({ seq: 1, closed: true }));
        yield* Fiber.join(next);
      }).pipe(Effect.provide(http.layer));
    }),
  );
  it.effect("scope shutdown joins delayed HTTP cleanup before settling pending receipts", () =>
    Effect.gen(function* () {
      const http = yield* makeProducerHttp;
      yield* Effect.gen(function* () {
        const cleaning = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const scope = yield* Scope.make();
        const base = yield* HttpClient.HttpClient;
        const delayed = HttpClient.make((request) =>
          base
            .execute(request)
            .pipe(
              Effect.ensuring(
                Deferred.succeed(cleaning, undefined).pipe(Effect.andThen(Deferred.await(release))),
              ),
            ),
        );
        const client = yield* DurableStreamsClient.make({ url: "http://localhost/text" });
        const producer = yield* client
          .producer({ producerId: "p", maxBatchBytes: 2, linger: Duration.hours(1) })
          .pipe(
            Effect.provideService(Scope.Scope, scope),
            Effect.provideService(HttpClient.HttpClient, delayed),
          );
        const active = yield* producer.append({ value: "ab" }).pipe(Effect.forkChild);
        const request = yield* Queue.take(http.requests);
        const pending = yield* producer.append({ value: "c" }).pipe(Effect.forkChild);
        yield* Queue.take(http.admitted);
        yield* Queue.take(http.admitted);
        const shutdown = yield* Scope.close(scope, Exit.void).pipe(Effect.forkChild);
        yield* Deferred.await(cleaning);
        expect(shutdown.pollUnsafe()).toBeUndefined();
        expect(pending.pollUnsafe()).toBeUndefined();
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(shutdown);
        expect(Exit.hasInterrupts(yield* Fiber.await(active))).toBe(true);
        expect(Exit.hasInterrupts(yield* Fiber.await(pending))).toBe(true);
        expect(request.signal.aborted).toBe(true);
      }).pipe(Effect.provide(http.layer));
    }),
  );

  it.effect("remembers the claimed close tuple across a lost response", () =>
    Effect.gen(function* () {
      const http = yield* makeProducerHttp;
      yield* Effect.gen(function* () {
        const client = yield* DurableStreamsClient.make({
          url: "http://localhost/json",
          contentType: "application/json",
        });
        const producer = yield* client.producer({ producerId: "p", autoClaim: true });
        const first = yield* producer.close({ value: 1 }).pipe(Effect.forkChild);
        const stale = yield* Queue.take(http.requests);
        yield* Deferred.succeed(
          stale.reply,
          ScriptedResponse.Response({ status: 403, headers: { "producer-epoch": "4" } }),
        );
        const claimed = yield* Queue.take(http.requests);
        expect(claimed.headers).toMatchObject({ "producer-epoch": "5", "producer-seq": "0" });
        yield* Deferred.succeed(claimed.reply, ScriptedResponse.TransportFailure());
        expect((yield* Fiber.join(first).pipe(Effect.flip))._tag).toBe("StreamUnavailableError");
        const next = yield* producer.close({ value: 999 }).pipe(Effect.forkChild);
        const retried = yield* Queue.take(http.requests);
        expect(retried.body).toEqual(claimed.body);
        expect(retried.headers).toEqual(claimed.headers);
        yield* Deferred.succeed(
          retried.reply,
          producerReply({ seq: 0, epoch: 5, duplicate: true, closed: true }),
        );
        yield* Fiber.join(next);
      }).pipe(Effect.provide(http.layer));
    }),
  );

  it.effect("close cancellation releases its request and retries the same body", () =>
    Effect.gen(function* () {
      const http = yield* makeProducerHttp;
      yield* Effect.gen(function* () {
        const client = yield* DurableStreamsClient.make({ url: "http://localhost/text" });
        const producer = yield* client.producer({ producerId: "p" });
        const first = yield* producer.close({ value: "final" }).pipe(Effect.forkChild);
        const request = yield* Queue.take(http.requests);
        yield* Fiber.interrupt(first);
        expect(request.signal.aborted).toBe(true);
        const next = yield* producer.close({ value: "ignored" }).pipe(Effect.forkChild);
        const retry = yield* Queue.take(http.requests);
        expect(retry.body).toEqual(request.body);
        expect(retry.headers).toEqual(request.headers);
        yield* Deferred.succeed(
          retry.reply,
          producerReply({ seq: 0, duplicate: true, closed: true }),
        );
        yield* Fiber.join(next);
      }).pipe(Effect.provide(http.layer));
    }),
  );

  it.effect(
    "interrupting a receipt wait leaves admitted delivery owned by the producer scope",
    () =>
      Effect.gen(function* () {
        const http = yield* makeProducerHttp;
        yield* Effect.gen(function* () {
          const client = yield* DurableStreamsClient.make({ url: "http://localhost/text" });
          const producer = yield* client.producer({ producerId: "p", maxBatchBytes: 1 });
          const wait = yield* producer.append({ value: "kept" }).pipe(Effect.forkChild);
          const request = yield* Queue.take(http.requests);
          yield* Fiber.interrupt(wait);
          expect(request.signal.aborted).toBe(false);
          yield* Deferred.succeed(request.reply, producerReply({ seq: 0 }));
          yield* producer.flush;
          expect(request.signal.aborted).toBe(true);
          expect(yield* producer.nextSeq).toBe(1);
        }).pipe(Effect.provide(http.layer));
      }),
  );
  it.effect("flush waits only through its invocation watermark", () =>
    Effect.gen(function* () {
      const http = yield* makeProducerHttp;
      yield* Effect.gen(function* () {
        const client = yield* DurableStreamsClient.make({ url: "http://localhost/text" });
        const producer = yield* client.producer({ producerId: "p", linger: Duration.hours(1) });
        const first = yield* producer.append({ value: "a" }).pipe(Effect.forkChild);
        yield* Queue.take(http.admitted);
        const flush = yield* producer.flush.pipe(Effect.forkChild);
        const request = yield* Queue.take(http.requests);
        const second = yield* producer.append({ value: "b" }).pipe(Effect.forkChild);
        yield* Queue.take(http.admitted);
        yield* Deferred.succeed(request.reply, producerReply({ seq: 0 }));
        yield* Fiber.join(first);
        yield* Fiber.join(flush);
        expect(second.pollUnsafe()).toBeUndefined();
        const drain = yield* producer.detach.pipe(Effect.forkChild);
        const remaining = yield* Queue.take(http.requests);
        expect(remaining.headers["stream-closed"]).toBeUndefined();
        yield* Deferred.succeed(remaining.reply, producerReply({ seq: 1 }));
        yield* Fiber.join(second);
        yield* Fiber.join(drain);
        expect((yield* producer.append({ value: "after" }).pipe(Effect.flip))._tag).toBe(
          "ProducerClosedError",
        );
        yield* producer.detach;
      }).pipe(Effect.provide(http.layer));
    }),
  );

  it.effect(
    "restart resets epoch/sequence and close retries its immutable final tuple then caches success",
    () =>
      Effect.gen(function* () {
        const http = yield* makeProducerHttp;
        yield* Effect.gen(function* () {
          const client = yield* DurableStreamsClient.make({
            url: "http://localhost/json",
            contentType: "application/json",
          });
          const producer = yield* client.producer({ producerId: "p", maxBatchBytes: 1 });
          const append = yield* producer.append({ value: 1 }).pipe(Effect.forkChild);
          const first = yield* Queue.take(http.requests);
          yield* Deferred.succeed(first.reply, producerReply({ seq: 0 }));
          yield* Fiber.join(append);
          yield* producer.restart;
          expect(yield* producer.epoch).toBe(1);
          expect(yield* producer.nextSeq).toBe(0);
          const close = yield* producer.close({ value: [2, 3] }).pipe(Effect.forkChild);
          const failed = yield* Queue.take(http.requests);
          expect(new TextDecoder().decode(failed.body)).toBe("[[2,3]]");
          expect(failed.headers).toMatchObject({
            "producer-epoch": "1",
            "producer-seq": "0",
            "stream-closed": "true",
          });
          yield* Deferred.succeed(failed.reply, ScriptedResponse.TransportFailure());
          expect((yield* Fiber.join(close).pipe(Effect.flip))._tag).toBe("StreamUnavailableError");
          const retry = yield* producer.close({ value: 99 }).pipe(Effect.forkChild);
          const request = yield* Queue.take(http.requests);
          expect(request.body).toEqual(failed.body);
          expect(request.headers).toEqual(failed.headers);
          yield* Deferred.succeed(
            request.reply,
            producerReply({ seq: 0, epoch: 1, duplicate: true, closed: true }),
          );
          const result = yield* Fiber.join(retry);
          expect(yield* producer.close({})).toEqual(result);
          expect(yield* producer.nextSeq).toBe(1);
          expect(yield* Queue.size(http.requests)).toBe(0);
        }).pipe(Effect.provide(http.layer));
      }),
  );

  it.effect(
    "scope finalization interrupts active and pending receipts and never closes remotely",
    () =>
      Effect.gen(function* () {
        const http = yield* makeProducerHttp;
        yield* Effect.gen(function* () {
          const scope = yield* Scope.make();
          const client = yield* DurableStreamsClient.make({ url: "http://localhost/text" });
          const producer = yield* client
            .producer({ producerId: "p", maxBatchBytes: 2, linger: Duration.hours(1) })
            .pipe(Effect.provideService(Scope.Scope, scope));
          const active = yield* producer.append({ value: "ab" }).pipe(Effect.forkChild);
          const request = yield* Queue.take(http.requests);
          const pending = yield* producer.append({ value: "c" }).pipe(Effect.forkChild);
          yield* Queue.take(http.admitted);
          yield* Queue.take(http.admitted);
          yield* Scope.close(scope, Exit.void);
          expect(Exit.hasInterrupts(yield* Fiber.await(active))).toBe(true);
          expect(Exit.hasInterrupts(yield* Fiber.await(pending))).toBe(true);
          expect(request.signal.aborted).toBe(true);
          yield* TestClock.adjust("2 hours");
          expect(yield* Queue.size(http.requests)).toBe(0);
          expect(yield* producer.pendingCount).toBe(0);
        }).pipe(Effect.provide(http.layer));
      }),
  );

  it.effect("Sink exposes delivery failure rather than consuming an endless source", () =>
    Effect.gen(function* () {
      const http = yield* makeProducerHttp;
      yield* Effect.gen(function* () {
        const client = yield* DurableStreamsClient.make({ url: "http://localhost/text" });
        const producer = yield* client.producer({
          producerId: "p",
          maxBatchBytes: 1,
          maxInFlight: 1,
        });
        const run = yield* Stream.make("x").pipe(
          Stream.forever,
          Stream.run(producer.sink),
          Effect.forkChild,
        );
        const request = yield* Queue.take(http.requests);
        yield* Deferred.succeed(
          request.reply,
          ScriptedResponse.Response({ status: 413, headers: {} }),
        );
        const next = yield* Queue.take(http.requests);
        yield* Deferred.succeed(
          next.reply,
          ScriptedResponse.Response({ status: 413, headers: {} }),
        );
        expect((yield* Fiber.join(run).pipe(Effect.flip))._tag).toBe("PayloadTooLargeError");
      }).pipe(Effect.provide(http.layer));
    }),
  );
});
