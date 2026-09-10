import { describe, expect, it } from "@effect/vitest";
import { Effect, Queue, Stream } from "effect";
import { FetchHttpClient, HttpClient } from "effect/unstable/http";
import { DurableStreamsClient } from "../../src/index";
import { acquireDurableStreamServer } from "../support/server";

describe("reference server producer", () => {
  it.effect(
    "accepts, deduplicates, fences, auto-claims, pipelines and closes with ordered readback",
    () =>
      Effect.gen(function* () {
        const { baseUrl } = yield* acquireDurableStreamServer;
        const client = yield* DurableStreamsClient.make({
          url: baseUrl + "/producer",
          contentType: "application/json",
        });
        yield* client.create({});
        const first = yield* client.producer({ producerId: "p", maxBatchBytes: 1 });
        expect(yield* first.append({ value: 1 })).toMatchObject({
          duplicate: false,
          producerSeq: 0,
        });
        const duplicate = yield* client.producer({ producerId: "p", maxBatchBytes: 1 });
        expect(yield* duplicate.append({ value: 999 })).toMatchObject({
          duplicate: true,
          producerSeq: 0,
        });
        const newer = yield* client.producer({ producerId: "p", epoch: 2, maxBatchBytes: 1 });
        yield* newer.append({ value: 2 });
        expect((yield* first.append({ value: 888 }).pipe(Effect.flip))._tag).toBe(
          "ProducerFencedError",
        );
        const claim = yield* client.producer({
          producerId: "p",
          autoClaim: true,
          maxBatchBytes: 1,
          maxInFlight: 5,
        });
        yield* Stream.range(3, 22).pipe(Stream.run(claim.sink));
        expect(yield* claim.epoch).toBe(3);
        const closed = yield* claim.close({ value: [23, 24] });
        expect(yield* claim.close({ value: 999 })).toEqual(closed);
        expect(yield* client.head).toMatchObject({ closed: true, offset: closed.finalOffset });
        expect(yield* client.json.pipe(Stream.runCollect)).toEqual([
          1,
          2,
          ...Array.from({ length: 20 }, (_, i) => i + 3),
          [23, 24],
        ]);
      }).pipe(Effect.provide(FetchHttpClient.layer)),
  );

  it.effect("retains a failed sequence gap and detach leaves the server open", () =>
    Effect.gen(function* () {
      const { baseUrl } = yield* acquireDurableStreamServer;
      const client = yield* DurableStreamsClient.make({
        url: baseUrl + "/gap",
        contentType: "text/plain",
      });
      const responses = yield* Queue.unbounded<{
        readonly status: number;
        readonly headers: Readonly<Record<string, string>>;
      }>();
      const http = yield* HttpClient.HttpClient;
      const producer = yield* client
        .producer({ producerId: "p", maxBatchBytes: 1 })
        .pipe(
          Effect.provideService(
            HttpClient.HttpClient,
            http.pipe(
              HttpClient.tap((response) =>
                Queue.offer(responses, { status: response.status, headers: response.headers }),
              ),
            ),
          ),
        );
      const failure = yield* producer.append({ value: "missing" }).pipe(Effect.flip);
      expect(failure._tag).toBe("StreamNotFoundError");
      expect((yield* Queue.take(responses)).status).toBe(404);
      yield* client.create({});
      expect(yield* producer.append({ value: "gap" }).pipe(Effect.flip)).toBe(failure);
      expect(yield* Queue.take(responses)).toMatchObject({
        status: 409,
        headers: { "producer-expected-seq": "0", "producer-received-seq": "1" },
      });
      const healthy = yield* client.producer({ producerId: "healthy", maxBatchBytes: 1 });
      yield* healthy.append({ value: "kept" });
      yield* healthy.detach;
      expect(yield* client.head).toMatchObject({ closed: false });
      expect(yield* client.text.pipe(Stream.runCollect)).toEqual(["kept"]);
    }).pipe(Effect.provide(FetchHttpClient.layer)),
  );
});
