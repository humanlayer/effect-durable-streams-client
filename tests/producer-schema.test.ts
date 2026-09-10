import { describe, expect, expectTypeOf, it } from "@effect/vitest";
import {
  Context,
  Deferred,
  Duration,
  Effect,
  Fiber,
  Layer,
  Queue,
  Ref,
  Schema,
  SchemaGetter,
  Scope,
  Stream,
} from "effect";
import type { HttpClient } from "effect/unstable/http";
import { DurableStreamsClient } from "../src/index.ts";
import { ScriptedResponse } from "./support/http-client.ts";
import { makeProducerHttp, producerReply } from "./support/producer-http.ts";

describe("producer schema and dependencies", () => {
  it.effect(
    "keeps HTTP at scoped acquisition and encoding services at append, Sink and close",
    () =>
      Effect.gen(function* () {
        class Encoder extends Context.Service<Encoder, { readonly suffix: string }>()(
          "producer/Encoder",
        ) {}
        class Decoder extends Context.Service<Decoder, { readonly prefix: string }>()(
          "producer/Decoder",
        ) {}
        const count = yield* Ref.make(0);
        const codec = Schema.String.pipe(
          Schema.decodeTo(Schema.Struct({ id: Schema.String }), {
            decode: SchemaGetter.transformOrFail((id) =>
              Decoder.pipe(Effect.map((decoder) => ({ id: decoder.prefix + id }))),
            ),
            encode: SchemaGetter.transformOrFail((value) =>
              Encoder.pipe(
                Effect.tap(() => Ref.update(count, (n) => n + 1)),
                Effect.map((encoder) => value.id + encoder.suffix),
              ),
            ),
          }),
        );
        const client = yield* DurableStreamsClient.make({
          url: "http://localhost/json",
          schema: codec,
        });
        const acquisition = client.producer({ producerId: "p", autoClaim: true, maxBatchBytes: 1 });
        expectTypeOf<Effect.Services<typeof acquisition>>().toEqualTypeOf<
          HttpClient.HttpClient | Scope.Scope
        >();
        const http = yield* makeProducerHttp;
        const producer = yield* acquisition.pipe(Effect.provide(http.layer));
        expectTypeOf<Parameters<typeof producer.append>[0]["value"]>().toEqualTypeOf<{
          readonly id: string;
        }>();
        const append = producer.append({ value: { id: "a" } });
        const close = producer.close({ value: { id: "last" } });
        expectTypeOf<Effect.Services<typeof append>>().toEqualTypeOf<Encoder>();
        expectTypeOf<Effect.Services<typeof close>>().toEqualTypeOf<Encoder>();
        const ingest = Stream.make({ id: "sink" }).pipe(Stream.run(producer.sink));
        expectTypeOf<Effect.Services<typeof ingest>>().toEqualTypeOf<Encoder>();
        expectTypeOf<Effect.Services<typeof producer.flush>>().toEqualTypeOf<never>();
        const run = yield* append.pipe(
          Effect.provideService(Encoder, { suffix: "!" }),
          Effect.forkChild,
        );
        const first = yield* Queue.take(http.requests);
        expect(new TextDecoder().decode(first.body)).toBe('["a!"]');
        yield* Deferred.succeed(
          first.reply,
          ScriptedResponse.Response({ status: 403, headers: { "producer-epoch": "9" } }),
        );
        const retry = yield* Queue.take(http.requests);
        expect(retry.body).toEqual(first.body);
        expect(yield* Ref.get(count)).toBe(1);
        yield* Deferred.succeed(retry.reply, producerReply({ seq: 0, epoch: 10 }));
        yield* Fiber.join(run);
        const final = yield* close.pipe(
          Effect.provideService(Encoder, { suffix: "?" }),
          Effect.forkChild,
        );
        const request = yield* Queue.take(http.requests);
        expect(new TextDecoder().decode(request.body)).toBe('["last?"]');
        yield* Deferred.succeed(request.reply, producerReply({ seq: 1, epoch: 10, closed: true }));
        yield* Fiber.join(final);
      }),
  );

  it.effect("validates options and payloads before admission without network", () =>
    Effect.gen(function* () {
      const http = yield* makeProducerHttp;
      yield* Effect.gen(function* () {
        const client = yield* DurableStreamsClient.make({
          url: "http://localhost/json",
          schema: Schema.String.check(Schema.isMinLength(3)),
        });
        for (const options of [
          { producerId: "" },
          { producerId: "bad\nheader" },
          { producerId: "p", epoch: -1 },
          { producerId: "p", epoch: 0.5 },
          { producerId: "p", epoch: Number.MAX_SAFE_INTEGER + 1 },
          { producerId: "p", maxBatchBytes: 0 },
          { producerId: "p", maxInFlight: 0 },
          { producerId: "p", linger: Duration.millis(-1) },
          { producerId: "p", linger: Duration.infinity },
        ])
          expect((yield* client.producer(options).pipe(Effect.flip))._tag).toBe(
            "InvalidDurableStreamsConfigError",
          );
        const producer = yield* client.producer({ producerId: "p" });
        expect((yield* producer.append({ value: "x" }).pipe(Effect.flip))._tag).toBe(
          "PayloadEncodeError",
        );
        expect((yield* Stream.make("y").pipe(Stream.run(producer.sink), Effect.flip))._tag).toBe(
          "PayloadEncodeError",
        );
        expect(yield* producer.nextSeq).toBe(0);
        expect(yield* Queue.size(http.requests)).toBe(0);
      }).pipe(Effect.provide(http.layer));
    }),
  );

  it.effect("contextual raw producer retains precise fully provisioned requirements", () =>
    Effect.gen(function* () {
      const http = yield* makeProducerHttp;
      const program = Effect.gen(function* () {
        const client = yield* DurableStreamsClient;
        const producer = yield* client.producer({ producerId: "p", maxBatchBytes: 1 });
        expectTypeOf<Parameters<typeof producer.append>[0]["value"]>().toEqualTypeOf<
          Schema.Json | Uint8Array
        >();
        const run = yield* producer.append({ value: null }).pipe(Effect.forkChild);
        const request = yield* Queue.take(http.requests);
        yield* Deferred.succeed(request.reply, producerReply({ seq: 0 }));
        return yield* Fiber.join(run);
      });
      const provided = program.pipe(
        Effect.provide(
          Layer.merge(
            http.layer,
            DurableStreamsClient.layer({
              url: "http://localhost/json",
              contentType: "application/json",
            }),
          ),
        ),
        Effect.scoped,
      );
      expectTypeOf<Effect.Services<typeof provided>>().toEqualTypeOf<never>();
      expect(yield* provided).toMatchObject({ duplicate: false });
    }),
  );
});
