import { describe, expect, expectTypeOf, it } from "@effect/vitest";
import {
  Context,
  Data,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Match,
  Queue,
  Ref,
  Schema,
  Stream,
} from "effect";
import { HttpClient, HttpClientError, HttpClientResponse } from "effect/unstable/http";
import { DurableStreamsClient, type AppendError } from "../src/index.ts";
import { makeScriptedHttpClient, ScriptedResponse } from "./support/http-client.ts";

class SourceFailure extends Data.TaggedError("SourceFailure") {}
class SourceConfig extends Context.Service<SourceConfig, { readonly chunk: string }>()(
  "test/SourceConfig",
) {}

describe("streamed request bodies", () => {
  it.effect(
    "owns the native bridge consumer even when transport does not cancel its Web body",
    () =>
      Effect.gen(function* () {
        const consumer = yield* Deferred.make<Fiber.Fiber<unknown, unknown>>();
        const produced = yield* Deferred.make<void>();
        const http = HttpClient.make((request) =>
          Effect.gen(function* () {
            yield* Match.value(request.body).pipe(
              Match.tag("Stream", ({ stream }) =>
                stream.pipe(
                  Stream.mapEffect((bytes) =>
                    Effect.fiber.pipe(
                      Effect.tap((fiber) => Deferred.succeed(consumer, fiber)),
                      Effect.as(bytes),
                    ),
                  ),
                  (body) => Stream.toReadableStreamEffect(body),
                ),
              ),
              Match.orElse(() => Effect.die("Expected streamed body")),
            );
            yield* Deferred.await(produced);
            yield* Deferred.await(consumer);
            return HttpClientResponse.fromWeb(request, new Response(null, { status: 413 }));
          }),
        );
        const client = yield* DurableStreamsClient.make({ url: "https://streams.test/bytes" });
        const source = Stream.fromEffect(
          Deferred.succeed(produced, undefined).pipe(Effect.as("chunk")),
        ).pipe(Stream.concat(Stream.never));
        expect(
          yield* client
            .appendStream({ source })
            .pipe(Effect.flip, Effect.provide(Layer.succeed(HttpClient.HttpClient, http))),
        ).toHaveProperty("_tag", "PayloadTooLargeError");
        const bridge = yield* Deferred.await(consumer);
        expect(bridge.pollUnsafe()).toBeDefined();
      }),
  );
  it.effect(
    "does not replay transport or server uncertainty and validates seq before sending",
    () =>
      Effect.gen(function* () {
        const http = yield* makeScriptedHttpClient;
        const client = yield* DurableStreamsClient.make({ url: "https://streams.test/bytes" });
        for (const reply of [
          ScriptedResponse.TransportFailure(),
          ScriptedResponse.Response({ status: 503, headers: {} }),
        ]) {
          yield* http.respond(reply);
          expect(
            yield* client
              .appendStream({ source: Stream.make("data") })
              .pipe(Effect.flip, Effect.provide(http.layer)),
          ).toHaveProperty("_tag", "AppendOutcomeUnknownError");
          yield* Queue.take(http.requests);
          expect(yield* Queue.size(http.requests)).toBe(0);
        }
        expect(
          yield* client
            .appendStream({ source: Stream.make("data"), seq: "bad\nseq" })
            .pipe(Effect.flip, Effect.provide(http.layer)),
        ).toHaveProperty("_tag", "PayloadEncodeError");
        expect(yield* Queue.size(http.requests)).toBe(0);
      }),
  );

  it.effect("preserves source defects as defects and finalizes the source", () =>
    Effect.gen(function* () {
      const finalized = yield* Ref.make(false);
      const http = HttpClient.make((request) =>
        Match.value(request.body).pipe(
          Match.tag("Stream", ({ stream }) => stream.pipe(Stream.orDie, Stream.runDrain)),
          Match.orElse(() => Effect.die("Expected stream")),
          Effect.andThen(Effect.die("Expected source defect")),
        ),
      );
      const client = yield* DurableStreamsClient.make({ url: "https://streams.test/bytes" });
      const source = Stream.die("source defect").pipe(Stream.ensuring(Ref.set(finalized, true)));
      const exit = yield* client
        .appendStream({ source })
        .pipe(Effect.exit, Effect.provide(Layer.succeed(HttpClient.HttpClient, http)));
      expect(Exit.hasDies(exit)).toBe(true);
      expect(yield* Ref.get(finalized)).toBe(true);
    }),
  );
  it.effect("preserves source E/R, streams exact raw chunks, and waits for downstream demand", () =>
    Effect.gen(function* () {
      const received = yield* Queue.unbounded<Uint8Array>();
      const release = yield* Deferred.make<void>();
      const produced = yield* Ref.make(0);
      const finalized = yield* Ref.make(false);
      const http = HttpClient.make((request) =>
        Effect.gen(function* () {
          expect(request.method).toBe("POST");
          expect(request.headers).toMatchObject({
            "content-type": "application/json",
            "stream-seq": "opaque",
          });
          yield* Match.value(request.body).pipe(
            Match.tag("Stream", ({ stream }) =>
              stream.pipe(
                Stream.mapError(
                  (cause) =>
                    new HttpClientError.HttpClientError({
                      reason: new HttpClientError.TransportError({ request, cause }),
                    }),
                ),
                Stream.runForEach((bytes) =>
                  Queue.offer(received, bytes).pipe(Effect.andThen(Deferred.await(release))),
                ),
              ),
            ),
            Match.orElse(() => Effect.die("Expected real streaming HTTP body")),
            Effect.mapError(
              (cause) =>
                new HttpClientError.HttpClientError({
                  reason: new HttpClientError.TransportError({ request, cause }),
                }),
            ),
          );
          return HttpClientResponse.fromWeb(
            request,
            new Response(null, { status: 204, headers: { "stream-next-offset": "tail" } }),
          );
        }),
      );
      const source: Stream.Stream<string | Uint8Array, SourceFailure, SourceConfig> = Stream.unwrap(
        Effect.gen(function* () {
          const config = yield* SourceConfig;
          yield* Effect.addFinalizer(() => Ref.set(finalized, true));
          return Stream.make(config.chunk, new Uint8Array([50, 93])).pipe(
            Stream.rechunk(1),
            Stream.mapEffect((chunk) => Ref.update(produced, (n) => n + 1).pipe(Effect.as(chunk))),
          );
        }),
      );
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/json",
        schema: Schema.String,
      });
      const operation = client.appendStream({ source, seq: "opaque" });
      expectTypeOf<Effect.Error<typeof operation>>().toEqualTypeOf<AppendError | SourceFailure>();
      expectTypeOf<Effect.Services<typeof operation>>().toEqualTypeOf<
        HttpClient.HttpClient | SourceConfig
      >();
      const provided = operation.pipe(
        Effect.provide(
          Layer.merge(
            Layer.succeed(HttpClient.HttpClient, http),
            Layer.succeed(SourceConfig, { chunk: "[1," }),
          ),
        ),
      );
      expectTypeOf<Effect.Services<typeof provided>>().toEqualTypeOf<never>();
      const fiber = yield* provided.pipe(Effect.forkChild);
      expect(new TextDecoder().decode(yield* Queue.take(received))).toBe("[1,");
      expect(yield* Ref.get(produced)).toBe(1);
      expect(yield* Ref.get(finalized)).toBe(false);
      yield* Deferred.succeed(release, undefined);
      expect(yield* Queue.take(received)).toEqual(new Uint8Array([50, 93]));
      expect(yield* Fiber.join(fiber)).toEqual({ offset: "tail", closed: false });
      expect(yield* Ref.get(finalized)).toBe(true);
    }),
  );

  it.effect(
    "returns the exact typed source failure even when HTTP wraps it, and never replays",
    () =>
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        const released = yield* Ref.make(false);
        const sourceReleased = yield* Ref.make(false);
        const error = new SourceFailure();
        const http = HttpClient.make((request) =>
          Effect.gen(function* () {
            yield* Ref.update(calls, (n) => n + 1);
            return yield* Match.value(request.body).pipe(
              Match.tag("Stream", ({ stream }) =>
                stream.pipe(
                  Stream.mapError(
                    (cause) =>
                      new HttpClientError.HttpClientError({
                        reason: new HttpClientError.TransportError({ request, cause }),
                      }),
                  ),
                  Stream.runDrain,
                ),
              ),
              Match.orElse(() => Effect.die("Expected stream")),
              Effect.mapError(
                (cause) =>
                  new HttpClientError.HttpClientError({
                    reason: new HttpClientError.TransportError({ request, cause }),
                  }),
              ),
              Effect.andThen(Effect.die("Source should fail")),
            );
          }).pipe(Effect.ensuring(Ref.set(released, true))),
        );
        const client = yield* DurableStreamsClient.make({ url: "https://streams.test/bytes" });
        const source = Stream.make(new Uint8Array([1])).pipe(
          Stream.concat(Stream.fail(error)),
          Stream.ensuring(Ref.set(sourceReleased, true)),
        );
        expect(
          yield* client
            .appendStream({ source })
            .pipe(Effect.flip, Effect.provide(Layer.succeed(HttpClient.HttpClient, http))),
        ).toBe(error);
        expect(yield* Ref.get(calls)).toBe(1);
        expect(yield* Ref.get(released)).toBe(true);
        expect(yield* Ref.get(sourceReleased)).toBe(true);
      }),
  );

  it.effect(
    "interruption joins the pending source pull and releases HTTP and source resources",
    () =>
      Effect.gen(function* () {
        const pulling = yield* Deferred.make<void>();
        const sourceReleased = yield* Ref.make(false);
        const requestReleased = yield* Ref.make(false);
        const http = HttpClient.make((request) =>
          Match.value(request.body).pipe(
            Match.tag("Stream", ({ stream }) =>
              stream.pipe(
                Stream.mapError(
                  (cause) =>
                    new HttpClientError.HttpClientError({
                      reason: new HttpClientError.TransportError({ request, cause }),
                    }),
                ),
                Stream.runDrain,
              ),
            ),
            Match.orElse(() => Effect.die("Expected stream")),
            Effect.mapError(
              (cause) =>
                new HttpClientError.HttpClientError({
                  reason: new HttpClientError.TransportError({ request, cause }),
                }),
            ),
            Effect.andThen(Effect.die("Interrupted body cannot complete")),
            Effect.ensuring(Ref.set(requestReleased, true)),
          ),
        );
        const client = yield* DurableStreamsClient.make({ url: "https://streams.test/bytes" });
        const source = Stream.fromEffect(
          Deferred.succeed(pulling, undefined).pipe(Effect.andThen(Effect.never)),
        ).pipe(Stream.ensuring(Ref.set(sourceReleased, true)));
        const fiber = yield* client
          .appendStream({ source })
          .pipe(Effect.provide(Layer.succeed(HttpClient.HttpClient, http)), Effect.forkChild);
        yield* Deferred.await(pulling);
        yield* Fiber.interrupt(fiber);
        expect(Exit.hasInterrupts(yield* Fiber.await(fiber))).toBe(true);
        expect(yield* Ref.get(sourceReleased)).toBe(true);
        expect(yield* Ref.get(requestReleased)).toBe(true);
      }),
  );

  it.effect("early HTTP rejection closes an in-flight body source without requiring EOF", () =>
    Effect.gen(function* () {
      const pulling = yield* Deferred.make<void>();
      const sourceReleased = yield* Ref.make(false);
      const http = HttpClient.make((request) =>
        Effect.gen(function* () {
          yield* Match.value(request.body).pipe(
            Match.tag("Stream", ({ stream }) => stream.pipe(Stream.runDrain, Effect.forkScoped)),
            Match.orElse(() => Effect.die("Expected stream")),
          );
          yield* Deferred.await(pulling);
          return HttpClientResponse.fromWeb(request, new Response(null, { status: 413 }));
        }).pipe(Effect.scoped),
      );
      const client = yield* DurableStreamsClient.make({ url: "https://streams.test/bytes" });
      const source = Stream.fromEffect(
        Deferred.succeed(pulling, undefined).pipe(Effect.andThen(Effect.never)),
      ).pipe(Stream.ensuring(Ref.set(sourceReleased, true)));
      expect(
        yield* client
          .appendStream({ source })
          .pipe(Effect.flip, Effect.provide(Layer.succeed(HttpClient.HttpClient, http))),
      ).toHaveProperty("_tag", "PayloadTooLargeError");
      expect(yield* Ref.get(sourceReleased)).toBe(true);
    }),
  );
});
