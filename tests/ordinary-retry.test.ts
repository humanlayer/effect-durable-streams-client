import { describe, expect, it } from "@effect/vitest";
import {
  Deferred,
  Effect,
  Exit,
  Fiber,
  Match,
  Queue,
  Random,
  Ref,
  Schema,
  SchemaGetter,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import { HttpClient, HttpClientRequest } from "effect/unstable/http";
import {
  DurableStreamsClient,
  type AppendResult,
  type CloseResult,
  type CloseError,
} from "../src/index.js";
import { makeReadHttp, readReply } from "./support/read-http.js";
import { makeScriptedHttpClient, ScriptedResponse } from "./support/http-client.js";

describe("reference ordinary write retries", () => {
  it.effect(
    "does not retry definitive rejection with unlimited defaults, including final close",
    () =>
      Effect.gen(function* () {
        const http = yield* makeScriptedHttpClient;
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/s",
          batching: false,
        });
        for (const [status, tag] of [
          [400, "InvalidRequestError"],
          [401, "UnauthorizedError"],
          [403, "ForbiddenError"],
          [404, "StreamNotFoundError"],
          [405, "OperationNotSupportedError"],
          [408, "AppendOutcomeUnknownError"],
          [409, "StreamClosedError"],
          [410, "StreamGoneError"],
          [413, "PayloadTooLargeError"],
        ] as const) {
          const operations: ReadonlyArray<
            Effect.Effect<AppendResult | CloseResult, CloseError, HttpClient.HttpClient>
          > = [
            client.append({ value: "one", seq: "s" }),
            client.close({ value: "final", seq: "s" }),
          ];
          for (const operation of operations) {
            yield* http.respond(
              ScriptedResponse.Response({
                status,
                headers: { "stream-closed": "true", "stream-next-offset": "final" },
              }),
            );
            const failure = yield* operation.pipe(Effect.flip, Effect.provide(http.layer));
            expect(failure).toHaveProperty("_tag", tag);
            yield* Queue.take(http.requests);
            expect(yield* Queue.size(http.requests)).toBe(0);
          }
        }
      }),
  );
  for (const mode of ["text", "binary", "json", "undefined-final"] as const) {
    it.effect(`retries immutable ${mode} body, seq and closure while refreshing ambient auth`, () =>
      Effect.gen(function* () {
        const http = yield* makeScriptedHttpClient;
        const encodings = yield* Ref.make(0);
        const auth = yield* Ref.make(0);
        const schema = Schema.Null.pipe(
          Schema.decodeTo(Schema.Undefined, {
            decode: SchemaGetter.transform(() => undefined),
            encode: SchemaGetter.transformOrFail(() =>
              Ref.update(encodings, (n) => n + 1).pipe(Effect.as(null)),
            ),
          }),
        );
        const contentType =
          mode === "json" || mode === "undefined-final"
            ? "application/json"
            : mode === "text"
              ? "text/plain"
              : "application/octet-stream";
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/s",
          contentType,
          batching: false,
        });
        const custom = yield* DurableStreamsClient.make({
          url: "https://streams.test/s",
          schema,
          batching: false,
        });
        const value = Match.value(mode).pipe(
          Match.when("text", () => "hé"),
          Match.when("binary", () => new Uint8Array([0, 255, 10])),
          Match.orElse(() => [1, [2, 3]]),
        );
        const operations: ReadonlyArray<
          Effect.Effect<AppendResult | CloseResult, CloseError, HttpClient.HttpClient>
        > =
          mode === "undefined-final"
            ? [custom.close({ value: undefined, seq: "Opaque+" })]
            : [client.append({ value, seq: "Opaque+" }), client.close({ value, seq: "Opaque+" })];
        for (const operation of operations) {
          const run = yield* operation.pipe(
            Effect.provideServiceEffect(
              HttpClient.HttpClient,
              Effect.map(HttpClient.HttpClient, (base) =>
                base.pipe(
                  HttpClient.mapRequestEffect((request) =>
                    Ref.updateAndGet(auth, (n) => n + 1).pipe(
                      Effect.map((n) =>
                        HttpClientRequest.setHeader(request, "authorization", `token-${n}`),
                      ),
                    ),
                  ),
                ),
              ),
            ),
            Effect.provide(http.layer),
            Effect.forkChild,
          );
          const first = yield* Queue.take(http.requests);
          for (const failure of [
            ScriptedResponse.Response({ status: 503, headers: { "retry-after": "2" } }),
            ScriptedResponse.TransportFailure(),
            ScriptedResponse.Response({ status: 501, headers: {} }),
          ]) {
            yield* http.respond(failure);
            yield* TestClock.adjust("2 seconds");
            const retry = yield* Queue.take(http.requests);
            expect(retry.body).toEqual(first.body);
            expect(retry.headers["content-type"]).toBe(contentType);
            expect(retry.headers["stream-seq"]).toBe("Opaque+");
            expect(retry.headers["stream-closed"]).toBe(first.headers["stream-closed"]);
            expect(retry.headers.authorization).not.toBe(first.headers.authorization);
            expect(yield* Ref.get(http.active)).toBe(1);
          }
          expect(
            Match.value(first.body).pipe(
              Match.tag("Uint8Array", ({ body }) => body),
              Match.orElse(() => new Uint8Array()),
            ),
          ).toEqual(
            mode === "binary"
              ? new Uint8Array([0, 255, 10])
              : new TextEncoder().encode(
                  Match.value(mode).pipe(
                    Match.when("text", () => "hé"),
                    Match.when("json", () => "[[1,[2,3]]]"),
                    Match.orElse(() => "[null]"),
                  ),
                ),
          );
          yield* http.respond(
            ScriptedResponse.Response({
              status: 204,
              headers: { "stream-next-offset": "tail", "stream-closed": "true" },
            }),
          );
          yield* Fiber.join(run);
          expect(yield* Ref.get(http.active)).toBe(0);
          expect(yield* Queue.size(http.requests)).toBe(0);
        }
        expect(yield* Ref.get(encodings)).toBe(mode === "undefined-final" ? 1 : 0);
      }).pipe(
        Effect.provideService(Random.Random, {
          nextDoubleUnsafe: () => 0.5,
          nextIntUnsafe: () => 0,
        }),
      ),
    );
  }

  it.effect("finite exhaustion preserves typed last-response classification and snapshot", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/s",
        batching: false,
        backoffOptions: { maxRetries: 1 },
      });
      for (const [status, tag] of [
        [500, "AppendOutcomeUnknownError"],
        [501, "OperationNotSupportedError"],
        [429, "RateLimitedError"],
        [307, "AppendOutcomeUnknownError"],
      ] as const) {
        const operations: ReadonlyArray<
          Effect.Effect<AppendResult | CloseResult, CloseError, HttpClient.HttpClient>
        > = [client.append({ value: "one" }), client.close({ value: "final" })];
        for (const operation of operations) {
          const run = yield* operation.pipe(
            Effect.flip,
            Effect.provide(http.layer),
            Effect.forkChild,
          );
          yield* Queue.take(http.requests);
          yield* http.respond(ScriptedResponse.TransportFailure());
          yield* TestClock.adjust("100 millis");
          yield* Queue.take(http.requests);
          yield* http.respond(
            ScriptedResponse.Response({
              status,
              headers: { "retry-after": "3", "set-cookie": "secret" },
              body: "diagnostic",
            }),
          );
          const failure = yield* Fiber.join(run);
          expect(failure).toHaveProperty("_tag", tag);
          expect(failure).toHaveProperty("response.status", status);
          expect(failure).toHaveProperty("response.headers.set-cookie", "[REDACTED]");
          expect(yield* Queue.size(http.requests)).toBe(0);
        }
      }
    }),
  );

  it.effect("joins failed response cleanup before retry and cancels a write backoff", () =>
    Effect.gen(function* () {
      const http = yield* makeReadHttp;
      const releasing = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/s",
        batching: false,
      });
      const run = yield* client
        .append({ value: "one" })
        .pipe(Effect.provide(http.layer), Effect.forkChild);
      const first = yield* Queue.take(http.requests);
      const reply = readReply({ offset: "unused", text: "diagnostic" });
      yield* Queue.offer(http.replies, {
        ...reply,
        status: 503,
        headers: { "retry-after": "60" },
        body: reply.body.pipe(
          Stream.ensuring(
            Deferred.succeed(releasing, undefined).pipe(Effect.andThen(Deferred.await(release))),
          ),
        ),
      });
      yield* Deferred.await(releasing);
      yield* TestClock.adjust("1 hour");
      expect(yield* Queue.size(http.requests)).toBe(0);
      yield* Deferred.succeed(release, undefined);
      yield* TestClock.adjust("1 millis");
      expect(first.signal.aborted).toBe(true);
      yield* Fiber.interrupt(run);
      expect(Exit.hasInterrupts(yield* Fiber.await(run))).toBe(true);
      yield* TestClock.adjust("1 hour");
      expect(yield* Queue.size(http.requests)).toBe(0);
    }),
  );
});
