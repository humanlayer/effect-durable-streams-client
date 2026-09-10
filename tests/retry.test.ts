import { describe, expect, it } from "@effect/vitest";
import {
  Duration,
  Effect,
  Exit,
  Fiber,
  Option,
  Queue,
  Random,
  Ref,
  Schedule,
  Schema,
} from "effect";
import { TestClock } from "effect/testing";
import { HttpClient, HttpClientRequest } from "effect/unstable/http";
import { DurableStreamsClient, DurableStreamsConnection } from "../src/index.ts";
import { parseRetryAfter, requestRetrySchedule, waitForSseReconnect } from "../src/retry.ts";
import { makeScriptedHttpClient, ScriptedResponse } from "./support/http-client.ts";

describe("reference retry policy", () => {
  it.effect("caps SSE full jitter and floors fractional milliseconds", () =>
    Effect.gen(function* () {
      const connection = yield* Schema.decodeEffect(DurableStreamsConnection)({
        url: "https://streams.test/s",
      });
      const completed = yield* Ref.make(false);
      const run = yield* waitForSseReconnect({ connection, shortConnections: 20 }).pipe(
        Effect.tap(() => Ref.set(completed, true)),
        Effect.forkChild,
      );
      yield* TestClock.adjust("1665 millis");
      expect(yield* Ref.get(completed)).toBe(false);
      yield* TestClock.adjust("1 millis");
      yield* Fiber.join(run);
      expect(yield* Ref.get(completed)).toBe(true);
    }).pipe(
      Effect.provideService(Random.Random, {
        nextDoubleUnsafe: () => 1 / 3,
        nextIntUnsafe: () => 0,
      }),
    ),
  );
  it.effect("matches 100ms/1.3/capped full jitter indefinitely and honors server floors", () =>
    Effect.gen(function* () {
      const connection = yield* Schema.decodeEffect(DurableStreamsConnection)({
        url: "https://streams.test/orders",
      });
      const step = yield* Schedule.toStep(requestRetrySchedule(connection));
      for (let attempt = 1; attempt <= 80; attempt++) {
        const [, delay] = yield* step(0, {});
        expect(Duration.toMillis(delay)).toBeCloseTo(
          Math.min(100 * 1.3 ** (attempt - 1), 60000) * 0.5,
          5,
        );
      }
      const [, floor] = yield* step(0, { retryAfter: Duration.seconds(90) });
      expect(Duration.toMillis(floor)).toBe(90000);
    }).pipe(
      Effect.provideService(Random.Random, { nextDoubleUnsafe: () => 0.5, nextIntUnsafe: () => 0 }),
    ),
  );

  it.effect("uses reference Retry-After numeric/date behavior without an invented budget", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(Date.UTC(2026, 0, 1));
      for (const [raw, millis] of [
        ["1.5", 1500],
        ["7200", 7200000],
        ["Thu, 01 Jan 2026 00:00:05 GMT", 5000],
        ["Fri, 02 Jan 2026 00:00:00 GMT", 3600000],
        ["Wed, 31 Dec 2025 23:59:59 GMT", 0],
      ] as const) {
        expect(yield* parseRetryAfter(raw)).toMatchObject({ value: Duration.millis(millis) });
      }
      expect(yield* parseRetryAfter("nonsense")).toEqual(Option.none());
      expect(yield* parseRetryAfter(undefined)).toEqual(Option.none());
    }),
  );

  it.effect("retries the immutable request after Retry-After and reevaluates ambient auth", () =>
    Effect.gen(function* () {
      const scripted = yield* makeScriptedHttpClient;
      const calls = yield* Ref.make(0);
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/orders",
        contentType: "text/plain",
      });
      const operation = Effect.gen(function* () {
        const http = yield* HttpClient.HttpClient;
        const authorized = http.pipe(
          HttpClient.mapRequestEffect((request) =>
            Ref.updateAndGet(calls, (n) => n + 1).pipe(
              Effect.map((n) =>
                request.pipe(HttpClientRequest.setHeader("authorization", `token-${n}`)),
              ),
            ),
          ),
        );
        return yield* client
          .create({ value: "immutable" })
          .pipe(Effect.provideService(HttpClient.HttpClient, authorized));
      }).pipe(Effect.provide(scripted.layer));
      yield* scripted.respond(
        ScriptedResponse.Response({ status: 429, headers: { "retry-after": "2" } }),
      );
      const fiber = yield* operation.pipe(Effect.forkChild);
      const first = yield* Queue.take(scripted.requests);
      yield* TestClock.adjust("1999 millis");
      expect(yield* Ref.get(calls)).toBe(1);
      yield* scripted.respond(
        ScriptedResponse.Response({
          status: 201,
          headers: { "content-type": "text/plain", "stream-next-offset": "tail" },
        }),
      );
      yield* TestClock.adjust("1 millis");
      expect(yield* Fiber.join(fiber)).toMatchObject({ status: 201 });
      const second = yield* Queue.take(scripted.requests);
      expect(first.body).toEqual(second.body);
      expect(first.url).toEqual(second.url);
      expect(first.headers.authorization).toBe("token-1");
      expect(second.headers.authorization).toBe("token-2");
    }),
  );

  it.effect("continues past finite budgets until interruption and releases request scope", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({ url: "https://streams.test/orders" });
      const fiber = yield* client.delete.pipe(Effect.provide(http.layer), Effect.forkChild);
      for (let i = 0; i < 10; i++) {
        yield* Queue.take(http.requests);
        yield* http.respond(
          ScriptedResponse.Response({
            status: i % 2 === 0 ? 503 : 302,
            headers: { "retry-after": "60" },
          }),
        );
        yield* TestClock.adjust("60 seconds");
      }
      yield* Queue.take(http.requests);
      yield* http.respond(ScriptedResponse.TransportFailure());
      yield* TestClock.adjust("1 millis");
      yield* Fiber.interrupt(fiber);
      expect(Exit.hasInterrupts(yield* Fiber.await(fiber))).toBe(true);
      expect(yield* Ref.get(http.active)).toBe(0);
      yield* TestClock.adjust("1 hour");
      expect(yield* Queue.size(http.requests)).toBe(0);
    }).pipe(
      Effect.provideService(Random.Random, { nextDoubleUnsafe: () => 0.5, nextIntUnsafe: () => 0 }),
    ),
  );

  it.effect("honors configured maxRetries and does not retry HEAD or definitive 405", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/orders",
        backoffOptions: { maxRetries: 1 },
      });
      yield* http.respond(ScriptedResponse.TransportFailure());
      yield* http.respond(ScriptedResponse.Response({ status: 503, headers: {} }));
      const fiber = yield* client.delete.pipe(
        Effect.flip,
        Effect.provide(http.layer),
        Effect.forkChild,
      );
      yield* Queue.take(http.requests);
      yield* TestClock.adjust("100 millis");
      const error = yield* Fiber.join(fiber);
      expect(error).toHaveProperty("_tag", "StreamUnavailableError");
      expect(error).toHaveProperty("response.status", 503);
      yield* Queue.take(http.requests);
      for (const operation of [client.head, client.close({}), client.delete]) {
        yield* http.respond(ScriptedResponse.Response({ status: 405, headers: {} }));
        expect(yield* operation.pipe(Effect.flip, Effect.provide(http.layer))).toMatchObject({
          _tag: operation === client.head ? "ProtocolViolationError" : "OperationNotSupportedError",
        });
        yield* Queue.take(http.requests);
        expect(yield* Queue.size(http.requests)).toBe(0);
      }
    }),
  );
});
