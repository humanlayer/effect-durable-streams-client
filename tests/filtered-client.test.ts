import { inspect } from "node:util";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Fiber, Layer, Logger, Queue, Schema } from "effect";
import { TestClock } from "effect/testing";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";
import {
  type CreateError,
  type CloseError,
  type DeleteError,
  type HeadError,
  DurableStreamsClient,
  ErrorResponseBody,
} from "../src/index";
import { makeScriptedHttpClient, ScriptedResponse } from "./support/http-client";

const Filtered = Layer.effect(
  HttpClient.HttpClient,
  Effect.map(HttpClient.HttpClient, HttpClient.filterStatusOk),
);

describe("ambient status-filtered HTTP clients", () => {
  it.effect("classifies definitive mutation rejections without retry or unknown outcomes", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/orders",
        contentType: "text/plain",
      });
      for (const operation of [
        client.create({}).pipe(Effect.as(undefined)),
        client.append({ value: "private" }).pipe(Effect.as(undefined)),
        client.close({}).pipe(Effect.as(undefined)),
        client.close({ value: "private" }).pipe(Effect.as(undefined)),
        client.delete,
      ]) {
        for (const [status, tag] of [
          [400, "InvalidRequestError"],
          [401, "UnauthorizedError"],
          [403, "ForbiddenError"],
          [410, "StreamGoneError"],
        ] as const) {
          yield* http.respond(
            ScriptedResponse.Response({ status, headers: {}, body: "diagnostic" }),
          );
          const error = yield* operation.pipe(
            Effect.flip<
              void,
              CreateError | CloseError | DeleteError | HeadError,
              HttpClient.HttpClient
            >,
            Effect.provide(Filtered.pipe(Layer.provide(http.layer))),
          );
          expect(error).toHaveProperty("_tag", tag);
          expect(error).toHaveProperty("response.status", status);
          yield* Queue.take(http.requests);
          expect(yield* Queue.size(http.requests)).toBe(0);
        }
      }
      for (const operation of [
        client.append({ value: "private" }).pipe(Effect.as(undefined)),
        client.close({}).pipe(Effect.as(undefined)),
        client.delete,
      ]) {
        for (const [status, tag] of [
          [404, "StreamNotFoundError"],
          [405, "OperationNotSupportedError"],
        ] as const) {
          yield* http.respond(ScriptedResponse.Response({ status, headers: {} }));
          expect(
            yield* operation.pipe(
              Effect.flip<
                void,
                CreateError | CloseError | DeleteError | HeadError,
                HttpClient.HttpClient
              >,
              Effect.provide(Filtered.pipe(Layer.provide(http.layer))),
            ),
          ).toHaveProperty("_tag", tag);
          yield* Queue.take(http.requests);
          expect(yield* Queue.size(http.requests)).toBe(0);
        }
      }
    }),
  );

  it.effect("preserves HEAD/connect missing metadata and rate-limit snapshots without retry", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/orders?secret=private",
      });
      for (const operation of [client.head, client.connect]) {
        yield* http.respond(ScriptedResponse.Response({ status: 404, headers: {} }));
        expect(
          yield* operation.pipe(Effect.provide(Filtered.pipe(Layer.provide(http.layer)))),
        ).toHaveProperty("_tag", "Missing");
        for (const [status, tag] of [
          [401, "UnauthorizedError"],
          [403, "ForbiddenError"],
          [410, "StreamGoneError"],
          [503, "StreamUnavailableError"],
          [429, "RateLimitedError"],
        ] as const) {
          yield* http.respond(
            ScriptedResponse.Response({
              status,
              headers: { "retry-after": "2", "set-cookie": "private" },
              body: "diagnostic",
            }),
          );
          const error = yield* operation.pipe(
            Effect.flip,
            Effect.provide(Filtered.pipe(Layer.provide(http.layer))),
          );
          expect(error).toHaveProperty("_tag", tag);
          expect(error.response?.url).toBe("https://streams.test/orders");
          expect(error.response?.headers["set-cookie"]).toBe("[REDACTED]");
          expect(Object.isFrozen(error.response)).toBe(true);
        }
      }
      expect(yield* Queue.size(http.requests)).toBe(12);
    }),
  );

  it.effect("honors filtered Retry-After for lifecycle and ordinary writes including 501", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/orders",
        contentType: "text/plain",
      });
      const layer = Filtered.pipe(Layer.provide(http.layer));
      for (const status of [429, 503, 307, 501]) {
        yield* http.respond(ScriptedResponse.Response({ status, headers: { "retry-after": "2" } }));
        yield* http.respond(ScriptedResponse.Response({ status: 204, headers: {} }));
        const fiber = yield* client.delete.pipe(Effect.provide(layer), Effect.forkChild);
        yield* Queue.take(http.requests);
        yield* TestClock.adjust("1 second");
        expect(yield* Queue.size(http.requests)).toBe(0);
        yield* TestClock.adjust("1 second");
        yield* Fiber.join(fiber);
        yield* Queue.take(http.requests);
      }
      for (const status of [429, 503, 307, 501]) {
        yield* http.respond(ScriptedResponse.Response({ status, headers: { "retry-after": "2" } }));
        yield* http.respond(
          ScriptedResponse.Response({ status: 204, headers: { "stream-next-offset": "tail" } }),
        );
        const run = yield* client
          .append({ value: "private" })
          .pipe(Effect.provide(layer), Effect.forkChild);
        yield* Queue.take(http.requests);
        yield* TestClock.adjust("1999 millis");
        expect(yield* Queue.size(http.requests)).toBe(0);
        yield* TestClock.adjust("1 millis");
        expect(yield* Fiber.join(run)).toHaveProperty("offset", "tail");
        yield* Queue.take(http.requests);
      }
    }),
  );

  it.effect("keeps filtered response bodies in scope until captured then releases them", () =>
    Effect.gen(function* () {
      const signals: Array<AbortSignal> = [];
      const http = HttpClient.make((request, _url, signal) =>
        Effect.sync(() => {
          signals.push(signal);
          return HttpClientResponse.fromWeb(request, new Response("diagnostic", { status: 401 }));
        }),
      ).pipe(HttpClient.filterStatusOk);
      const client = yield* DurableStreamsClient.make({ url: "https://streams.test/orders" });
      for (const operation of [client.head, client.delete]) {
        yield* operation.pipe(
          Effect.catchTag("UnauthorizedError", (error) =>
            Effect.sync(() => {
              expect(error.response.body).toEqual(
                ErrorResponseBody.cases.Bytes.make({
                  value: new TextEncoder().encode("diagnostic"),
                  truncated: false,
                  contentType: "text/plain;charset=UTF-8",
                }),
              );
            }),
          ),
          Effect.provideService(HttpClient.HttpClient, http),
        );
      }
      expect(signals).toHaveLength(2);
      expect(signals.every((signal) => signal.aborted)).toBe(true);
    }),
  );

  it.effect("captures issue and path classifications with operation context without secrets", () =>
    Effect.gen(function* () {
      const logs: Array<string> = [];
      const http = yield* makeScriptedHttpClient;
      yield* Effect.gen(function* () {
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/orders?token=SECRET",
          schema: Schema.Record(
            Schema.String,
            Schema.String.check(
              Schema.makeFilter((value) => value.length >= 20 || "SECRET_MESSAGE"),
            ),
          ),
        });
        for (const operation of [
          client.create({ value: { SECRET_KEY: "SECRET_VALUE" } }).pipe(Effect.as(undefined)),
          client.append({ value: { SECRET_KEY: "SECRET_VALUE" } }).pipe(Effect.as(undefined)),
          client.close({ value: { SECRET_KEY: "SECRET_VALUE" } }).pipe(Effect.as(undefined)),
          client.create({ contentType: "SECRET\r\n" }).pipe(Effect.as(undefined)),
          client.close({ seq: "SECRET\r\n" }).pipe(Effect.as(undefined)),
        ]) {
          expect(
            yield* operation.pipe(
              Effect.flip<
                void,
                CreateError | CloseError | DeleteError | HeadError,
                HttpClient.HttpClient
              >,
            ),
          ).toHaveProperty("_tag", "PayloadEncodeError");
        }
        expect(yield* Queue.size(http.requests)).toBe(0);
        for (const [operation, status] of [
          [client.head.pipe(Effect.as(undefined)), 200],
          [client.connect.pipe(Effect.as(undefined)), 200],
          [client.create({}).pipe(Effect.as(undefined)), 201],
          [
            client
              .append({ value: { valid: "a sufficiently long string" } })
              .pipe(Effect.as(undefined)),
            204,
          ],
        ] as const) {
          yield* http.respond(
            ScriptedResponse.Response({
              status,
              headers: {
                "content-type": "application/json",
                "stream-next-offset": "SECRET INTERNAL",
              },
            }),
          );
          expect(
            yield* operation.pipe(
              Effect.flip<
                void,
                CreateError | CloseError | DeleteError | HeadError,
                HttpClient.HttpClient
              >,
            ),
          ).toHaveProperty("_tag", "ProtocolViolationError");
        }
      }).pipe(
        Effect.provide(
          Layer.merge(
            http.layer,
            Logger.layer([
              Logger.make(({ message }) => {
                logs.push(inspect(message, { depth: 10 }));
              }),
            ]),
          ),
        ),
      );
      const output = logs.join("\n");
      expect(output).not.toContain("SECRET");
      expect(output).toContain("Filter");
      expect(output).toContain("[key]");
      expect(output).toContain("stream-next-offset");
      for (const operation of ["create", "append", "close", "head", "connect"])
        expect(output).toContain(`operation: '${operation}'`);
    }),
  );
});
