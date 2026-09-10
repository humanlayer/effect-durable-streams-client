import { describe, expect, it } from "@effect/vitest";
import { Duration, Effect, Queue } from "effect";
import type { HttpClient } from "effect/unstable/http";
import {
  DurableStreamsClient,
  ErrorResponseBody,
  type AppendError,
  type CreateError,
  type CloseError,
  type DeleteError,
  type AppendResult,
  type CreateResult,
  type CloseResult,
} from "../src/index";
import { makeScriptedHttpClient, ScriptedResponse } from "./support/http-client";

describe("mutation error contracts", () => {
  it.effect("classifies ordinary failures when retries are explicitly disabled", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/orders?secret=private",
        contentType: "text/plain",
        backoffOptions: { maxRetries: 0 },
      });
      for (const [status, tag] of [
        [400, "InvalidRequestError"],
        [401, "UnauthorizedError"],
        [403, "ForbiddenError"],
        [404, "StreamNotFoundError"],
        [405, "OperationNotSupportedError"],
        [408, "AppendOutcomeUnknownError"],
        [409, "AppendConflictError"],
        [410, "StreamGoneError"],
        [413, "PayloadTooLargeError"],
        [429, "RateLimitedError"],
        [500, "AppendOutcomeUnknownError"],
        [501, "OperationNotSupportedError"],
        [503, "AppendOutcomeUnknownError"],
        [302, "AppendOutcomeUnknownError"],
      ] as const) {
        yield* http.respond(
          ScriptedResponse.Response({ status, headers: {}, body: "private response" }),
        );
        const error = yield* client
          .append({ value: "private request" })
          .pipe(Effect.flip, Effect.provide(http.layer));
        expect(error).toMatchObject({
          _tag: tag,
          response: { status, url: "https://streams.test/orders" },
        });
        yield* Queue.take(http.requests);
        expect(yield* Queue.size(http.requests)).toBe(0);
      }
      yield* http.respond(ScriptedResponse.TransportFailure());
      expect(
        yield* client.close({ value: "final" }).pipe(Effect.flip, Effect.provide(http.layer)),
      ).toHaveProperty("_tag", "AppendOutcomeUnknownError");
      yield* Queue.take(http.requests);
      expect(yield* Queue.size(http.requests)).toBe(0);
    }),
  );

  it.effect("projects closed offset and rate metadata with immutable bounded snapshots", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({
        url: "https://user:password@streams.test/orders?token=private",
        contentType: "text/plain",
        backoffOptions: { maxRetries: 0 },
      });
      yield* http.respond(
        ScriptedResponse.Response({
          status: 409,
          headers: {
            "Stream-Closed": "true",
            "Stream-Next-Offset": "FINAL",
            "Set-Cookie": "private",
            "X-Custom": "diagnostic",
          },
          body: "x".repeat(70000),
        }),
      );
      yield* client.append({ value: "one" }).pipe(
        Effect.catchTag("StreamClosedError", (error) =>
          Effect.sync(() => {
            expect(error.finalOffset).toBe("FINAL");
            expect(error.response.url).toBe("https://streams.test/orders");
            expect(error.response.headers).toMatchObject({
              "set-cookie": "[REDACTED]",
              "x-custom": "diagnostic",
            });
            expect(Object.isFrozen(error.response)).toBe(true);
            expect(Object.isFrozen(error.response.headers)).toBe(true);
            expect(Object.isFrozen(error.response.body)).toBe(true);
            expect(ErrorResponseBody.guards.Bytes(error.response.body)).toBe(true);
            if (ErrorResponseBody.guards.Bytes(error.response.body)) {
              expect(error.response.body.value.length).toBe(65536);
              expect(error.response.body.truncated).toBe(true);
              error.response.body.value[0] = 0;
              expect(error.response.body.value[0]).toBe(120);
            }
          }),
        ),
        Effect.provide(http.layer),
      );
      yield* http.respond(
        ScriptedResponse.Response({ status: 429, headers: { "Retry-After": "1.5" } }),
      );
      yield* client.append({ value: "two" }).pipe(
        Effect.catchTag("RateLimitedError", (error) =>
          Effect.sync(() => {
            expect(error.retryAfter).toEqual(Duration.millis(1500));
          }),
        ),
        Effect.provide(http.layer),
      );
    }),
  );

  it.effect("classifies create/delete/close failures and malformed success metadata", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/orders",
        backoffOptions: { maxRetries: 0 },
      });
      const cases: ReadonlyArray<
        readonly [
          Effect.Effect<
            void | CreateResult | AppendResult | CloseResult,
            CreateError | AppendError | CloseError | DeleteError,
            HttpClient.HttpClient
          >,
          number,
          string,
        ]
      > = [
        [client.create({}), 409, "CreateConflictError"],
        [client.create({}), 413, "PayloadTooLargeError"],
        [client.delete, 404, "StreamNotFoundError"],
        [client.delete, 501, "OperationNotSupportedError"],
        [client.close({}), 501, "OperationNotSupportedError"],
        [client.close({}), 503, "StreamUnavailableError"],
        [client.create({}), 503, "StreamUnavailableError"],
        [client.delete, 429, "RateLimitedError"],
        [client.create({}), 204, "ProtocolViolationError"],
        [client.create({}), 201, "ProtocolViolationError"],
        [client.append({ value: "one" }), 204, "ProtocolViolationError"],
        [client.close({}), 204, "ProtocolViolationError"],
        [client.delete, 200, "ProtocolViolationError"],
      ];
      for (const [operation, status, tag] of cases) {
        yield* http.respond(ScriptedResponse.Response({ status, headers: {} }));
        expect(yield* operation.pipe(Effect.flip, Effect.provide(http.layer))).toMatchObject({
          _tag: tag,
        });
      }
    }),
  );
});
