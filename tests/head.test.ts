import { describe, expect, it } from "@effect/vitest";
import {
  Clock,
  DateTime,
  Duration,
  Effect,
  Fiber,
  Inspectable,
  Layer,
  Logger,
  Option,
  Queue,
  Ref,
  Schema,
} from "effect";
import { DurableStreamsClient, ErrorResponseBody, StreamMetadata } from "../src/index.ts";
import { makeScriptedHttpClient, ScriptedResponse } from "./support/http-client.ts";

describe("HEAD and connect", () => {
  it.effect("connect validates discovered JSON for schemas while head only inspects", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/orders",
        schema: Schema.String,
      });
      for (const contentType of [
        "text/plain",
        "application/json",
        "Application/JSON; charset=utf-8",
      ]) {
        const reply = ScriptedResponse.Response({
          status: 200,
          headers: { "content-type": contentType, "stream-next-offset": "opaque" },
        });
        yield* http.respond(reply);
        expect(yield* client.head.pipe(Effect.provide(http.layer))).toMatchObject({ contentType });
        yield* http.respond(reply);
        const connected = yield* client.connect.pipe(
          Effect.catchTag("ProtocolViolationError", (error) =>
            Effect.sync(() => {
              expect(error.response?.status).toBe(200);
              return "incompatible";
            }),
          ),
          Effect.provide(http.layer),
        );
        if (contentType === "text/plain") expect(connected).toBe("incompatible");
        else expect(connected).toMatchObject({ contentType });
      }
      yield* http.respond(ScriptedResponse.Response({ status: 404, headers: {} }));
      expect(yield* client.connect.pipe(Effect.provide(http.layer))).toEqual(
        StreamMetadata.cases.Missing.make({}),
      );
    }),
  );

  it.effect("rejects impossible expiry dates without rejecting leap days or timezone offsets", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({ url: "https://streams.test/orders" });
      const cases = [
        ["2030-02-31T00:00:00Z", false],
        ["2030-02-29T00:00:00Z", false],
        ["2100-02-29T00:00:00Z", false],
        ["2030-04-31T00:00:00+05:30", false],
        ["2030-01-01T24:00:00Z", false],
        ["2032-02-29T00:00:00Z", true],
        ["2000-02-29T23:59:59.123-05:00", true],
        ["2030-03-01T00:00:00+05:30", true],
      ] as const;
      for (const [expiresAt, valid] of cases) {
        yield* http.respond(
          ScriptedResponse.Response({
            status: 200,
            headers: {
              "content-type": "text/plain",
              "stream-next-offset": "opaque",
              "stream-expires-at": expiresAt,
            },
          }),
        );
        const result = yield* client.head.pipe(
          Effect.map(() => true),
          Effect.catchTag("ProtocolViolationError", () => Effect.succeed(false)),
          Effect.provide(http.layer),
        );
        expect(result).toBe(valid);
      }
    }),
  );

  it.effect("parses complete metadata and preserves opaque offsets", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/orders?token=secret",
      });
      yield* http.respond(
        ScriptedResponse.Response({
          status: 200,
          headers: {
            "Content-Type": "application/json",
            "Stream-Next-Offset": "AbC_00-+:~",
            "Stream-Closed": "true",
            "Stream-TTL": "3600",
            "Stream-Expires-At": "2030-01-01T00:00:00Z",
            ETag: '"v1"',
            "Cache-Control": "no-store",
          },
        }),
      );
      expect(yield* client.head.pipe(Effect.provide(http.layer))).toEqual(
        StreamMetadata.cases.Existing.make({
          contentType: "application/json",
          offset: "AbC_00-+:~",
          closed: true,
          ttlSeconds: 3600,
          expiresAt: "2030-01-01T00:00:00Z",
          etag: '"v1"',
          cacheControl: "no-store",
        }),
      );
      const request = yield* Queue.take(http.requests);
      expect(request.method).toBe("HEAD");
      expect(request.url).toBe("https://streams.test/orders?token=secret");
    }),
  );

  it.effect("classifies status failures without exposing raw HTTP failures", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({ url: "https://streams.test/orders" });
      const cases = [
        [401, "unauthorized"],
        [403, "forbidden"],
        [410, "gone"],
        [429, "limited"],
        [503, "unavailable"],
        [302, "protocol"],
      ] as const;
      for (const [status, expected] of cases) {
        yield* http.respond(ScriptedResponse.Response({ status, headers: {} }));
        const result = yield* client.head.pipe(
          Effect.catchTags({
            UnauthorizedError: () => Effect.succeed("unauthorized"),
            ForbiddenError: () => Effect.succeed("forbidden"),
            StreamGoneError: () => Effect.succeed("gone"),
            RateLimitedError: () => Effect.succeed("limited"),
            StreamUnavailableError: () => Effect.succeed("unavailable"),
            ProtocolViolationError: () => Effect.succeed("protocol"),
          }),
          Effect.provide(http.layer),
        );
        expect(result).toBe(expected);
      }
      yield* http.respond(ScriptedResponse.TransportFailure());
      expect(
        yield* client.connect.pipe(
          Effect.catchTag("StreamUnavailableError", () => Effect.succeed("transport")),
          Effect.provide(http.layer),
        ),
      ).toBe("transport");
    }),
  );

  it.effect("rejects missing and malformed protocol headers", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({ url: "https://streams.test/orders" });
      const cases: ReadonlyArray<Readonly<Record<string, string>>> = [
        {},
        { "stream-next-offset": "now", "content-type": "text/plain" },
        { "stream-next-offset": "-1", "content-type": "text/plain" },
        { "stream-next-offset": "opaque/invalid", "content-type": "text/plain" },
        { "stream-next-offset": "opaque", "content-type": "text/plain", "stream-closed": "yes" },
        { "stream-next-offset": "opaque", "content-type": "text/plain", "stream-ttl": "01" },
        {
          "stream-next-offset": "opaque",
          "content-type": "text/plain",
          "stream-expires-at": "bad",
        },
      ];
      for (const headers of cases) {
        yield* http.respond(ScriptedResponse.Response({ status: 200, headers }));
        expect(
          yield* client.head.pipe(
            Effect.catchTag("ProtocolViolationError", (error) =>
              Effect.succeed(error.response?.status),
            ),
            Effect.provide(http.layer),
          ),
        ).toBe(200);
      }
    }),
  );

  it.effect("snapshots bounded bodies, normalized redacted headers, and retry metadata", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({
        url: "https://user:password@streams.test/orders?token=secret#private",
      });
      yield* http.respond(
        ScriptedResponse.Response({
          status: 429,
          headers: {
            "Set-Cookie": "secret",
            "X-Extension": "kept",
            "Content-Type": "text/plain",
            "Retry-After": "3",
          },
          body: "x".repeat(70 * 1024),
        }),
      );
      const result = yield* client.head.pipe(
        Effect.catchTag("RateLimitedError", (error) =>
          Effect.sync(() => {
            expect(error.response.url).toBe("https://streams.test/orders");
            expect(error.response.headers["set-cookie"]).toBe("[REDACTED]");
            expect(error.response.headers["x-extension"]).toBe("kept");
            expect(error.message).not.toContain("secret");
            expect(error.retryAfter).toEqual(Duration.seconds(3));
            expect(error.response.body).toMatchObject({
              truncated: true,
              contentType: "text/plain",
            });
            expect(error.response.body).toHaveProperty(
              "value",
              new Uint8Array(64 * 1024).fill(120),
            );
            return "limited";
          }),
        ),
        Effect.provide(http.layer),
      );
      expect(result).toBe("limited");
    }),
  );

  it.effect("interrupts an in-flight request without converting interruption to an SDK error", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({ url: "https://streams.test/orders" });
      const fiber = yield* client.head.pipe(Effect.provide(http.layer), Effect.forkChild);
      yield* Queue.take(http.requests);
      expect(yield* Ref.get(http.active)).toBe(1);
      yield* Fiber.interrupt(fiber);
      expect(yield* Ref.get(http.active)).toBe(0);
    }),
  );

  it.effect("distinguishes empty, exact-cap and truncated diagnostic bodies", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({ url: "https://streams.test/orders" });
      for (const size of [0, 64 * 1024, 64 * 1024 + 1]) {
        yield* http.respond(
          ScriptedResponse.Response({ status: 403, headers: {}, body: "x".repeat(size) }),
        );
        yield* client.head.pipe(
          Effect.catchTag("ForbiddenError", (error) =>
            Effect.sync(() => {
              expect(Object.isFrozen(error.response)).toBe(true);
              expect(Object.isFrozen(error.response.headers)).toBe(true);
              expect(Object.isFrozen(error.response.body)).toBe(true);
              ErrorResponseBody.match(error.response.body, {
                Empty: () => {
                  expect(size).toBe(0);
                },
                Bytes: (body) => {
                  expect(body.value).toHaveLength(Math.min(size, 64 * 1024));
                  expect(body.truncated).toBe(size > 64 * 1024);
                  body.value.fill(0);
                  new Uint8Array(body.value.buffer).fill(1);
                  body.value.subarray().fill(2);
                  expect(body.value).toEqual(new Uint8Array(Math.min(size, 64 * 1024)).fill(120));
                  expect(Schema.encodeSync(ErrorResponseBody)(body)).toMatchObject({
                    value: body.value,
                  });
                },
              });
            }),
          ),
          Effect.provide(http.layer),
        );
      }
    }),
  );

  it.effect("parses HTTP-date Retry-After and omits malformed values without retrying", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({ url: "https://streams.test/orders" });
      const now = yield* Clock.currentTimeMillis;
      for (const retryAfter of [
        DateTime.toDateUtc(DateTime.makeUnsafe(now + 5000)).toUTCString(),
        "nonsense",
        "-1",
        "1.5",
      ]) {
        yield* http.respond(
          ScriptedResponse.Response({ status: 429, headers: { "retry-after": retryAfter } }),
        );
        yield* client.head.pipe(
          Effect.catchTag("RateLimitedError", (error) =>
            Effect.sync(() => {
              expect(error.retryAfter).toEqual(
                retryAfter.endsWith("GMT") ? Duration.seconds(5) : undefined,
              );
            }),
          ),
          Effect.provide(http.layer),
        );
        expect((yield* Queue.take(http.requests)).method).toBe("HEAD");
        expect(yield* Queue.poll(http.requests)).toEqual(Option.none());
      }
    }),
  );

  it.effect(
    "captures safe rejection diagnostics without logging response bodies or credentials",
    () =>
      Effect.gen(function* () {
        const http = yield* makeScriptedHttpClient;
        const logs: Array<string> = [];
        const logger = Logger.layer([
          Logger.make(({ message }) => {
            logs.push(Inspectable.toStringUnknown(message));
          }),
        ]);
        const client = yield* DurableStreamsClient.make({
          url: "https://username:secret-password@streams.test/orders?token=secret-token#secret-fragment",
        });
        yield* http.respond(
          ScriptedResponse.Response({
            status: 401,
            headers: { "set-cookie": "secret-cookie" },
            body: "secret-body",
          }),
        );
        yield* client.head.pipe(
          Effect.catchTag("UnauthorizedError", () => Effect.succeed("authentication required")),
          Effect.provide(Layer.merge(http.layer, logger)),
        );
        expect(logs.join("\n")).toContain("Stream request rejected");
        expect(logs.join("\n")).toContain("https://streams.test/orders");
        expect(logs.join("\n")).not.toContain("secret-");
        expect(logs.join("\n")).not.toContain("username");
      }),
  );
});
