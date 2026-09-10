import { describe, expect, it } from "@effect/vitest";
import { Effect, Exit, Fiber, Match, Queue, Ref } from "effect";
import { DurableStreamsClient, StreamLifetime } from "../src/index.js";
import { makeScriptedHttpClient, ScriptedResponse } from "./support/http-client.js";

describe("lifecycle requests", () => {
  it.effect("rejects malformed lifetimes and retry settings locally", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({ url: "https://streams.test/orders" });
      const lifetime = StreamLifetime.cases.ExpiresAt.make({ expiresAt: "2030-01-01T00:00:00Z" });
      for (const expiresAt of ["2030-02-30T00:00:00Z", "not-a-date", "2030-01-01"]) {
        expect(
          yield* client
            .create({ lifetime: { ...lifetime, expiresAt } })
            .pipe(Effect.flip, Effect.provide(http.layer)),
        ).toHaveProperty("_tag", "PayloadEncodeError");
      }
      expect(yield* Queue.size(http.requests)).toBe(0);
      for (const initialDelay of [0, -1, NaN, Infinity]) {
        expect(
          yield* DurableStreamsClient.make({
            url: "https://streams.test/orders",
            backoffOptions: { initialDelay },
          }).pipe(Effect.flip),
        ).toHaveProperty("_tag", "InvalidDurableStreamsConfigError");
      }
      for (const maxRetries of [-1, 1.5, NaN]) {
        expect(
          yield* DurableStreamsClient.make({
            url: "https://streams.test/orders",
            backoffOptions: { maxRetries },
          }).pipe(Effect.flip),
        ).toHaveProperty("_tag", "InvalidDurableStreamsConfigError");
      }
      yield* DurableStreamsClient.make({
        url: "https://streams.test/orders",
        backoffOptions: { maxRetries: Infinity },
      });
    }),
  );

  it.effect(
    "interrupts an active ordinary append without converting interruption or replaying",
    () =>
      Effect.gen(function* () {
        const http = yield* makeScriptedHttpClient;
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/orders",
          contentType: "text/plain",
        });
        const fiber = yield* client
          .append({ value: "data" })
          .pipe(Effect.provide(http.layer), Effect.forkChild);
        yield* Queue.take(http.requests);
        expect(yield* Ref.get(http.active)).toBe(1);
        yield* Fiber.interrupt(fiber);
        expect(Exit.hasInterrupts(yield* Fiber.await(fiber))).toBe(true);
        expect(yield* Ref.get(http.active)).toBe(0);
        expect(yield* Queue.size(http.requests)).toBe(0);
      }),
  );
  it.effect("constructs sorted PUT, discovers content type, closes atomically and deletes", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/orders?z=last",
        params: { a: "a b", m: "x/y" },
        headers: { "X-Extension": "yes" },
        batching: false,
      });
      yield* http.respond(
        ScriptedResponse.Response({
          status: 201,
          headers: { "content-type": "application/json", "stream-next-offset": "initial" },
        }),
      );
      expect(
        yield* client
          .create({
            contentType: "application/json",
            value: { id: 1 },
            lifetime: StreamLifetime.cases.Ttl.make({ ttlSeconds: 3600 }),
          })
          .pipe(Effect.provide(http.layer)),
      ).toEqual({ status: 201, contentType: "application/json", offset: "initial", closed: false });
      const create = yield* Queue.take(http.requests);
      expect(create.method).toBe("PUT");
      expect(create.url).toBe("https://streams.test/orders?a=a+b&m=x%2Fy&z=last");
      expect(create.headers).toMatchObject({
        "content-type": "application/json",
        "stream-ttl": "3600",
        "x-extension": "yes",
      });
      expect(
        Match.value(create.body).pipe(
          Match.tag("Uint8Array", (body) => new TextDecoder().decode(body.body)),
          Match.orElse(() => "missing"),
        ),
      ).toBe('[{"id":1}]');
      yield* http.respond(
        ScriptedResponse.Response({
          status: 204,
          headers: { "stream-next-offset": "final", "stream-closed": "true" },
        }),
      );
      expect(
        yield* client.close({ value: [2, 3], seq: "009" }).pipe(Effect.provide(http.layer)),
      ).toEqual({ finalOffset: "final" });
      const close = yield* Queue.take(http.requests);
      expect(close.method).toBe("POST");
      expect(close.headers).toMatchObject({
        "content-type": "application/json",
        "stream-seq": "009",
        "stream-closed": "true",
      });
      expect(
        Match.value(close.body).pipe(
          Match.tag("Uint8Array", (body) => new TextDecoder().decode(body.body)),
          Match.orElse(() => "missing"),
        ),
      ).toBe("[[2,3]]");
      yield* http.respond(ScriptedResponse.Response({ status: 204, headers: {} }));
      yield* client.delete.pipe(Effect.provide(http.layer));
      const deleted = yield* Queue.take(http.requests);
      expect(deleted.method).toBe("DELETE");
      expect(deleted.url).toBe(create.url);
      expect(deleted.body).toHaveProperty("_tag", "Empty");
    }),
  );

  it.effect("sends absolute expiry and initially closed state; close-only has no payload", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({ url: "https://streams.test/orders" });
      yield* http.respond(
        ScriptedResponse.Response({
          status: 200,
          headers: {
            "content-type": "application/octet-stream",
            "stream-next-offset": "zero",
            "stream-closed": "true",
          },
        }),
      );
      yield* client
        .create({
          closed: true,
          lifetime: StreamLifetime.cases.ExpiresAt.make({ expiresAt: "2030-01-01T00:00:00Z" }),
        })
        .pipe(Effect.provide(http.layer));
      const request = yield* Queue.take(http.requests);
      expect(request.headers).toMatchObject({
        "stream-expires-at": "2030-01-01T00:00:00Z",
        "stream-closed": "true",
      });
      expect(request.headers["stream-ttl"]).toBeUndefined();
      yield* http.respond(
        ScriptedResponse.Response({
          status: 204,
          headers: { "stream-next-offset": "zero", "stream-closed": "true" },
        }),
      );
      expect(yield* client.close({}).pipe(Effect.provide(http.layer))).toEqual({
        finalOffset: "zero",
      });
      const close = yield* Queue.take(http.requests);
      expect(close.body).toHaveProperty("_tag", "Empty");
      expect(close.headers["content-type"]).toBeUndefined();
    }),
  );

  it.effect("protects protocol extensions before dispatch", () =>
    Effect.gen(function* () {
      const inputs: ReadonlyArray<Readonly<Record<string, string>>> = [
        { "Stream-Closed": "true" },
        { "PRODUCER-ID": "p" },
        { "Content-Type": "text/plain" },
        { "X-Bad": "a\r\nb" },
      ];
      for (const headers of inputs) {
        expect(
          yield* DurableStreamsClient.make({ url: "https://streams.test/orders", headers }).pipe(
            Effect.flip,
          ),
        ).toHaveProperty("_tag", "InvalidDurableStreamsConfigError");
      }
      for (const key of ["offset", "live", "cursor"]) {
        expect(
          yield* DurableStreamsClient.make({
            url: "https://streams.test/orders",
            params: { [key]: "bad" },
          }).pipe(Effect.flip),
        ).toHaveProperty("_tag", "InvalidDurableStreamsConfigError");
      }
    }),
  );
});
