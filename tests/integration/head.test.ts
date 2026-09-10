import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { FetchHttpClient, HttpClient } from "effect/unstable/http";
import { DurableStreamsClient, StreamMetadata } from "../../src/index.js";
import { acquireDurableStreamServer } from "../support/server.js";

describe("reference server HEAD/connect", () => {
  it.effect("inspects an existing stream through the real HTTP boundary", () =>
    Effect.gen(function* () {
      const { baseUrl } = yield* acquireDurableStreamServer;
      const http = yield* HttpClient.HttpClient;
      const created = yield* http.put(`${baseUrl}/orders`, {
        headers: {
          "content-type": "application/json",
          "stream-closed": "true",
          "stream-ttl": "3600",
        },
      });
      expect(created.status).toBe(201);
      const client = yield* DurableStreamsClient.make({
        url: `${baseUrl}/orders`,
      });
      const metadata = yield* client.head;
      expect(metadata).toMatchObject({
        contentType: "application/json",
        closed: true,
        ttlSeconds: 3600,
      });
      expect(metadata).toHaveProperty("offset", created.headers["stream-next-offset"]);
      expect(yield* client.connect).toEqual(metadata);
      expect((yield* http.head(`${baseUrl}/orders`)).headers["stream-closed"]).toBe("true");
    }).pipe(Effect.provide(FetchHttpClient.layer)),
  );

  it.effect("returns Missing for both HEAD and connect without creating a stream", () =>
    Effect.gen(function* () {
      const { server, baseUrl } = yield* acquireDurableStreamServer;
      const client = yield* DurableStreamsClient.make({ url: `${baseUrl}/missing` });
      expect(yield* client.head).toEqual(StreamMetadata.cases.Missing.make({}));
      expect(yield* client.connect).toEqual(StreamMetadata.cases.Missing.make({}));
      expect(server.store.get("/missing")).toBeUndefined();
    }).pipe(Effect.provide(FetchHttpClient.layer)),
  );

  it.effect("closes the random-port listener when its scope ends", () =>
    Effect.gen(function* () {
      const baseUrl = yield* Effect.scoped(
        Effect.gen(function* () {
          const fixture = yield* acquireDurableStreamServer;
          const client = yield* DurableStreamsClient.make({ url: `${fixture.baseUrl}/missing` });
          expect(yield* client.head).toEqual(StreamMetadata.cases.Missing.make({}));
          return fixture.baseUrl;
        }),
      );
      const client = yield* DurableStreamsClient.make({ url: `${baseUrl}/missing` });
      expect(
        yield* client.head.pipe(
          Effect.catchTag("StreamUnavailableError", () => Effect.succeed("listener stopped")),
        ),
      ).toBe("listener stopped");
    }).pipe(Effect.provide(FetchHttpClient.layer)),
  );
});
