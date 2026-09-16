import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { FetchHttpClient, HttpClient } from "effect/unstable/http";
import { DurableStreamsClient, StreamLifetime, StreamMetadata } from "../../src/index";
import { acquireDurableStreamServer } from "../support/server";

describe("reference server lifecycle round trips", () => {
  it.effect("creates, appends, reads back, closes and deletes byte/text/JSON streams", () =>
    Effect.gen(function* () {
      const { baseUrl } = yield* acquireDurableStreamServer;
      const http = yield* HttpClient.HttpClient;
      const cases = [
        {
          path: "/binary",
          contentType: "application/octet-stream",
          initial: new Uint8Array([0, 255]),
          value: new Uint8Array([10, 13]),
          expected: new Uint8Array([0, 255, 10, 13]),
        },
        {
          path: "/text",
          contentType: "text/plain",
          initial: "hé",
          value: "llo",
          expected: new TextEncoder().encode("héllo"),
        },
        {
          path: "/json",
          contentType: "application/json",
          initial: { id: 1 },
          value: [2, 3],
          expected: new TextEncoder().encode('[{"id":1},[2,3]]'),
        },
      ];
      for (const entry of cases) {
        const client = yield* DurableStreamsClient.make({
          url: baseUrl + entry.path,
          batching: false,
        });
        const create = yield* client.create({
          contentType: entry.contentType,
          value: entry.initial,
          lifetime: StreamLifetime.cases.Ttl.make({ ttlSeconds: 3600 }),
        });
        expect(create.status).toBe(201);
        expect(
          (yield* client.create({
            contentType: entry.contentType,
            lifetime: StreamLifetime.cases.Ttl.make({ ttlSeconds: 3600 }),
          })).status,
        ).toBe(200);
        const appended = yield* client.append({ value: entry.value });
        expect(appended.offset).not.toBe(create.offset);
        const response = yield* http.get(baseUrl + entry.path);
        expect(new Uint8Array(yield* response.arrayBuffer)).toEqual(entry.expected);
        expect(yield* client.head).toMatchObject({
          contentType: entry.contentType,
          offset: appended.offset,
          closed: false,
          ttlSeconds: 3600,
        });
        const closed = yield* client.close({});
        expect(closed.finalOffset).toBe(appended.offset);
        expect(yield* client.close({})).toEqual(closed);
        yield* client.append({ value: entry.value }).pipe(
          Effect.catchTag("StreamClosedError", (error) =>
            Effect.sync(() => {
              expect(error.finalOffset).toBe(closed.finalOffset);
            }),
          ),
        );
        yield* client.delete;
        expect(yield* client.head).toEqual(StreamMetadata.cases.Missing.make({}));
      }
    }).pipe(Effect.provide(FetchHttpClient.layer)),
  );

  it.effect("supports typed initial/final values, create-closed and conflict recovery by tag", () =>
    Effect.gen(function* () {
      const { baseUrl } = yield* acquireDurableStreamServer;
      const http = yield* HttpClient.HttpClient;
      const client = yield* DurableStreamsClient.make({
        url: `${baseUrl}/typed`,
        schema: Schema.Struct({ id: Schema.Int }),
      });
      yield* client.create({ value: { id: 1 } });
      const closed = yield* client.close({ value: { id: 2 } });
      const response = yield* http.get(`${baseUrl}/typed`);
      expect(yield* response.json).toEqual([{ id: 1 }, { id: 2 }]);
      expect(yield* client.head).toMatchObject({ closed: true, offset: closed.finalOffset });
      expect(
        yield* client
          .create({})
          .pipe(Effect.catchTag("CreateConflictError", () => Effect.succeed("conflict"))),
      ).toBe("conflict");
      const single = yield* DurableStreamsClient.make({
        url: `${baseUrl}/single`,
        contentType: "text/plain",
      });
      yield* single.create({
        value: "done",
        closed: true,
        lifetime: StreamLifetime.cases.ExpiresAt.make({ expiresAt: "2035-01-01T00:00:00Z" }),
      });
      expect(yield* (yield* http.get(`${baseUrl}/single`)).text).toBe("done");
      expect(yield* single.head).toMatchObject({ closed: true, expiresAt: "2035-01-01T00:00:00Z" });
    }).pipe(Effect.provide(FetchHttpClient.layer)),
  );
});
