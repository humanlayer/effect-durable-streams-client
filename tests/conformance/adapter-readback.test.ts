import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Ref, Schema, SchemaGetter } from "effect";
import { FetchHttpClient, HttpClient } from "effect/unstable/http";
import { acquireDurableStreamServer } from "../support/server";
import { AdapterState } from "./adapter-state";
import { handleCommand } from "./adapter";
import { DurableStreamsClient } from "../../src/index";

const AdapterLive = Layer.merge(
  FetchHttpClient.layer,
  Layer.effect(AdapterState, AdapterState.make),
);

describe("adapter wire-body translation", () => {
  it.effect("custom encoding transforms preserve each logical initial message atomically", () =>
    Effect.gen(function* () {
      const { baseUrl } = yield* acquireDurableStreamServer;
      const http = yield* HttpClient.HttpClient;
      const schema = Schema.Json.pipe(
        Schema.decodeTo(Schema.String, {
          decode: SchemaGetter.transform(() => "initial"),
          encode: SchemaGetter.transform(() => [1, 2]),
        }),
      );
      const client = yield* DurableStreamsClient.make({ url: baseUrl + "/codec", schema });
      yield* client.create({ values: ["initial", "initial"], closed: true });
      const response = yield* http.get(baseUrl + "/codec");
      expect(yield* response.json).toEqual([
        [1, 2],
        [1, 2],
      ]);
      expect(response.headers["stream-closed"]).toBe("true");
    }).pipe(Effect.provide(FetchHttpClient.layer)),
  );

  it.effect("normalizes JSON media types for create, append, binary append and close", () =>
    Effect.gen(function* () {
      const { baseUrl } = yield* acquireDurableStreamServer;
      const http = yield* HttpClient.HttpClient;
      yield* handleCommand({ type: "init", serverUrl: baseUrl });
      expect(
        yield* handleCommand({
          type: "create",
          path: "/mixed",
          contentType: "Application/JSON ; charset=utf-8",
          data: '{"id":1}',
        }),
      ).toMatchObject({ success: true });
      expect(yield* handleCommand({ type: "append", path: "/mixed", data: "[2,3]" })).toMatchObject(
        { success: true },
      );
      expect(
        yield* handleCommand({ type: "append", path: "/mixed", data: "NA==", binary: true }),
      ).toMatchObject({ success: true });
      expect(
        yield* handleCommand({
          type: "close",
          path: "/mixed",
          contentType: "APPLICATION/JSON; charset=utf-8",
          data: "[]",
        }),
      ).toMatchObject({ success: true });
      const response = yield* http.get(baseUrl + "/mixed");
      expect(yield* response.json).toEqual([{ id: 1 }, [2, 3], 4, []]);
      expect(yield* handleCommand({ type: "head", path: "/mixed" })).toMatchObject({
        streamClosed: true,
      });
    }).pipe(Effect.provide(AdapterLive)),
  );

  it.effect("translates initial envelopes with one atomic closed create", () =>
    Effect.gen(function* () {
      const { baseUrl } = yield* acquireDurableStreamServer;
      const http = yield* HttpClient.HttpClient;
      const requests = yield* Ref.make<ReadonlyArray<string>>([]);
      yield* handleCommand({ type: "init", serverUrl: baseUrl });
      for (const [index, data, expected] of [
        [0, "[]", []],
        [1, "[1]", [1]],
        [2, "[[1,2]]", [[1, 2]]],
        [3, "[null]", [null]],
        [4, "[1,2]", [1, 2]],
        [5, "[[1,2],[],[3]]", [[1, 2], [], [3]]],
      ] as const) {
        const path = `/closed-${index}`;
        const result = yield* handleCommand({
          type: "create",
          path,
          contentType: "Application/JSON; charset=utf-8",
          data,
          closed: true,
        }).pipe(
          Effect.provideService(
            HttpClient.HttpClient,
            http.pipe(
              HttpClient.mapRequestEffect((request) =>
                Ref.update(requests, (methods) => [...methods, request.method]).pipe(
                  Effect.as(request),
                ),
              ),
            ),
          ),
        );
        expect(result).toMatchObject({ success: true, status: 201 });
        const response = yield* http.get(baseUrl + path);
        expect(yield* response.json).toEqual(expected);
        expect(response.headers["stream-closed"]).toBe("true");
        expect(yield* handleCommand({ type: "append", path, data: "9" })).toMatchObject({
          errorCode: "STREAM_CLOSED",
        });
      }
      expect(yield* Ref.get(requests)).toEqual(["PUT", "PUT", "PUT", "PUT", "PUT", "PUT"]);
    }).pipe(Effect.provide(AdapterLive)),
  );

  it.effect("creates multiple initial messages in open and closed streams", () =>
    Effect.gen(function* () {
      const { baseUrl } = yield* acquireDurableStreamServer;
      const http = yield* HttpClient.HttpClient;
      yield* handleCommand({ type: "init", serverUrl: baseUrl });
      for (const closed of [false, true]) {
        const path = `/multiple-${closed}`;
        expect(
          yield* handleCommand({
            type: "create",
            path,
            contentType: "application/json",
            data: "[1,2]",
            closed,
          }),
        ).toMatchObject({
          success: true,
          type: "create",
          status: 201,
        });
        const response = yield* http.get(baseUrl + path);
        expect(yield* response.json).toEqual([1, 2]);
        expect(yield* handleCommand({ type: "head", path })).toMatchObject({
          status: 200,
          streamClosed: closed,
        });
      }
    }).pipe(Effect.provide(AdapterLive)),
  );

  it.effect(
    "defaults every create to octet-stream and forgets types only after successful deletion",
    () =>
      Effect.gen(function* () {
        const { baseUrl } = yield* acquireDurableStreamServer;
        const http = yield* HttpClient.HttpClient;
        const state = yield* AdapterState;
        const path = "/recreated";
        yield* handleCommand({ type: "init", serverUrl: baseUrl });
        expect(
          yield* handleCommand({ type: "create", path, contentType: "application/json" }),
        ).toMatchObject({ success: true });
        expect(yield* handleCommand({ type: "create", path })).toMatchObject({
          errorCode: "CONFLICT",
        });
        expect(yield* state.contentType({ path })).toBe("application/json");
        expect(yield* handleCommand({ type: "delete", path })).toMatchObject({ success: true });
        expect(yield* state.contentType({ path })).toBeUndefined();
        expect(yield* handleCommand({ type: "create", path, data: "[1,2]" })).toMatchObject({
          success: true,
        });
        expect(yield* handleCommand({ type: "append", path, data: "raw" })).toMatchObject({
          success: true,
        });
        const response = yield* http.get(baseUrl + path);
        expect(response.headers["content-type"]).toBe("application/octet-stream");
        expect(yield* response.text).toBe("[1,2]raw");
      }).pipe(Effect.provide(AdapterLive)),
  );
});
