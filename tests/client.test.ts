import { describe, expect, expectTypeOf, it } from "@effect/vitest";
import { Context, Effect, Layer, Option, Schema, SchemaGetter, Stream } from "effect";
import type { HttpClient } from "effect/unstable/http";
import {
  DurableStreamsClient,
  type DurableStreamsClientLayerConfig,
  StreamMetadata,
  type HeadError,
  type ReadError,
  type InvalidDurableStreamsConfigError,
} from "../src/index";
import { makeScriptedHttpClient, ScriptedResponse } from "./support/http-client";

describe("construction and API types", () => {
  it.effect("makes a cold client without HttpClient and preserves direct schema types", () =>
    Effect.gen(function* () {
      const Event = Schema.Struct({ orderId: Schema.String });
      const make: Effect.Effect<
        Effect.Success<ReturnType<typeof DurableStreamsClient.make<typeof Event>>>,
        InvalidDurableStreamsConfigError
      > = DurableStreamsClient.make({
        url: "https://streams.test/orders",
        contentType: "application/json",
        schema: Event,
      });
      const client = yield* make;
      const json: Stream.Stream<typeof Event.Type, ReadError, HttpClient.HttpClient> = client.json;
      const head: Effect.Effect<
        Effect.Success<typeof client.head>,
        HeadError,
        HttpClient.HttpClient
      > = client.head;
      expect(json).toBe(client.json);
      expect(head).toBe(client.head);
      expectTypeOf<Effect.Services<typeof client.head>>().toEqualTypeOf<HttpClient.HttpClient>();
      expectTypeOf<Stream.Success<typeof client.json>>().toEqualTypeOf<typeof Event.Type>();
      expect(yield* client.offset).toEqual(Option.none());
    }),
  );

  it.effect(
    "preserves serviceful schema configuration without requiring its services to construct",
    () =>
      Effect.gen(function* () {
        class DecodeService extends Context.Service<DecodeService, { readonly prefix: string }>()(
          "DecodeService",
        ) {}
        class EncodeService extends Context.Service<EncodeService, { readonly suffix: string }>()(
          "EncodeService",
        ) {}
        const codec = Schema.String.pipe(
          Schema.decodeTo(Schema.String, {
            decode: SchemaGetter.transformOrFail((value) =>
              DecodeService.pipe(Effect.map((service) => service.prefix + value)),
            ),
            encode: SchemaGetter.transformOrFail((value) =>
              EncodeService.pipe(Effect.map((service) => value + service.suffix)),
            ),
          }),
        );
        const construction = DurableStreamsClient.make({
          url: "https://streams.test/orders",
          schema: codec,
        });
        const client = yield* construction;
        expectTypeOf<Effect.Services<typeof construction>>().toEqualTypeOf<never>();
        expectTypeOf<typeof codec.EncodingServices>().toEqualTypeOf<EncodeService>();
        expectTypeOf<Stream.Success<typeof client.json>>().toEqualTypeOf<string>();
        const json: Stream.Stream<string, ReadError, DecodeService | HttpClient.HttpClient> =
          client.json;
        expect(json).toBe(client.json);
        expectTypeOf<Stream.Services<typeof client.json>>().toEqualTypeOf<
          DecodeService | HttpClient.HttpClient
        >();
      }),
  );

  it.effect("keeps the raw JSON default and consumes real payloads", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/orders",
        contentType: "application/json",
      });
      expectTypeOf<Stream.Success<typeof client.json>>().toEqualTypeOf<Schema.Json>();
      yield* http.respond(
        ScriptedResponse.Response({
          status: 200,
          headers: {
            "content-type": "application/json",
            "stream-next-offset": "end",
            "stream-up-to-date": "true",
          },
          body: "[1]",
        }),
      );
      const result = yield* client.json.pipe(Stream.runCollect, Effect.provide(http.layer));
      expect(result).toEqual([1]);
    }),
  );

  it.effect("provides the contextual HEAD capability independently of HTTP", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      yield* http.respond(ScriptedResponse.Response({ status: 404, headers: {} }));
      const program: Effect.Effect<
        StreamMetadata,
        HeadError,
        DurableStreamsClient | HttpClient.HttpClient
      > = Effect.gen(function* () {
        const client = yield* DurableStreamsClient;
        return yield* client.head;
      });
      expect(
        yield* program.pipe(
          Effect.provide(
            Layer.merge(
              DurableStreamsClient.layer({ url: "https://streams.test/orders" }),
              http.layer,
            ),
          ),
        ),
      ).toEqual(StreamMetadata.cases.Missing.make({}));
    }),
  );

  it.effect("keeps contextual JSON precise through complete provisioning", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const program = Effect.gen(function* () {
        const client = yield* DurableStreamsClient;
        expectTypeOf<Stream.Success<typeof client.json>>().toEqualTypeOf<Schema.Json>();
        expectTypeOf<Stream.Services<typeof client.json>>().toEqualTypeOf<HttpClient.HttpClient>();
        return yield* Stream.runCollect(client.json);
      });
      expectTypeOf<Effect.Services<typeof program>>().toEqualTypeOf<
        DurableStreamsClient | HttpClient.HttpClient
      >();
      const clientProvided = program.pipe(
        Effect.provide(DurableStreamsClient.layer({ url: "https://streams.test/orders" })),
      );
      expectTypeOf<Effect.Services<typeof clientProvided>>().toEqualTypeOf<HttpClient.HttpClient>();
      const provided = clientProvided.pipe(Effect.provide(http.layer));
      expectTypeOf<Effect.Services<typeof provided>>().toEqualTypeOf<never>();
      yield* http.respond(
        ScriptedResponse.Response({
          status: 200,
          headers: {
            "content-type": "application/json",
            "stream-next-offset": "end",
            "stream-up-to-date": "true",
          },
          body: '["123"]',
        }),
      );
      expect(yield* provided).toEqual(["123"]);
    }),
  );

  it.effect("rejects schemas in layer types and at the untyped runtime boundary", () =>
    Effect.gen(function* () {
      const custom = { url: "https://streams.test/orders", schema: Schema.String };
      const raw = { url: custom.url, schema: Schema.Json };
      expectTypeOf<
        typeof custom extends Parameters<typeof DurableStreamsClient.layer>[0] ? true : false
      >().toEqualTypeOf<false>();
      expectTypeOf<
        typeof raw extends DurableStreamsClientLayerConfig ? true : false
      >().toEqualTypeOf<false>();
      for (const input of [custom, raw, { url: custom.url, schema: "invalid" }]) {
        const erased: { readonly url: string } = input;
        const result = yield* Effect.service(DurableStreamsClient).pipe(
          Effect.provide(DurableStreamsClient.layer(erased)),
          Effect.catchTag("InvalidDurableStreamsConfigError", (error) =>
            Effect.succeed(error.field),
          ),
        );
        expect(result).toBe("schema");
      }
    }),
  );

  it.effect("supports an app-owned typed contextual service built with make", () =>
    Effect.gen(function* () {
      class DecodeOrder extends Context.Service<DecodeOrder, { readonly prefix: string }>()(
        "app/DecodeOrder",
      ) {}
      class EncodeOrder extends Context.Service<EncodeOrder, { readonly suffix: string }>()(
        "app/EncodeOrder",
      ) {}
      const Order = Schema.String.pipe(
        Schema.decodeTo(Schema.Struct({ orderId: Schema.String }), {
          decode: SchemaGetter.transformOrFail((value) =>
            DecodeOrder.pipe(Effect.map((service) => ({ orderId: service.prefix + value }))),
          ),
          encode: SchemaGetter.transformOrFail((value) =>
            EncodeOrder.pipe(Effect.map((service) => value.orderId + service.suffix)),
          ),
        }),
      );
      const makeOrders = DurableStreamsClient.make({
        url: "https://streams.test/orders",
        schema: Order,
      });
      const direct = yield* makeOrders;
      expectTypeOf<Stream.Success<typeof direct.json>>().toEqualTypeOf<typeof Order.Type>();
      expectTypeOf<Stream.Services<typeof direct.json>>().toEqualTypeOf<
        DecodeOrder | HttpClient.HttpClient
      >();
      class Orders extends Context.Service<
        Orders,
        {
          readonly events: Stream.Stream<
            typeof Order.Type,
            ReadError,
            DecodeOrder | HttpClient.HttpClient
          >;
        }
      >()("app/Orders") {}
      const ordersLive = Layer.effect(
        Orders,
        makeOrders.pipe(Effect.map((client) => ({ events: client.json }))),
      );
      expectTypeOf<Effect.Services<typeof makeOrders>>().toEqualTypeOf<never>();
      expectTypeOf<typeof Order.Encoded>().toEqualTypeOf<string>();
      const encode = Schema.encodeEffect(Order)({ orderId: "123" });
      expectTypeOf<Effect.Services<typeof encode>>().toEqualTypeOf<EncodeOrder>();
      expect(yield* encode.pipe(Effect.provideService(EncodeOrder, { suffix: "!" }))).toBe("123!");
      const program = Effect.gen(function* () {
        const orders = yield* Orders;
        return yield* Stream.runCollect(orders.events);
      });
      expectTypeOf<Effect.Services<typeof program>>().toEqualTypeOf<
        Orders | DecodeOrder | HttpClient.HttpClient
      >();
      const http = yield* makeScriptedHttpClient;
      const provided = program.pipe(
        Effect.provide(
          Layer.mergeAll(ordersLive, Layer.succeed(DecodeOrder, { prefix: "order-" }), http.layer),
        ),
      );
      expectTypeOf<Effect.Services<typeof provided>>().toEqualTypeOf<never>();
      yield* http.respond(
        ScriptedResponse.Response({
          status: 200,
          headers: {
            "content-type": "application/json",
            "stream-next-offset": "end",
            "stream-up-to-date": "true",
          },
          body: '["123"]',
        }),
      );
      expect(yield* provided).toEqual([{ orderId: "order-123" }]);
    }),
  );

  it.effect("uses the ambient HTTP layer anew for each invocation", () =>
    Effect.gen(function* () {
      const first = yield* makeScriptedHttpClient;
      const second = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({ url: "https://streams.test/orders" });
      yield* first.respond(ScriptedResponse.Response({ status: 404, headers: {} }));
      yield* second.respond(
        ScriptedResponse.Response({
          status: 200,
          headers: { "content-type": "text/plain", "stream-next-offset": "opaque" },
        }),
      );
      expect(yield* client.head.pipe(Effect.provide(first.layer))).toEqual(
        StreamMetadata.cases.Missing.make({}),
      );
      expect(yield* client.connect.pipe(Effect.provide(second.layer))).toMatchObject({
        offset: "opaque",
      });
    }),
  );

  it.effect("rejects invalid configuration without acquiring HTTP", () =>
    Effect.gen(function* () {
      for (const url of ["not a url", "file:///private", "/relative"]) {
        const rejected = yield* DurableStreamsClient.make({ url }).pipe(
          Effect.catchTag("InvalidDurableStreamsConfigError", () => Effect.succeed("rejected")),
        );
        expect(rejected).toBe("rejected");
      }
      const rejected = yield* DurableStreamsClient.make({
        url: "https://streams.test/orders",
        contentType: "text/plain",
        schema: Schema.String,
      }).pipe(
        Effect.catchTag("InvalidDurableStreamsConfigError", (error) => Effect.succeed(error.field)),
      );
      expect(rejected).toBe("schema");
    }),
  );
});
