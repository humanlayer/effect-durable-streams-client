import { describe, expect, expectTypeOf, it } from "@effect/vitest";
import { Context, Deferred, Effect, Schema, SchemaGetter, Scope } from "effect";
import { FetchHttpClient, HttpClient } from "effect/unstable/http";
import { DurableStream, makeEffectClient } from "../src/async-await.js";

class Encoder extends Context.Service<Encoder, { readonly suffix: string }>()("async/Encoder") {}
class Decoder extends Context.Service<Decoder, { readonly prefix: string }>()("async/Decoder") {}
const _codec = Schema.String.pipe(
  Schema.decodeTo(Schema.Struct({ id: Schema.String }), {
    decode: SchemaGetter.transformOrFail((id) =>
      Decoder.pipe(Effect.map((service) => ({ id: service.prefix + id }))),
    ),
    encode: SchemaGetter.transformOrFail((value) =>
      Encoder.pipe(Effect.map((service) => value.id + service.suffix)),
    ),
  }),
);

describe("async schema specialization (phase E)", () => {
  it.effect("overlapping advanced scopes keep HTTP and both schema services isolated", () =>
    Effect.gen(function* () {
      const aReady = yield* Deferred.make<void>();
      const bReady = yield* Deferred.make<void>();
      const bodies: Array<string> = [];
      const released: Array<string> = [];
      const run = (id: string, ready: Deferred.Deferred<void>, other: Deferred.Deferred<void>) =>
        Effect.gen(function* () {
          yield* Effect.acquireRelease(Effect.void, () =>
            Effect.sync(() => {
              released.push(id);
            }),
          );
          const client = yield* makeEffectClient({
            url: "https://example.test/shared",
            schema: _codec,
          });
          yield* Deferred.succeed(ready, undefined);
          yield* Deferred.await(other);
          yield* Effect.promise(async () => {
            await client.appendJson({ id: "value" });
            const response = await client.stream({ live: false });
            expect(await response.json()).toEqual([{ id: `${id}:${id}` }]);
            await response.closed;
            expect(released).not.toContain(id);
          });
        }).pipe(
          Effect.scoped,
          Effect.provide(FetchHttpClient.layer),
          Effect.provideService(FetchHttpClient.Fetch, async (_url, init) => {
            if (init?.method === "POST") {
              bodies.push(`${id}:${await new Response(init.body).text()}`);
              return new Response(null, { status: 204, headers: { "stream-next-offset": "tail" } });
            }
            return new Response(`["${id}"]`, {
              headers: {
                "content-type": "application/json",
                "stream-next-offset": "tail",
                "stream-up-to-date": "true",
              },
            });
          }),
          Effect.provideService(Encoder, { suffix: id }),
          Effect.provideService(Decoder, { prefix: `${id}:` }),
        );
      yield* Effect.all([run("A", aReady, bReady), run("B", bReady, aReady)], { concurrency: 2 });
      expect(bodies.toSorted()).toEqual(['A:["valueA"]', 'B:["valueB"]']);
      expect(released.toSorted()).toEqual(["A", "B"]);
    }),
  );
  it("infers schema values while raw methods retain serialized inputs", async () => {
    const bodies: Array<string> = [];
    const fetchClient: typeof fetch = async (_url, init) => {
      if (init?.method === "POST") {
        bodies.push(await new Response(init.body).text());
        return new Response(null, { status: 204, headers: { "stream-next-offset": "tail" } });
      }
      return new Response('[{"id":"one"}]', {
        headers: {
          "content-type": "application/json",
          "stream-next-offset": "tail",
          "stream-up-to-date": "true",
        },
      });
    };
    const client = DurableStream.withSchema({
      url: "https://example.test/a",
      fetch: fetchClient,
      schema: Schema.Struct({ id: Schema.String }),
    });
    expectTypeOf<Parameters<typeof client.appendJson>[0]>().toEqualTypeOf<{
      readonly id: string;
    }>();
    await client.appendJson({ id: "one" });
    await client.append("false");
    expect(bodies).toEqual(['[{"id":"one"}]', "[false]"]);
    const response = await client.stream();
    expectTypeOf<Awaited<ReturnType<typeof response.json>>>().toEqualTypeOf<
      Array<{ readonly id: string }>
    >();
    expect(await response.json()).toEqual([{ id: "one" }]);
  });

  it("maps typed decode failures before acknowledging the response", async () => {
    const payload = '[{"id":1}]';
    const fetchClient: typeof fetch = async () =>
      new Response(payload, {
        headers: {
          "content-type": "application/json",
          "stream-next-offset": "tail",
          "stream-up-to-date": "true",
        },
      });
    const client = DurableStream.withSchema({
      url: "https://example.test/a",
      fetch: fetchClient,
      schema: Schema.Struct({ id: Schema.String }),
    });
    const response = await client.stream();
    await expect(response.json()).rejects.toMatchObject({ code: "PARSE_ERROR" });
    await expect(response.closed).rejects.toMatchObject({ code: "PARSE_ERROR" });
    expect(response.offset).toBe("-1");
  });

  it.effect("captures both service requirements and parent-owned read cleanup", () =>
    Effect.gen(function* () {
      const requests: Array<string> = [];
      const fetchClient: typeof fetch = async (_url, init) => {
        if (init?.method === "POST") {
          requests.push(await new Response(init.body).text());
          return new Response(null, { status: 204, headers: { "stream-next-offset": "tail" } });
        }
        return new Response('["wire"]', {
          headers: {
            "content-type": "application/json",
            "stream-next-offset": "tail",
            "stream-up-to-date": "true",
          },
        });
      };
      const acquisition = makeEffectClient({ url: "https://example.test/a", schema: _codec });
      expectTypeOf<Effect.Services<typeof acquisition>>().toEqualTypeOf<
        HttpClient.HttpClient | Scope.Scope | Encoder | Decoder
      >();
      const escaped = yield* Effect.gen(function* () {
        const client = yield* acquisition;
        const response = yield* Effect.promise(async () => {
          await client.appendJson({ id: "write" });
          const first = await client.stream();
          expect(await first.json()).toEqual([{ id: "read-wire" }]);
          return client.stream();
        });
        return { client, response };
      }).pipe(
        Effect.scoped,
        Effect.provide(FetchHttpClient.layer),
        Effect.provideService(FetchHttpClient.Fetch, fetchClient),
        Effect.provideService(Encoder, { suffix: "!" }),
        Effect.provideService(Decoder, { prefix: "read-" }),
      );
      expect(requests).toEqual(['["write!"]']);
      yield* Effect.promise(async () => {
        await escaped.response.closed;
        await expect(escaped.client.appendJson({ id: "after-close" })).rejects.toMatchObject({
          code: "ABORTED",
        });
      });
      expect(requests).toHaveLength(1);
    }),
  );
});
