import { Clock, Context, Data, Effect, Exit, Match, Option, Ref, Schema, Scope } from "effect";
import type { IdempotentProducer } from "../../src/index";
import { HttpClientRequest } from "effect/unstable/http";

export class AdapterNotInitialized extends Data.TaggedError("AdapterNotInitialized") {}

export const DynamicValue = Schema.Struct({
  name: Schema.String,
  valueType: Schema.Literals(["counter", "timestamp", "token"]),
  initialValue: Schema.optionalKey(Schema.String),
});
export type DynamicValue = typeof DynamicValue.Type;

export class AdapterState extends Context.Service<AdapterState>()("conformance/AdapterState", {
  make: Effect.gen(function* () {
    const producerScope = yield* Effect.scope;
    const producers = new Map<
      string,
      {
        readonly scope: Scope.Closeable;
        readonly producer: IdempotentProducer<Schema.Json | Uint8Array>;
      }
    >();
    const server = yield* Ref.make<Option.Option<URL>>(Option.none());
    const headers = new Map<string, DynamicValue & { counter: number }>();
    const params = new Map<string, DynamicValue & { counter: number }>();
    const contentTypes = new Map<string, string>();
    const sent = yield* Ref.make<{
      readonly headersSent: Readonly<Record<string, string>>;
      readonly paramsSent: Readonly<Record<string, string>>;
    }>({ headersSent: {}, paramsSent: {} });
    return {
      producers,
      producerScope,
      contentType: (input: { readonly path: string }) =>
        Effect.sync(() => contentTypes.get(input.path)),
      remember: (input: { readonly path: string; readonly contentType: string }) =>
        Effect.sync(() => {
          contentTypes.set(input.path, input.contentType);
        }),
      forget: (input: { readonly path: string }) =>
        Effect.sync(() => {
          contentTypes.delete(input.path);
        }),
      setHeader: (input: DynamicValue) =>
        Effect.sync(() => {
          headers.set(input.name, { ...input, counter: 0 });
        }),
      setParam: (input: DynamicValue) =>
        Effect.sync(() => {
          params.set(input.name, { ...input, counter: 0 });
        }),
      clear: Effect.sync(() => {
        headers.clear();
        params.clear();
      }),
      sent: Ref.get(sent),
      transform: (request: HttpClientRequest.HttpClientRequest) =>
        Effect.gen(function* () {
          const now = yield* Clock.currentTimeMillis;
          const resolve = (entry: DynamicValue & { counter: number }) => {
            entry.counter++;
            return Match.value(entry.valueType).pipe(
              Match.when("counter", () => String(entry.counter)),
              Match.when("timestamp", () => String(now)),
              Match.when("token", () => entry.initialValue ?? "default-token"),
              Match.exhaustive,
            );
          };
          const headersSent = Object.fromEntries(
            [...headers].map(([name, entry]) => [name, resolve(entry)]),
          );
          const paramsSent = Object.fromEntries(
            [...params].map(([name, entry]) => [name, resolve(entry)]),
          );
          yield* Ref.set(sent, { headersSent, paramsSent });
          return request.pipe(
            HttpClientRequest.setHeaders(headersSent),
            HttpClientRequest.setUrlParams(paramsSent),
          );
        }),
      initialize: (input: { readonly serverUrl: string }) =>
        Schema.decodeEffect(Schema.URLFromString)(input.serverUrl).pipe(
          Effect.tap(() =>
            Effect.forEach(producers.values(), (entry) => Scope.close(entry.scope, Exit.void), {
              discard: true,
            }),
          ),
          Effect.tap(() =>
            Effect.sync(() => {
              producers.clear();
              headers.clear();
              params.clear();
              contentTypes.clear();
            }),
          ),
          Effect.flatMap((url) => Ref.set(server, Option.some(url))),
        ),
      location: (input: { readonly path: string }) =>
        Effect.gen(function* () {
          const url = yield* Ref.get(server);
          if (Option.isNone(url)) return yield* new AdapterNotInitialized();
          const location = yield* Schema.decodeEffect(Schema.URLFromString)(
            url.value.href.replace(/\/$/, "") + input.path,
          );
          return location.href;
        }),
    };
  }),
}) {}
