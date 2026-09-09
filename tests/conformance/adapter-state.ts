import { Context, Data, Effect, Option, Ref, Schema } from "effect";

export class AdapterNotInitialized extends Data.TaggedError("AdapterNotInitialized") {}

export class AdapterState extends Context.Service<AdapterState>()("conformance/AdapterState", {
  make: Effect.gen(function* () {
    const server = yield* Ref.make<Option.Option<URL>>(Option.none());
    return {
      initialize: (input: { readonly serverUrl: string }) =>
        Schema.decodeEffect(Schema.URLFromString)(input.serverUrl).pipe(
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
