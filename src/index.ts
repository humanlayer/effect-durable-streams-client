import { Context, Effect, Layer, Option, Ref, Schema, Stream } from "effect";
import type { HttpClient } from "effect/unstable/http";
import { InvalidDurableStreamsConfigError } from "./errors.ts";
import {
  DurableStreamsConnection,
  type DurableStreamsClientConfig,
  type DurableStreamsClientLayerConfig,
  type Offset,
} from "./model.ts";
import { inspectStream } from "./transport.ts";

export * from "./model.ts";
export * from "./errors.ts";

const _make = <S extends Schema.Top = typeof Schema.Json>(config: DurableStreamsClientConfig<S>) =>
  Effect.gen(function* () {
    const connection = yield* Schema.decodeEffect(DurableStreamsConnection)(config).pipe(
      Effect.mapError(
        () =>
          new InvalidDurableStreamsConfigError({
            field: "connection",
            issues: ["Expected an absolute URL and valid connection options"],
          }),
      ),
    );
    if (connection.url.protocol !== "http:" && connection.url.protocol !== "https:") {
      return yield* new InvalidDurableStreamsConfigError({
        field: "url",
        issues: ["Expected HTTP or HTTPS"],
      });
    }
    if (config.schema !== undefined && !Schema.isSchema(config.schema)) {
      return yield* new InvalidDurableStreamsConfigError({
        field: "schema",
        issues: ["Expected an Effect schema"],
      });
    }
    if (
      config.schema !== undefined &&
      connection.contentType !== undefined &&
      connection.contentType.split(";")[0]?.trim().toLowerCase() !== "application/json"
    ) {
      return yield* new InvalidDurableStreamsConfigError({
        field: "schema",
        issues: ["Custom schemas require application/json"],
      });
    }
    const offset = yield* Ref.make<Option.Option<Offset>>(Option.none());
    const json: Stream.Stream<S["Type"], never, HttpClient.HttpClient | S["DecodingServices"]> =
      Stream.die("Durable Streams JSON reads are not implemented until Phase 4");
    return {
      head: inspectStream({ connection, operation: "head" }),
      connect: inspectStream({
        connection,
        operation: "connect",
        hasSchema: config.schema !== undefined,
      }),
      offset: Ref.get(offset).pipe(Effect.withSpan("durable_streams.offset")),
      json,
    };
  }).pipe(Effect.withSpan("durable_streams.make"));

const LayerSchemaPolicy = Schema.Struct({ schema: Schema.optionalKey(Schema.Never) });

export class DurableStreamsClient extends Context.Service<
  DurableStreamsClient,
  Effect.Success<ReturnType<typeof _make<typeof Schema.Json>>>
>()("effect-durable-streams/DurableStreamsClient") {
  static readonly make = _make;

  static readonly layer = (config: DurableStreamsClientLayerConfig) =>
    Layer.effect(
      DurableStreamsClient,
      Schema.decodeEffect(LayerSchemaPolicy)(config).pipe(
        Effect.mapError(
          () =>
            new InvalidDurableStreamsConfigError({
              field: "schema",
              issues: [
                "Custom schemas require DurableStreamsClient.make; layer provides raw JSON only",
              ],
            }),
        ),
        Effect.andThen(() => _make<typeof Schema.Json>(config)),
      ),
    );
}
