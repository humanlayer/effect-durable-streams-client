import { Context, Effect, Layer, Option, Ref, Schema, Stream, Match, type Scope } from "effect";
import type { HttpClient } from "effect/unstable/http";
import {
  InvalidDurableStreamsConfigError,
  type AppendError,
  type CloseError,
  type CreateError,
  type DeleteError,
  type HeadError,
  type ReadError,
} from "./errors.js";
import {
  DurableStreamsConnection,
  type DurableStreamsClientConfig,
  type DurableStreamsClientLayerConfig,
  type Offset,
  type AppendInput,
  type AppendStreamInput,
  type CloseInput,
  type CreateInput,
  type AppendResult,
  type CloseResult,
  type CreateResult,
  type StreamMetadata,
} from "./model.js";
import { inspectStream } from "./transport.js";
import { checkExtensions } from "./request.js";
import { closeStream, createStream, deleteStream } from "./lifecycle.js";
import { allocateRead } from "./read.js";
import { appendSource, allocateOrdinaryAppends } from "./append.js";
import { acquireProducer, type IdempotentProducer } from "./producer.js";
import type { ProducerOptions } from "./model.js";
export type { IdempotentProducer } from "./producer.js";

export * from "./model.js";
export * from "./errors.js";

export type Client<S extends Schema.Top, A = S["Type"]> = {
  readonly producer: (
    input: ProducerOptions,
  ) => Effect.Effect<
    IdempotentProducer<A, S["EncodingServices"]>,
    InvalidDurableStreamsConfigError,
    HttpClient.HttpClient | Scope.Scope
  >;
  readonly head: Effect.Effect<StreamMetadata, HeadError, HttpClient.HttpClient>;
  readonly connect: Effect.Effect<StreamMetadata, HeadError, HttpClient.HttpClient>;
  readonly create: (
    input: CreateInput<A>,
  ) => Effect.Effect<CreateResult, CreateError, HttpClient.HttpClient | S["EncodingServices"]>;
  readonly append: (
    input: AppendInput<A>,
  ) => Effect.Effect<AppendResult, AppendError, HttpClient.HttpClient | S["EncodingServices"]>;
  readonly close: (
    input: CloseInput<A>,
  ) => Effect.Effect<CloseResult, CloseError, HttpClient.HttpClient | S["EncodingServices"]>;
  readonly appendStream: <E, R>(
    input: AppendStreamInput<E, R>,
  ) => Effect.Effect<AppendResult, AppendError | E, HttpClient.HttpClient | R>;
  readonly delete: Effect.Effect<void, DeleteError, HttpClient.HttpClient>;
  readonly offset: Effect.Effect<Option.Option<Offset>>;
  readonly bytes: Stream.Stream<Uint8Array, ReadError, HttpClient.HttpClient>;
  readonly text: Stream.Stream<string, ReadError, HttpClient.HttpClient>;
  readonly json: Stream.Stream<S["Type"], ReadError, HttpClient.HttpClient | S["DecodingServices"]>;
};

function _make(
  config: DurableStreamsClientLayerConfig,
): Effect.Effect<
  Client<typeof Schema.Json, Schema.Json | Uint8Array>,
  InvalidDurableStreamsConfigError
>;
function _make<S extends Schema.Top>(
  config: DurableStreamsClientConfig<S>,
): Effect.Effect<Client<S>, InvalidDurableStreamsConfigError>;
function _make<S extends Schema.Top = typeof Schema.Json>(config: DurableStreamsClientConfig<S>) {
  const result: Effect.Effect<
    Client<S> | Client<typeof Schema.Json, Schema.Json | Uint8Array>,
    InvalidDurableStreamsConfigError
  > = Effect.gen(function* () {
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
    yield* checkExtensions(connection);
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
    const read = yield* allocateRead({
      connection,
      schema: config.schema ?? Schema.Json,
      hasSchema: config.schema !== undefined,
    });
    const currentConnection = yield* Ref.make(connection);
    const append = yield* allocateOrdinaryAppends;
    return {
      producer: (input: ProducerOptions) =>
        Ref.get(currentConnection).pipe(
          Effect.flatMap((connection) =>
            acquireProducer<S>({ connection, schema: config.schema, input }),
          ),
          Effect.map(({ native }) => native),
        ),
      create: (input: CreateInput<S["Type"] | Uint8Array>) =>
        Ref.get(currentConnection).pipe(
          Effect.flatMap((connection) =>
            createStream({ connection, schema: config.schema, input }),
          ),
          Effect.tap((result) =>
            Ref.update(currentConnection, (current) => ({
              ...current,
              contentType: result.contentType,
            })),
          ),
        ),
      append: (input: AppendInput<S["Type"] | Uint8Array>) =>
        Ref.get(currentConnection).pipe(
          Effect.flatMap((connection) => append({ connection, schema: config.schema, input })),
        ),
      appendStream: <E, R>(input: AppendStreamInput<E, R>) =>
        Ref.get(currentConnection).pipe(
          Effect.flatMap((connection) =>
            appendSource({ connection, hasSchema: config.schema !== undefined, input }),
          ),
        ),
      close: (input: CloseInput<S["Type"] | Uint8Array>) =>
        Ref.get(currentConnection).pipe(
          Effect.flatMap((connection) => closeStream({ connection, schema: config.schema, input })),
        ),
      delete: deleteStream(connection),
      head: inspectStream({ connection, operation: "head" }),
      connect: inspectStream({
        connection,
        operation: "connect",
        hasSchema: config.schema !== undefined,
      }).pipe(
        Effect.tap((metadata) =>
          Match.value(metadata).pipe(
            Match.tagsExhaustive({
              Missing: () => Effect.void,
              Existing: (result) =>
                Ref.update(currentConnection, (current) => ({
                  ...current,
                  contentType: result.contentType,
                })),
            }),
          ),
        ),
      ),
      bytes: read.bytes,
      text: read.text,
      json: read.json,
      offset: read.offset,
    };
  }).pipe(Effect.withSpan("durable_streams.make"));
  return result;
}

const LayerSchemaPolicy = Schema.Struct({ schema: Schema.optionalKey(Schema.Never) });

export class DurableStreamsClient extends Context.Service<
  DurableStreamsClient,
  Client<typeof Schema.Json, Schema.Json | Uint8Array>
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
        Effect.andThen(() => _make(config)),
      ),
    );
}
