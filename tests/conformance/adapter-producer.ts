import { Duration, Effect, Exit, Match, Predicate, Record, Schema, Scope, Stream } from "effect";
import type { TestResult } from "@durable-streams/client-conformance-tests/protocol";
import { DurableStreamsClient } from "../../src/index.ts";
import { AdapterState } from "./adapter-state.ts";

const ProducerIdentity = {
  path: Schema.String,
  producerId: Schema.String,
  epoch: Schema.Finite,
  headers: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
};
export const ProducerCommand = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("idempotent-append"),
    ...ProducerIdentity,
    data: Schema.String,
    autoClaim: Schema.Boolean,
  }),
  Schema.Struct({
    type: Schema.Literal("idempotent-append-batch"),
    ...ProducerIdentity,
    items: Schema.Array(Schema.String),
    autoClaim: Schema.Boolean,
    maxInFlight: Schema.optionalKey(Schema.Finite),
  }),
  Schema.Struct({
    type: Schema.Literal("idempotent-close"),
    ...ProducerIdentity,
    data: Schema.optionalKey(Schema.String),
    autoClaim: Schema.Boolean,
  }),
  Schema.Struct({ type: Schema.Literal("idempotent-detach"), ...ProducerIdentity }),
]).pipe(Schema.toTaggedUnion("type"));
export type ProducerCommand = typeof ProducerCommand.Type;
export const ValidateCommand = Schema.Struct({
  type: Schema.Literal("validate"),
  target: Schema.Union([
    Schema.Struct({
      target: Schema.Literal("retry-options"),
      maxRetries: Schema.optionalKey(Schema.Finite),
      initialDelayMs: Schema.optionalKey(Schema.Finite),
      maxDelayMs: Schema.optionalKey(Schema.Finite),
      multiplier: Schema.optionalKey(Schema.Finite),
    }),
    Schema.Struct({
      target: Schema.Literal("idempotent-producer"),
      producerId: Schema.optionalKey(Schema.String),
      epoch: Schema.optionalKey(Schema.Finite),
      maxBatchBytes: Schema.optionalKey(Schema.Finite),
      maxBatchItems: Schema.optionalKey(Schema.Finite),
    }),
  ]),
});

export const validateOptions = (input: typeof ValidateCommand.Type) =>
  Effect.gen(function* () {
    yield* Match.value(input.target).pipe(
      Match.discriminatorsExhaustive("target")({
        "retry-options": (target) =>
          DurableStreamsClient.make({
            url: "http://localhost/validate",
            backoffOptions: Record.filter(
              {
                maxRetries: target.maxRetries,
                initialDelay: target.initialDelayMs,
                maxDelay: target.maxDelayMs,
                multiplier: target.multiplier,
              },
              Predicate.isNotUndefined,
            ),
          }),
        "idempotent-producer": (target) =>
          Effect.gen(function* () {
            const client = yield* DurableStreamsClient.make({ url: "http://localhost/validate" });
            return yield* client.producer({
              producerId: target.producerId ?? "test-producer",
              ...Record.filter(
                { epoch: target.epoch, maxBatchBytes: target.maxBatchBytes },
                Predicate.isNotUndefined,
              ),
            });
          }).pipe(Effect.scoped),
      }),
    );
    return { type: "validate", success: true } satisfies TestResult;
  });

export const handleProducer = (input: ProducerCommand) =>
  Effect.gen(function* () {
    const state = yield* AdapterState;
    const url = yield* state.location(input);
    const contentType = (yield* state.contentType(input)) ?? "application/octet-stream";
    const decode = (value: string) =>
      contentType.split(";")[0]?.trim().toLowerCase() === "application/json"
        ? Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(value)
        : Effect.succeed(value);
    const client = yield* DurableStreamsClient.make({
      url,
      contentType,
      ...Record.filter({ headers: input.headers }, Predicate.isNotUndefined),
    });
    if (input.type === "idempotent-append-batch")
      return yield* Effect.gen(function* () {
        const concurrent = (input.maxInFlight ?? 1) > 1;
        const producer = yield* client.producer({
          producerId: input.producerId,
          epoch: input.epoch,
          autoClaim: input.autoClaim,
          maxInFlight: input.maxInFlight ?? 1,
          maxBatchBytes: concurrent ? 1 : 1048576,
          linger: Duration.millis(concurrent ? 0 : 1000),
        });
        yield* Stream.fromIterable(input.items).pipe(
          Stream.mapEffect(decode),
          Stream.run(producer.sink),
        );
        yield* producer.detach;
        return {
          type: "idempotent-append-batch",
          success: true,
          status: 200,
          producerSeq: (yield* producer.nextSeq) - 1,
        } satisfies TestResult;
      }).pipe(Effect.scoped);
    const key = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Array(Schema.Json)))([
      input.path,
      input.producerId,
      input.epoch,
    ]);
    const cached = state.producers.get(key);
    const entry =
      cached ??
      (yield* Effect.gen(function* () {
        const scope = yield* Scope.fork(state.producerScope);
        const producer = yield* client
          .producer({
            producerId: input.producerId,
            epoch: input.epoch,
            autoClaim: input.type !== "idempotent-detach" && input.autoClaim,
            maxBatchBytes: 1,
          })
          .pipe(Effect.provideService(Scope.Scope, scope));
        const entry = { scope, producer };
        state.producers.set(key, entry);
        return entry;
      }));
    return yield* Match.value(input).pipe(
      Match.discriminatorsExhaustive("type")({
        "idempotent-append": (command) =>
          Effect.gen(function* () {
            const result = yield* entry.producer.append({ value: yield* decode(command.data) });
            return {
              type: "idempotent-append",
              success: true,
              status: result.duplicate ? 204 : 200,
              ...result,
            } satisfies TestResult;
          }),
        "idempotent-close": (command) =>
          Effect.gen(function* () {
            const value = command.data === undefined ? undefined : yield* decode(command.data);
            const result = yield* entry.producer.close(
              Record.filter({ value }, Predicate.isNotUndefined),
            );
            return {
              type: "idempotent-close",
              success: true,
              status: 200,
              ...result,
            } satisfies TestResult;
          }),
        "idempotent-detach": () =>
          entry.producer.detach.pipe(
            Effect.andThen(Scope.close(entry.scope, Exit.void)),
            Effect.map(() => {
              state.producers.delete(key);
              return { type: "idempotent-detach", success: true, status: 200 } satisfies TestResult;
            }),
          ),
      }),
    );
  });
