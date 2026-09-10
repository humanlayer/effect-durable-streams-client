import {
  Array as Arr,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Option,
  Predicate,
  Record,
  Ref,
  Schema,
  Scope,
  Stream,
} from "effect";
import type { HttpClient } from "effect/unstable/http";
import { AppendOutcomeUnknownError, PayloadEncodeError, type AppendError } from "./errors";
import {
  captureSchemaFailure,
  combineAppendBodies,
  encodePayload,
  type PreparedBody,
} from "./encoding";
import { FieldValue } from "./headers";
import { appendStreamValue, type LifecycleContext } from "./lifecycle";
import type {
  AppendInput,
  AppendResult,
  AppendStreamInput,
  DurableStreamsConnection,
} from "./model";

type Entry = {
  executionContext: Context.Context<HttpClient.HttpClient> | undefined;
  readonly body: Uint8Array;
  readonly contentType: string;
  readonly connection: DurableStreamsConnection;
  readonly seq?: string;
  readonly cancelled: Deferred.Deferred<void>;
  readonly receipt: Deferred.Deferred<AppendResult, AppendError>;
};

type Burst = {
  readonly scope: Scope.Closeable;
  active: ReadonlyArray<Entry>;
  buffer: Array<Entry>;
  leases: number;
};

export const sendOrdinaryBatch = (entries: ReadonlyArray<Entry>) => {
  const first = entries[0];
  if (first === undefined) return Effect.die("Empty ordinary append batch");
  if (first.executionContext === undefined) return Effect.fail(new AppendOutcomeUnknownError({}));
  const seq = entries.findLast((entry) => entry.seq !== undefined)?.seq;
  const send = appendStreamValue<typeof Schema.Json>({
    connection: first.connection,
    input: { value: null, ...Record.filter({ seq }, Predicate.isNotUndefined) },
    prepared: {
      contentType: first.contentType,
      body: combineAppendBodies({
        bodies: entries.map((entry) => entry.body),
        contentType: first.contentType,
      }),
    },
  }).pipe(Effect.provideContext(first.executionContext));
  return Effect.forEach(entries, (entry) => Deferred.isDone(entry.cancelled)).pipe(
    Effect.flatMap((cancelled) =>
      cancelled.some(Boolean) ? Effect.fail(new AppendOutcomeUnknownError({})) : send,
    ),
  );
};

export const runOrdinaryBurst = ({
  state,
  burst,
}: {
  readonly state: Ref.Ref<Burst | undefined>;
  readonly burst: Burst;
}) =>
  Effect.gen(function* () {
    while (Arr.isReadonlyArrayNonEmpty(burst.active)) {
      const batch = burst.active;
      const outcome = yield* Effect.raceAllFirst([
        sendOrdinaryBatch(batch),
        ...batch.map((entry) =>
          Deferred.await(entry.cancelled).pipe(
            Effect.andThen(Effect.fail(new AppendOutcomeUnknownError({}))),
          ),
        ),
      ]).pipe(Effect.exit);
      yield* Ref.modify(state, (current) => {
        for (const entry of batch) {
          entry.executionContext = undefined;
          Deferred.doneUnsafe(entry.receipt, outcome);
        }
        if (Exit.isFailure(outcome)) {
          for (const entry of burst.buffer) {
            entry.executionContext = undefined;
            Deferred.doneUnsafe(entry.receipt, outcome);
          }
          burst.buffer = [];
        }
        const first = burst.buffer[0];
        const boundary =
          first === undefined
            ? 0
            : burst.buffer.findIndex((entry) => {
                const left = first.executionContext;
                const right = entry.executionContext;
                return (
                  entry.contentType !== first.contentType ||
                  (left !== undefined &&
                    right !== undefined &&
                    left !== right &&
                    (left.mapUnsafe.size !== right.mapUnsafe.size ||
                      [...left.mapUnsafe].some(
                        ([key, value]) =>
                          !right.mapUnsafe.has(key) || right.mapUnsafe.get(key) !== value,
                      )))
                );
              });
        burst.active = burst.buffer.splice(0, boundary < 0 ? burst.buffer.length : boundary);
        return [
          undefined,
          current === burst && !Arr.isReadonlyArrayNonEmpty(burst.active) ? undefined : current,
        ];
      });
    }
  }).pipe(
    Effect.onExit((exit) =>
      Ref.modify(state, (current) => {
        const outcome =
          Exit.isFailure(exit) && !Exit.hasInterrupts(exit)
            ? exit
            : Exit.fail(new AppendOutcomeUnknownError({}));
        for (const entry of [...burst.active, ...burst.buffer]) {
          entry.executionContext = undefined;
          Deferred.doneUnsafe(entry.receipt, outcome);
        }
        burst.active = [];
        burst.buffer = [];
        return [undefined, current === burst ? undefined : current];
      }),
    ),
  );

export const allocateOrdinaryAppends = Effect.gen(function* () {
  const state = yield* Ref.make<Burst | undefined>(undefined);
  return <S extends Schema.Top>(
    context: LifecycleContext<S> & {
      readonly input: AppendInput<S["Type"] | Uint8Array>;
      readonly prepared?: PreparedBody;
    },
  ) =>
    Effect.contextWith((executionContext: Context.Context<HttpClient.HttpClient>) =>
      Effect.gen(function* () {
        if (context.connection.batching === false) return yield* appendStreamValue(context);
        const contentType =
          context.prepared?.contentType ??
          context.connection.contentType ??
          (context.schema === undefined ? "application/octet-stream" : "application/json");
        const body =
          context.prepared?.body ??
          (yield* encodePayload({
            ...context,
            value: context.input.value,
            contentType,
            operation: "append",
          }));
        if (context.input.seq !== undefined)
          yield* Schema.decodeEffect(FieldValue)(context.input.seq);
        const entry: Entry = {
          executionContext,
          body,
          contentType,
          connection: context.connection,
          ...Record.filter({ seq: context.input.seq }, Predicate.isNotUndefined),
          cancelled: yield* Deferred.make<void>(),
          receipt: yield* Deferred.make<AppendResult, AppendError>(),
        };
        return yield* Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            const { burst, start } = yield* Ref.modify(state, (existing) => {
              const burst: Burst = existing ?? {
                scope: Scope.makeUnsafe(),
                active: [entry],
                buffer: [],
                leases: 0,
              };
              burst.leases++;
              if (existing !== undefined) burst.buffer.push(entry);
              return [{ burst, start: existing === undefined }, burst];
            });
            return yield* Effect.gen(function* () {
              if (start)
                yield* runOrdinaryBurst({ state, burst }).pipe(
                  Effect.interruptible,
                  Effect.forkIn(burst.scope),
                  Effect.updateContext<never, never>(() => Context.empty()),
                );
              yield* Effect.logDebug("Ordinary append admitted");
              return yield* restore(Deferred.await(entry.receipt)).pipe(
                Effect.onInterrupt(() =>
                  Effect.gen(function* () {
                    yield* Deferred.succeed(entry.cancelled, undefined);
                    yield* Effect.logWarning("Ordinary append batch interrupted");
                    if (burst.active.includes(entry)) {
                      yield* Effect.exit(Deferred.await(entry.receipt));
                    } else {
                      entry.executionContext = undefined;
                    }
                  }),
                ),
              );
            }).pipe(
              Effect.ensuring(
                Effect.suspend(() => {
                  burst.leases--;
                  return burst.leases === 0 ? Scope.close(burst.scope, Exit.void) : Effect.void;
                }),
              ),
            );
          }),
        );
      }).pipe(
        Effect.tapErrorTag("SchemaError", (cause) =>
          captureSchemaFailure({ cause, operation: "append", component: "write options" }),
        ),
        Effect.catchTag("SchemaError", () =>
          Effect.fail(new PayloadEncodeError({ component: "write options" })),
        ),
        Effect.withSpan("durable_streams.append"),
      ),
    );
});

export const appendSource = Effect.fn("durable_streams.append_stream")(function* <E, R>(context: {
  readonly connection: DurableStreamsConnection;
  readonly hasSchema: boolean;
  readonly input: AppendStreamInput<E, R>;
}) {
  const scope = yield* Effect.scope;
  const failed = yield* Deferred.make<never, E>();
  const pull = yield* context.input.source.pipe(
    Stream.map((chunk) => (Predicate.isString(chunk) ? new TextEncoder().encode(chunk) : chunk)),
    Stream.catchCause((cause) =>
      Stream.fromEffect(
        Deferred.failCause(failed, cause).pipe(Effect.andThen(Effect.failCause(cause))),
      ),
    ),
    Stream.toPull,
  );
  const bodyStream = Stream.fromPull(
    Effect.gen(function* () {
      const consumer = yield* Effect.fiber;
      return yield* Scope.addFinalizer(
        scope,
        Effect.withFiber((current) =>
          current.id === consumer.id ? Effect.interrupt : Fiber.interrupt(consumer),
        ),
      ).pipe(Effect.map(() => pull.pipe(Effect.forkIn(scope), Effect.flatMap(Fiber.join))));
    }),
  );
  const contentType =
    context.connection.contentType ??
    (context.hasSchema ? "application/json" : "application/octet-stream");
  return yield* appendStreamValue<typeof Schema.Json>({
    connection: context.connection,
    input: {
      value: null,
      ...Record.filter({ seq: context.input.seq }, Predicate.isNotUndefined),
    },
    prepared: { bodyStream, contentType },
  }).pipe(
    Effect.catch((error) =>
      Deferred.poll(failed).pipe(
        Effect.flatMap((failure) => {
          const outcome: Effect.Effect<never, E | AppendError> = Option.isNone(failure)
            ? Effect.fail(error)
            : failure.value;
          return outcome;
        }),
      ),
    ),
    Effect.raceFirst(Deferred.await(failed)),
  );
}, Effect.scoped);
