import {
  Array as Arr,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  Option,
  Predicate,
  Record,
  Schema,
  Scope,
  Semaphore,
  Sink,
} from "effect";
import { Queue } from "effect";
import type { HttpClient } from "effect/unstable/http";
import {
  InvalidDurableStreamsConfigError,
  ProducerClosedError,
  ProducerFencedError,
  ProducerSequenceGapError,
  ProtocolViolationError,
  type ProducerError,
} from "./errors.js";
import { combineAppendBodies, encodePayload, type PreparedBody } from "./encoding.js";
import {
  CloseResult,
  ProducerOptions,
  type ProducerAppendInput,
  type ProducerAppendResult,
  type ProducerCloseInput,
} from "./model.js";
import type { LifecycleContext } from "./lifecycle.js";
import { ProducerResponse, sendProducerRequest, type ProducerRequest } from "./producer-request.js";
import { freezeErrorResponse } from "./transport.js";

export type IdempotentProducer<A, R = never> = {
  readonly append: (
    input: ProducerAppendInput<A>,
  ) => Effect.Effect<ProducerAppendResult, ProducerError, R>;
  readonly sink: Sink.Sink<void, A, never, ProducerError, R>;
  readonly flush: Effect.Effect<void, ProducerError>;
  readonly detach: Effect.Effect<void, ProducerError>;
  readonly restart: Effect.Effect<void, ProducerError>;
  readonly close: (input: ProducerCloseInput<A>) => Effect.Effect<CloseResult, ProducerError, R>;
  readonly epoch: Effect.Effect<number>;
  readonly nextSeq: Effect.Effect<number>;
  readonly pendingCount: Effect.Effect<number>;
  readonly inFlightCount: Effect.Effect<number>;
  readonly lastSuccessfulOffset: Effect.Effect<Option.Option<string>>;
};

type Entry = {
  readonly id: number;
  readonly body: Uint8Array;
  readonly contentType: string;
  readonly receipt: Deferred.Deferred<ProducerAppendResult, ProducerError>;
};
type Batch = {
  readonly entries: ReadonlyArray<Entry>;
  readonly body: Uint8Array;
  readonly contentType: string;
  readonly completion: Deferred.Deferred<ProducerAppendResult, ProducerError>;
  tuple: { readonly epoch: number; readonly seq: number } | undefined;
  readonly firstClaim: boolean;
};

export const acquireProducer = Effect.fn("durable_streams.producer.make")(function* <
  S extends Schema.Top,
>(
  context: LifecycleContext<S> & {
    readonly input: ProducerOptions;
    readonly facade?: {
      readonly contentType: () => string;
      readonly onBatchExit: (
        exit: Exit.Exit<ProducerAppendResult, ProducerError>,
      ) => Effect.Effect<void>;
    };
  },
) {
  type ProducerState = {
    epoch: number;
    nextSeq: number;
    id: number;
    pending: Array<Entry>;
    bytes: number;
    timer: Fiber.Fiber<void> | undefined;
    generation: number;
    claimed: boolean;
    claiming: boolean;
    active: number;
    mode: "open" | "closing" | "detached" | "stopped";
    firstFailure:
      | { readonly id: number; readonly exit: Exit.Exit<never, ProducerError> }
      | undefined;
    closeRequest: ProducerRequest | undefined;
    closeBody: { readonly body?: Uint8Array } | undefined;
    closeResult: CloseResult | undefined;
    offset: string | undefined;
  };
  type SequenceProgress = {
    through: number;
    failure: { readonly seq: number; readonly exit: Exit.Exit<never, ProducerError> } | undefined;
  };
  const options = yield* ProducerOptions.makeEffect(context.input).pipe(
    Effect.mapError(
      () =>
        new InvalidDurableStreamsConfigError({
          field: "producer",
          issues: ["Invalid producer options"],
        }),
    ),
  );
  const executionContext = yield* Effect.context<HttpClient.HttpClient>();
  const scope = yield* Scope.fork(yield* Effect.scope);
  const mutex = yield* Semaphore.make(1);
  const lifecycle = yield* Semaphore.make(1);
  const maxInFlight = options.maxInFlight ?? 5;
  const tasks = yield* Queue.make<Batch>(
    context.facade === undefined ? { capacity: maxInFlight } : {},
  );
  const claim = { current: yield* Deferred.make<void, ProducerError>() };
  const stopped = yield* Deferred.make<never>();
  const contentType =
    context.connection.contentType ??
    (context.schema === undefined ? "application/octet-stream" : "application/json");
  const state: ProducerState = {
    epoch: options.epoch ?? 0,
    nextSeq: 0,
    id: 0,
    pending: [],
    bytes: 0,
    timer: undefined,
    generation: 0,
    claimed: !options.autoClaim,
    claiming: false,
    active: 0,
    mode: "open",
    firstFailure: undefined,
    closeRequest: undefined,
    closeBody: undefined,
    closeResult: undefined,
    offset: undefined,
  };
  const outstanding = new Map<number, Entry>();
  const sequences = new Map<number, Deferred.Deferred<ProducerAppendResult, ProducerError>>();
  const progress: SequenceProgress = {
    through: -1,
    failure: undefined,
  };
  const batches = new Set<Batch>();
  const recordOffset = (result: ProducerAppendResult) => {
    if (result.offset !== undefined && (state.offset === undefined || result.offset > state.offset))
      state.offset = result.offset;
  };
  const complete = (input: {
    readonly batch: Batch;
    readonly outcome: Exit.Exit<ProducerAppendResult, ProducerError>;
  }) => {
    if (
      Exit.isFailure(input.outcome) &&
      input.batch.tuple !== undefined &&
      (progress.failure === undefined || input.batch.tuple.seq < progress.failure.seq)
    )
      progress.failure = { seq: input.batch.tuple.seq, exit: Exit.failCause(input.outcome.cause) };
    Deferred.doneUnsafe(input.batch.completion, input.outcome);
    batches.delete(input.batch);
    for (const entry of input.batch.entries) {
      outstanding.delete(entry.id);
      Deferred.doneUnsafe(entry.receipt, input.outcome);
      if (
        Exit.isFailure(input.outcome) &&
        (state.firstFailure === undefined || entry.id < state.firstFailure.id)
      )
        state.firstFailure = { id: entry.id, exit: Exit.failCause(input.outcome.cause) };
    }
    if (Exit.isSuccess(input.outcome)) recordOffset(input.outcome.value);
    while (true) {
      const next = sequences.get(progress.through + 1);
      if (next === undefined || !Deferred.isDoneUnsafe(next)) break;
      sequences.delete(++progress.through);
    }
  };
  const reserve = (batch: Batch) => {
    const tuple = { epoch: state.epoch, seq: state.nextSeq++ };
    batch.tuple = tuple;
    sequences.set(tuple.seq, batch.completion);
    return tuple;
  };
  const send = (initial: ProducerRequest) =>
    Effect.gen(function* () {
      const current = { request: initial };
      while (true) {
        const request = current.request;
        const response = yield* sendProducerRequest(request).pipe(
          Effect.provideContext(executionContext),
        );
        const delivered = yield* ProducerResponse.$match(response, {
          Delivered: ({ result }) => Effect.succeed(Option.some(result)),
          Fenced: (fenced) =>
            Effect.gen(function* () {
              if (!options.autoClaim || fenced.currentEpoch >= Number.MAX_SAFE_INTEGER)
                return yield* new ProducerFencedError({
                  currentEpoch: fenced.currentEpoch,
                  response: fenced.response,
                });
              state.epoch = fenced.currentEpoch + 1;
              state.nextSeq = request.close ? 0 : 1;
              current.request = { ...request, epoch: state.epoch, seq: 0 };
              if (request.close) state.closeRequest = current.request;
              return Option.none<ProducerAppendResult>();
            }),
          Gap: (gap) =>
            Effect.gen(function* () {
              if (
                request.close ||
                gap.expectedSequence >= request.seq ||
                gap.receivedSequence !== request.seq ||
                request.epoch !== state.epoch
              )
                return yield* new ProducerSequenceGapError({
                  expectedSequence: gap.expectedSequence,
                  receivedSequence: gap.receivedSequence,
                  response: gap.response,
                });
              if (progress.failure !== undefined && progress.failure.seq < request.seq)
                return yield* progress.failure.exit;
              const position = { seq: gap.expectedSequence };
              while (position.seq < request.seq) {
                const seq = position.seq++;
                if (seq <= progress.through) continue;
                const completion = sequences.get(seq);
                if (completion === undefined)
                  return yield* new ProducerSequenceGapError({
                    expectedSequence: gap.expectedSequence,
                    receivedSequence: gap.receivedSequence,
                    response: gap.response,
                  });
                yield* Deferred.await(completion);
              }
              return Option.none<ProducerAppendResult>();
            }),
        });
        if (Option.isSome(delivered)) return delivered.value;
      }
    }).pipe(
      Effect.tapError((error) =>
        Predicate.hasProperty(error, "response") ? freezeErrorResponse(error) : Effect.void,
      ),
    );
  const worker = Effect.gen(function* () {
    while (true) {
      const batch = yield* Queue.take(tasks);
      const outcome = yield* Effect.gen(function* () {
        if (!state.claimed && !batch.firstClaim) yield* Deferred.await(claim.current);
        const tuple = batch.tuple ?? reserve(batch);
        if (!Number.isSafeInteger(tuple.seq))
          return yield* new ProtocolViolationError({ component: "producer sequence exhausted" });
        return yield* Effect.acquireUseRelease(
          Effect.sync(() => {
            state.active++;
          }),
          () =>
            send({
              connection: context.connection,
              contentType: batch.contentType,
              producerId: options.producerId,
              ...tuple,
              body: batch.body,
              close: false,
            }),
          () =>
            Effect.sync(() => {
              state.active--;
            }),
        );
      }).pipe(Effect.exit);
      if (batch.firstClaim) {
        if (Exit.isSuccess(outcome)) {
          state.claimed = true;
          for (const held of batches) if (held.tuple === undefined) reserve(held);
        }
        yield* Deferred.done(claim.current, Exit.isSuccess(outcome) ? Exit.void : outcome);
      }
      complete({ batch, outcome });
      if (context.facade !== undefined) yield* context.facade.onBatchExit(outcome);
    }
  });
  const emit = Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      if (!Arr.isArrayNonEmpty(state.pending)) return;
      const entries = state.pending;
      state.pending = [];
      state.bytes = 0;
      state.generation++;
      const timer = state.timer;
      state.timer = undefined;
      if (timer !== undefined) {
        if (context.facade === undefined) yield* Fiber.interrupt(timer);
        else timer.interruptUnsafe();
      }
      const batch: Batch = {
        entries,
        body: combineAppendBodies({
          bodies: entries.map((entry) => entry.body),
          contentType: entries[0].contentType,
        }),
        contentType: entries[0].contentType,
        completion: Deferred.makeUnsafe(),
        tuple: undefined,
        firstClaim: !state.claimed && !state.claiming,
      };
      if (batch.firstClaim) state.claiming = true;
      if (state.claimed || batch.firstClaim) reserve(batch);
      batches.add(batch);
      yield* restore(Queue.offer(tasks, batch)).pipe(
        Effect.onExit((outcome) =>
          Effect.sync(() => {
            if (Exit.isFailure(outcome))
              complete({ batch, outcome: Exit.failCause(outcome.cause) });
          }),
        ),
      );
    }),
  );
  const flush = mutex
    .withPermit(
      Effect.gen(function* () {
        const watermark = state.id;
        const receipts = [...outstanding.values()]
          .filter((entry) => entry.id <= watermark)
          .map((entry) => entry.receipt);
        yield* emit;
        return { watermark, receipts };
      }),
    )
    .pipe(
      Effect.flatMap(({ watermark, receipts }) =>
        Effect.gen(function* () {
          yield* Effect.forEach(receipts, (receipt) => Deferred.await(receipt).pipe(Effect.exit), {
            discard: true,
          });
          if (
            context.facade === undefined &&
            state.firstFailure !== undefined &&
            state.firstFailure.id <= watermark
          )
            return yield* state.firstFailure.exit;
          return undefined;
        }),
      ),
      Effect.raceFirst(Deferred.await(stopped)),
      Effect.withSpan("durable_streams.producer.flush"),
    );
  const admit = (
    input: ProducerAppendInput<S["Type"] | Uint8Array> & { readonly prepared?: PreparedBody },
  ) =>
    Effect.gen(function* () {
      if (state.mode !== "open") return yield* new ProducerClosedError();
      const body =
        input.prepared?.body ??
        (yield* encodePayload({
          ...context,
          value: input.value,
          contentType,
          operation: "append",
        }));
      return yield* mutex.withPermit(
        Effect.gen(function* () {
          if (state.mode !== "open") return yield* new ProducerClosedError();
          const entryContentType = input.prepared?.contentType ?? contentType;
          const firstPending = state.pending[0];
          if (firstPending !== undefined && firstPending.contentType !== entryContentType)
            yield* emit;
          const entry: Entry = {
            id: ++state.id,
            body,
            contentType: entryContentType,
            receipt: Deferred.makeUnsafe(),
          };
          outstanding.set(entry.id, entry);
          state.pending.push(entry);
          state.bytes +=
            body.length -
            (entryContentType.split(";")[0]?.trim().toLowerCase() === "application/json" ? 2 : 0);
          if (state.bytes >= (options.maxBatchBytes ?? 1048576)) yield* emit;
          else if (state.timer === undefined) {
            const generation = state.generation;
            state.timer = yield* Effect.sleep(options.linger ?? Duration.millis(5)).pipe(
              Effect.andThen(
                mutex.withPermit(
                  Effect.gen(function* () {
                    if (generation !== state.generation) return;
                    state.timer = undefined;
                    yield* emit;
                  }),
                ),
              ),
              Effect.forkIn(scope),
            );
          }
          yield* Effect.logDebug("Producer append admitted");
          return entry.receipt;
        }),
      );
    }).pipe((effect) =>
      context.facade === undefined
        ? effect.pipe(Effect.raceFirst(Deferred.await(stopped)))
        : effect,
    );
  yield* Effect.forEach(Arr.range(1, maxInFlight), () => worker.pipe(Effect.forkIn(scope)), {
    discard: true,
  });
  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      state.mode = "stopped";
      yield* Scope.close(scope, Exit.void);
      yield* Queue.shutdown(tasks);
      for (const entry of outstanding.values()) yield* Deferred.interrupt(entry.receipt);
      outstanding.clear();
      state.pending = [];
      batches.clear();
      sequences.clear();
      yield* Deferred.interrupt(stopped);
    }),
  );
  const append = (input: ProducerAppendInput<S["Type"] | Uint8Array>) =>
    admit({ value: input.value }).pipe(
      Effect.flatMap(Deferred.await),
      Effect.withSpan("durable_streams.producer.append"),
    );
  const producer: IdempotentProducer<S["Type"] | Uint8Array, S["EncodingServices"]> = {
    append,
    sink: Sink.forEach((value: S["Type"] | Uint8Array) =>
      Effect.suspend(() =>
        state.firstFailure === undefined ? admit({ value }) : state.firstFailure.exit,
      ),
    ).pipe(Sink.mapEffect(() => flush)),
    flush,
    detach: lifecycle
      .withPermit(
        Effect.gen(function* () {
          if (state.mode === "stopped") return yield* new ProducerClosedError();
          state.mode = "detached";
          yield* flush;
          return undefined;
        }),
      )
      .pipe(Effect.withSpan("durable_streams.producer.detach")),
    restart: lifecycle
      .withPermit(
        Effect.gen(function* () {
          if (state.mode !== "open" && context.facade === undefined)
            return yield* new ProducerClosedError();
          yield* mutex.withPermit(
            Effect.sync(() => {
              if (context.facade === undefined) state.mode = "closing";
            }),
          );
          yield* flush;
          if (state.epoch >= Number.MAX_SAFE_INTEGER)
            return yield* new ProtocolViolationError({ component: "producer epoch exhausted" });
          state.epoch++;
          state.nextSeq = 0;
          if (state.closeResult === undefined) state.closeRequest = undefined;
          progress.through = -1;
          progress.failure = undefined;
          sequences.clear();
          if (context.facade !== undefined) {
            claim.current = Deferred.makeUnsafe();
            state.claimed = !options.autoClaim;
            state.claiming = false;
            state.firstFailure = undefined;
          }
          if (context.facade === undefined) state.mode = "open";
          return undefined;
        }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              if (context.facade === undefined && state.mode === "closing") state.mode = "open";
            }),
          ),
        ),
      )
      .pipe(Effect.withSpan("durable_streams.producer.restart")),
    close: (input) => close(Predicate.hasProperty(input, "value") ? { value: input.value } : {}),
    epoch: Effect.sync(() => state.epoch),
    nextSeq: Effect.sync(() => state.nextSeq),
    pendingCount: Effect.sync(() => state.pending.length),
    inFlightCount: Effect.sync(() => state.active),
    lastSuccessfulOffset: Effect.sync(() => Option.fromUndefinedOr(state.offset)),
  };
  const close = (
    input: ProducerCloseInput<S["Type"] | Uint8Array> & { readonly prepared?: PreparedBody },
  ) =>
    lifecycle
      .withPermit(
        Effect.gen(function* () {
          if (state.closeResult !== undefined) return state.closeResult;
          if (
            state.mode === "stopped" ||
            (state.mode === "detached" && context.facade === undefined)
          )
            return yield* new ProducerClosedError();
          if (state.closeBody === undefined) {
            const body =
              input.prepared !== undefined
                ? input.prepared.body
                : Predicate.hasProperty(input, "value")
                  ? yield* encodePayload({
                      ...context,
                      value: input.value,
                      contentType,
                      operation: "close",
                    })
                  : undefined;
            yield* mutex.withPermit(
              Effect.sync(() => {
                state.mode = "closing";
                state.closeBody = Record.filter({ body }, Predicate.isNotUndefined);
              }),
            );
          }
          if (state.closeRequest === undefined) {
            yield* flush;
            state.closeRequest = {
              connection: context.connection,
              contentType:
                input.prepared?.contentType ?? context.facade?.contentType() ?? contentType,
              producerId: options.producerId,
              epoch: state.epoch,
              seq: state.nextSeq,
              close: true,
              ...state.closeBody,
            };
          }
          const result = yield* context.facade !== undefined
            ? send(state.closeRequest)
            : send(state.closeRequest).pipe(
                Effect.forkIn(scope),
                Effect.flatMap((fiber) =>
                  Fiber.join(fiber).pipe(Effect.onInterrupt(() => Fiber.interrupt(fiber))),
                ),
              );
          if (result.offset === undefined)
            return yield* new ProtocolViolationError({ component: "producer close offset" });
          state.nextSeq++;
          recordOffset(result);
          state.closeResult = CloseResult.make({ finalOffset: result.offset });
          return state.closeResult;
        }),
      )
      .pipe(
        Effect.raceFirst(Deferred.await(stopped)),
        Effect.withSpan("durable_streams.producer.close"),
      );
  return {
    ...producer,
    native: producer,
    admitPrepared: (prepared: PreparedBody) => admit({ value: null, prepared }),
    closePrepared: (prepared: PreparedBody | undefined) => close({ prepared }),
    releaseWorkers: Scope.close(scope, Exit.void).pipe(Effect.andThen(Queue.shutdown(tasks))),
    snapshot: () => ({
      epoch: state.epoch,
      nextSeq: state.nextSeq,
      pendingCount: state.pending.length,
      inFlightCount: batches.size,
      lastSuccessfulOffset: state.offset,
    }),
  };
});
