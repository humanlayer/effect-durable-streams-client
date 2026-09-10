import {
  Context,
  Duration,
  Effect,
  Exit,
  Match,
  Predicate,
  Record,
  Schema,
  Scope,
  Stream,
} from "effect";
import { PayloadEncodeError, InvalidDurableStreamsConfigError } from "./errors.js";
import { allocateOrdinaryAppends, appendSource } from "./append.js";
import { encodePayload, prepareSerializedBody, type PreparedBody } from "./encoding.js";
import { closeStream, createStream, deleteStream } from "./lifecycle.js";
import { inspectStream } from "./transport.js";
import {
  ContentType,
  ProducerOptions,
  StreamLifetime,
  type DurableStreamsConnection,
} from "./model.js";
import { acquireProducer } from "./producer.js";
import {
  createClientRuntime,
  createBoundRuntime,
  type ClientOwner,
  mergeHeaders,
  parseClientConnection,
  runClientSync,
  validateTransportOptions,
  type TransportOptions,
  type MaybePromise,
} from "./client-runtime.js";
import {
  DurableStreamError,
  InvalidClientOptionsError,
  mapClientErrors,
  unwrapClientExit,
} from "./client-errors.js";
import { decodeReadJson, openStreamResponse } from "./client-response.js";

export {
  DurableStreamError,
  InvalidClientOptionsError,
  AbortError,
  ClientInternalError,
  StreamClosedError,
  StaleEpochError,
  SequenceGapError,
} from "./client-errors.js";
export type { DurableStreamErrorCode, ErrorResponseSnapshot } from "./client-errors.js";
export type { HeadersRecord, ParamsRecord, MaybePromise } from "./client-runtime.js";
export type {
  StreamResponse,
  BatchMeta,
  JsonBatch,
  ByteChunk,
  TextChunk,
  ReadableStreamAsyncIterable,
} from "./client-response.js";

export type SerializedBody = string | Uint8Array;
export type JsonValue = Schema.Json;
export type LiveMode = boolean | "long-poll" | "sse";
export type BackoffOptions = NonNullable<DurableStreamsConnection["backoffOptions"]>;
export type SSEResilienceOptions = NonNullable<DurableStreamsConnection["sseResilience"]>;
export type DurableStreamOptions = TransportOptions & {
  readonly url: string | URL;
  readonly contentType?: string;
  readonly batching?: boolean;
  readonly backoffOptions?: BackoffOptions;
};
export type CreateInput = {
  readonly contentType?: string;
  readonly body?: SerializedBody;
  readonly closed?: boolean;
} & (
  | { readonly ttlSeconds?: number; readonly expiresAt?: never }
  | { readonly expiresAt: string; readonly ttlSeconds?: never }
);
export type CreateOptions = DurableStreamOptions & CreateInput;
export type AppendOptions = {
  readonly seq?: string;
  readonly contentType?: string;
  readonly signal?: AbortSignal;
};
export type CloseOptions = {
  readonly body?: SerializedBody;
  readonly contentType?: string;
  readonly signal?: AbortSignal;
};
export type ReadOptions = TransportOptions & {
  readonly offset?: string;
  readonly live?: LiveMode;
  readonly json?: boolean;
  readonly backoffOptions?: BackoffOptions;
  readonly sseResilience?: SSEResilienceOptions;
};
export type HeadResult =
  | { readonly exists: false }
  | {
      readonly exists: true;
      readonly contentType: string;
      readonly offset: string;
      readonly streamClosed: boolean;
      readonly etag?: string;
      readonly cacheControl?: string;
    };
export type CloseResult = { readonly finalOffset: string };

type Binding = {
  connection: DurableStreamsConnection;
  readonly options: DurableStreamOptions;
  runtime: ReturnType<typeof createClientRuntime>;
  runtimeFor: (options: TransportOptions) => ReturnType<typeof createClientRuntime>;
  readonly append: Effect.Success<typeof allocateOrdinaryAppends>;
};
const _bindings = new WeakMap<DurableStream, Binding>();

export class DurableStream {
  readonly #binding: Binding;
  constructor(options: DurableStreamOptions) {
    if (Object.hasOwn(options, "onError")) throw new InvalidClientOptionsError();
    const url = String(options.url);
    validateTransportOptions({ ...options, url });
    const stored = {
      ...options,
      headers: mergeHeaders(undefined, options.headers),
      params: { ...options.params },
    };
    this.#binding = {
      options: stored,
      connection: parseClientConnection({
        url,
        ...Record.filter({ contentType: options.contentType }, Predicate.isNotUndefined),
        ...Record.filter({ batching: options.batching }, Predicate.isNotUndefined),
        ...Record.filter({ backoffOptions: options.backoffOptions }, Predicate.isNotUndefined),
      }),
      runtime: createClientRuntime(stored),
      runtimeFor: createClientRuntime,
      append: runClientSync(allocateOrdinaryAppends),
    };
    _bindings.set(this, this.#binding);
  }
  get url() {
    return this.#binding.connection.url.href;
  }
  get contentType() {
    return this.#binding.connection.contentType;
  }
  static async create(options: CreateOptions) {
    const handle = new DurableStream(options);
    await handle.create(options);
    return handle;
  }
  static async connect(options: DurableStreamOptions) {
    const handle = new DurableStream(options);
    await handle.head();
    return handle;
  }
  static async head(options: DurableStreamOptions) {
    return new DurableStream(options).head();
  }
  static async delete(options: DurableStreamOptions) {
    return new DurableStream(options).delete();
  }
  async head(options: { readonly signal?: AbortSignal } = {}) {
    const metadata = await this.#binding.runtime.run(
      inspectStream({ connection: this.#binding.connection, operation: "head" }),
      options.signal ?? this.#binding.options.signal,
    );
    const result: HeadResult = Match.value(metadata).pipe(
      Match.tagsExhaustive({
        Missing: () => ({ exists: false as const }),
        Existing: (value) => {
          this.#binding.connection = {
            ...this.#binding.connection,
            contentType: value.contentType,
          };
          return {
            exists: true as const,
            contentType: value.contentType,
            offset: value.offset,
            streamClosed: value.closed,
            ...Record.filter({ etag: value.etag }, Predicate.isNotUndefined),
            ...Record.filter({ cacheControl: value.cacheControl }, Predicate.isNotUndefined),
          };
        },
      }),
    );
    return result;
  }
  async create(options: CreateInput = {}) {
    if (options.ttlSeconds !== undefined && options.expiresAt !== undefined)
      throw new InvalidClientOptionsError();
    const contentType = options.contentType ?? this.contentType ?? "application/octet-stream";
    const prepared =
      options.body === undefined
        ? undefined
        : runClientSync(
            prepareSerializedBody({ value: options.body, contentType, complete: true }),
          );
    const lifetime =
      options.ttlSeconds !== undefined
        ? runClientSync(
            StreamLifetime.cases.Ttl.makeEffect({ ttlSeconds: options.ttlSeconds }).pipe(
              Effect.mapError(
                () =>
                  new InvalidDurableStreamsConfigError({
                    field: "ttlSeconds",
                    issues: ["Invalid lifetime"],
                  }),
              ),
            ),
          )
        : options.expiresAt !== undefined
          ? runClientSync(
              StreamLifetime.cases.ExpiresAt.makeEffect({ expiresAt: options.expiresAt }).pipe(
                Effect.mapError(
                  () =>
                    new InvalidDurableStreamsConfigError({
                      field: "expiresAt",
                      issues: ["Invalid lifetime"],
                    }),
                ),
              ),
            )
          : undefined;
    const result = await this.#binding.runtime.run(
      createStream<typeof Schema.Json>({
        connection: this.#binding.connection,
        input: {
          contentType,
          ...Record.filter({ closed: options.closed }, Predicate.isNotUndefined),
          ...Record.filter({ lifetime }, Predicate.isNotUndefined),
        },
        ...Record.filter({ prepared }, Predicate.isNotUndefined),
      }),
      this.#binding.options.signal,
    );
    this.#binding.connection = { ...this.#binding.connection, contentType: result.contentType };
    return this;
  }
  async delete(options: { readonly signal?: AbortSignal } = {}) {
    await this.#binding.runtime.run(
      deleteStream(this.#binding.connection),
      options.signal ?? this.#binding.options.signal,
    );
  }
  async append(body: MaybePromise<SerializedBody>, options: AppendOptions = {}) {
    if (["producerId", "producerEpoch", "producerSeq"].some((key) => Object.hasOwn(options, key)))
      throw new InvalidClientOptionsError();
    const contentType = options.contentType ?? this.contentType ?? "application/octet-stream";
    const prepared = runClientSync(
      prepareSerializedBody({
        value: Predicate.isPromise(body)
          ? await this.#binding.runtime.run(
              Effect.tryPromise({
                try: () => body,
                catch: () => new PayloadEncodeError({ component: "append body" }),
              }),
              options.signal ?? this.#binding.options.signal,
            )
          : body,
        contentType,
        complete: false,
      }),
    );
    await this.#binding.runtime.run(
      this.#binding.append<typeof Schema.Json>({
        connection: this.#binding.connection,
        input: { value: null, ...Record.filter({ seq: options.seq }, Predicate.isNotUndefined) },
        prepared,
      }),
      options.signal ?? this.#binding.options.signal,
    );
  }
  async close(options: CloseOptions = {}) {
    const contentType = options.contentType ?? this.contentType ?? "application/octet-stream";
    const prepared =
      options.body === undefined
        ? { contentType }
        : runClientSync(
            prepareSerializedBody({ value: options.body, contentType, complete: false }),
          );
    return this.#binding.runtime.run(
      closeStream<typeof Schema.Json>({
        connection: this.#binding.connection,
        input: options.body === undefined ? {} : { value: null },
        prepared,
      }),
      options.signal ?? this.#binding.options.signal,
    );
  }
  async appendStream(
    source: AsyncIterable<SerializedBody> | ReadableStream<SerializedBody>,
    options: AppendOptions = {},
  ) {
    if (["producerId", "producerEpoch", "producerSeq"].some((key) => Object.hasOwn(options, key)))
      throw new InvalidClientOptionsError();
    const contentType = runClientSync(
      Schema.decodeEffect(ContentType)(
        options.contentType ?? this.contentType ?? "application/octet-stream",
      ).pipe(Effect.mapError(() => new PayloadEncodeError({ component: "content-type" }))),
    );
    const chunks = Schema.is(Schema.instanceOf(ReadableStream<SerializedBody>))(source)
      ? Stream.fromReadableStream({
          evaluate: () => source,
          onError: () => new PayloadEncodeError({ component: "upload source" }),
        })
      : Stream.fromAsyncIterable(
          source,
          () => new PayloadEncodeError({ component: "upload source" }),
        );
    await this.#binding.runtime.run(
      appendSource({
        connection: {
          ...this.#binding.connection,
          contentType,
        },
        hasSchema: false,
        input: { source: chunks, ...Record.filter({ seq: options.seq }, Predicate.isNotUndefined) },
      }),
      options.signal ?? this.#binding.options.signal,
    );
  }
  async stream(options: ReadOptions = {}) {
    return _openClientRead({
      handle: this,
      options,
      requireJson: options.json === true,
      decoder: (batch) => decodeReadJson({ batch, schema: Schema.Json }),
    });
  }
  withSchema<S extends ServiceFreeSchema>(schema: S) {
    return _specializeClient({ raw: this, schema, context: Context.empty() });
  }
  static withSchema<S extends ServiceFreeSchema>(
    options: DurableStreamOptions & { readonly schema: S },
  ) {
    return new DurableStream({
      ...options,
      contentType: options.contentType ?? "application/json",
    }).withSchema(options.schema);
  }
  static async createWithSchema<S extends ServiceFreeSchema>(
    options: CreateOptions & { readonly schema: S },
  ) {
    const client = DurableStream.withSchema(options);
    await client.create(options);
    return client;
  }
  writable(options: WritableOptions = {}) {
    const delivery = new Map<"first", DurableStreamError>();
    const producer = new IdempotentProducer(
      this,
      // oxlint-disable-next-line automation/no-ambient-nondeterminism, effecttsgo/crypto-random-uuid -- SAFETY: the foreign Web runtime owns generation of the optional producer identity.
      options.producerId ?? globalThis.crypto.randomUUID(),
      {
        ...options,
        autoClaim: true,
        onError: (error) => {
          if (!delivery.has("first")) delivery.set("first", error);
          options.onError?.(error);
        },
      },
    );
    const writable: globalThis.WritableStream<SerializedBody> = new WritableStream<SerializedBody>({
      write: (body) => producer.append(body),
      close: async () => {
        await producer.close();
        const failure = delivery.get("first");
        if (failure !== undefined) throw failure;
      },
      abort: () => {
        void producer.detach();
      },
    });
    return writable;
  }
}

const _openClientRead = async <A>(input: {
  readonly handle: DurableStream;
  readonly options: ReadOptions;
  readonly requireJson: boolean;
  readonly decoder: (
    batch: import("./read.js").ReadBatch,
  ) => Effect.Effect<Array<A>, import("./errors.js").PayloadDecodeError>;
}) => {
  const { handle, options, requireJson, decoder } = input;
  if (Object.hasOwn(options, "onError")) throw new InvalidClientOptionsError();
  if (![undefined, false, true, "long-poll", "sse"].includes(options.live))
    throw new InvalidClientOptionsError();
  const binding = _bindings.get(handle);
  if (binding === undefined) throw new InvalidClientOptionsError();
  const transport = {
    ...binding.options,
    ...options,
    headers: mergeHeaders(binding.options.headers, options.headers),
    params: { ...binding.options.params, ...options.params },
  };
  validateTransportOptions({ ...transport, url: handle.url });
  const connection = parseClientConnection({
    url: handle.url,
    offset: options.offset ?? "-1",
    ...Record.filter(
      {
        live: Match.value(options.live).pipe(
          Match.when(false, () => undefined),
          Match.when("sse", () => "sse" as const),
          Match.orElse(() => "long-poll" as const),
        ),
      },
      Predicate.isNotUndefined,
    ),
    ...Record.filter({ backoffOptions: transport.backoffOptions }, Predicate.isNotUndefined),
    ...Record.filter({ sseResilience: options.sseResilience }, Predicate.isNotUndefined),
  });
  return openStreamResponse({
    connection,
    runtime: binding.runtimeFor(transport),
    signal: transport.signal,
    live: options.live ?? true,
    requireJson,
    decodeJson: decoder,
  });
};

export type ServiceFreeSchema = Schema.Top & {
  readonly EncodingServices: never;
  readonly DecodingServices: never;
};
const _specializeClient = <S extends Schema.Top>(input: {
  readonly raw: DurableStream;
  readonly schema: S;
  readonly context: Context.Context<S["EncodingServices"] | S["DecodingServices"]>;
}) => {
  const { raw, schema, context } = input;
  if (
    !Schema.isSchema(schema) ||
    (raw.contentType !== undefined &&
      raw.contentType.split(";")[0]?.trim().toLowerCase() !== "application/json")
  )
    throw new InvalidClientOptionsError();
  const binding = _bindings.get(raw);
  if (binding === undefined) throw new InvalidClientOptionsError();
  const appendJson = async (value: S["Type"], options: AppendOptions = {}) => {
    const contentType = options.contentType ?? raw.contentType ?? "application/json";
    await binding.runtime.run(
      encodePayload({ schema, value, contentType, operation: "append" }).pipe(
        Effect.provide(context),
        Effect.flatMap((body) =>
          binding.append<typeof Schema.Json>({
            connection: binding.connection,
            input: {
              value: null,
              ...Record.filter({ seq: options.seq }, Predicate.isNotUndefined),
            },
            prepared: { body, contentType },
          }),
        ),
      ),
      options.signal ?? binding.options.signal,
    );
  };
  return {
    raw,
    get url() {
      return raw.url;
    },
    get contentType() {
      return raw.contentType;
    },
    head: raw.head.bind(raw),
    create: raw.create.bind(raw),
    delete: raw.delete.bind(raw),
    append: raw.append.bind(raw),
    close: raw.close.bind(raw),
    appendJson,
    appendJsonBatch: async (values: ReadonlyArray<S["Type"]>, options: AppendOptions = {}) => {
      for (const value of values) await appendJson(value, options);
    },
    stream: (options: ReadOptions = {}) =>
      _openClientRead({
        handle: raw,
        options,
        requireJson: true,
        decoder: (batch) => decodeReadJson({ batch, schema }).pipe(Effect.provide(context)),
      }),
  };
};
export type TypedDurableStream<A> = {
  readonly raw: DurableStream;
  readonly url: string;
  readonly contentType: string | undefined;
  readonly head: DurableStream["head"];
  readonly create: DurableStream["create"];
  readonly delete: DurableStream["delete"];
  readonly append: DurableStream["append"];
  readonly close: DurableStream["close"];
  appendJson(value: A, options?: AppendOptions): Promise<void>;
  appendJsonBatch(values: ReadonlyArray<A>, options?: AppendOptions): Promise<void>;
  stream(options?: ReadOptions): Promise<import("./client-response.js").StreamResponse<A>>;
};
type StaticTransport = {
  readonly headers?: Readonly<Record<string, string>>;
  readonly params?: Readonly<Record<string, string | undefined>>;
};
export type EffectClientOptions = Omit<DurableStreamOptions, "fetch" | "headers" | "params"> &
  StaticTransport;
export type EffectReadOptions = Omit<ReadOptions, "fetch" | "headers" | "params"> & StaticTransport;
export type EffectRawClient = Omit<DurableStream, "stream" | "writable" | "withSchema"> & {
  stream(
    options?: EffectReadOptions,
  ): Promise<import("./client-response.js").StreamResponse<JsonValue>>;
};
export const makeEffectClient = <S extends Schema.Top>(
  options: EffectClientOptions & { readonly schema: S },
) =>
  Effect.gen(function* () {
    const context = yield* Effect.context<
      | import("effect/unstable/http").HttpClient.HttpClient
      | S["EncodingServices"]
      | S["DecodingServices"]
    >();
    const owner: ClientOwner = {
      // oxlint-disable-next-line effecttsgo/abort-controller-in-effect -- SAFETY: this controller joins borrowed parent scope lifetime to the foreign Promise API.
      controller: new AbortController(),
      scopes: new Set(),
      pending: new Set(),
    };
    yield* Effect.addFinalizer(() =>
      Effect.promise(async () => {
        owner.controller.abort();
        await Promise.all(
          [...owner.scopes].map((scope) => Effect.runPromiseExit(Scope.close(scope, Exit.void))),
        );
        await Promise.all(owner.pending);
      }),
    );
    if (Object.hasOwn(options, "fetch")) return yield* Effect.fail(new InvalidClientOptionsError());
    const raw = yield* Effect.try({
      try: () =>
        new DurableStream({ ...options, contentType: options.contentType ?? "application/json" }),
      catch: () => new InvalidClientOptionsError(),
    });
    const binding = _bindings.get(raw);
    if (binding === undefined) return yield* Effect.die(new Error("Missing client binding"));
    binding.runtimeFor = (transport) => {
      if (
        transport.fetch !== undefined ||
        Object.values(transport.headers ?? {}).some(Predicate.isFunction) ||
        Object.values(transport.params ?? {}).some(Predicate.isFunction)
      )
        throw new InvalidClientOptionsError();
      return createBoundRuntime({ options: transport, context, owner });
    };
    const client = yield* Effect.try({
      try: () => {
        binding.runtime = binding.runtimeFor(binding.options);
        return _specializeClient({ raw, schema: options.schema, context });
      },
      catch: () => new InvalidClientOptionsError(),
    });
    const advancedRaw: EffectRawClient = raw;
    return {
      ...client,
      raw: advancedRaw,
      stream: (readOptions: EffectReadOptions = {}) => client.stream(readOptions),
    };
  });

export const stream = async (options: ReadOptions & { readonly url: string | URL }) =>
  new DurableStream(options).stream(options);

export type IdempotentProducerOptions = TransportOptions & {
  readonly epoch?: number;
  readonly autoClaim?: boolean;
  readonly maxBatchBytes?: number;
  readonly lingerMs?: number;
  readonly maxInFlight?: number;
  readonly onError?: (error: DurableStreamError) => void;
};
export type WritableOptions = Pick<
  IdempotentProducerOptions,
  "headers" | "lingerMs" | "maxBatchBytes" | "onError" | "signal"
> & { readonly producerId?: string };

type Coordinator = Effect.Success<ReturnType<typeof acquireProducer<typeof Schema.Json>>>;
export class IdempotentProducer {
  readonly #binding: Binding;
  readonly #options: IdempotentProducerOptions;
  readonly #input: ProducerOptions;
  readonly #scope = Scope.makeUnsafe();
  #coordinator: Coordinator | undefined;
  #acquisition: Promise<Coordinator> | undefined;
  #ingress = Promise.resolve();
  #waiting = 0;
  #closed = false;
  #finalBody: PreparedBody | undefined;
  #closeStarted = false;
  readonly #abort = () => {
    void this.#binding.runtime.closeScope(this.#scope);
  };
  constructor(handle: DurableStream, producerId: string, options: IdempotentProducerOptions = {}) {
    const binding = _bindings.get(handle);
    if (binding === undefined) throw new InvalidClientOptionsError();
    const transport = {
      ...binding.options,
      ...options,
      headers: mergeHeaders(binding.options.headers, options.headers),
      params: { ...binding.options.params, ...options.params },
    };
    validateTransportOptions({ ...transport, url: handle.url });
    this.#binding = {
      ...binding,
      get connection() {
        return binding.connection;
      },
      runtime: binding.runtimeFor(transport),
    };
    this.#options = { ...options, signal: options.signal ?? binding.options.signal };
    if (
      (options.lingerMs !== undefined &&
        (!Number.isFinite(options.lingerMs) || options.lingerMs < 0)) ||
      (options.onError !== undefined && !Predicate.isFunction(options.onError))
    )
      throw new InvalidClientOptionsError();
    this.#input = runClientSync(
      ProducerOptions.makeEffect({
        producerId,
        ...Record.filter(
          {
            epoch: options.epoch,
            autoClaim: options.autoClaim,
            maxBatchBytes: options.maxBatchBytes,
            maxInFlight: options.maxInFlight,
          },
          Predicate.isNotUndefined,
        ),
        linger: Duration.millis(options.lingerMs ?? 5),
      }).pipe(
        Effect.mapError(
          () =>
            new InvalidDurableStreamsConfigError({
              field: "producer",
              issues: ["Invalid options"],
            }),
        ),
      ),
    );
    this.#binding.runtime.own(this.#scope);
    this.#options.signal?.addEventListener("abort", this.#abort, { once: true });
  }
  #acquire() {
    this.#acquisition ??= this.#binding.runtime
      .run(
        acquireProducer<typeof Schema.Json>({
          connection: this.#binding.connection,
          input: this.#input,
          facade: {
            contentType: () => this.#binding.connection.contentType ?? "application/octet-stream",
            onBatchExit: (exit) =>
              Effect.sync(() => {
                if (Exit.isFailure(exit)) {
                  const mapped = Effect.runSyncExit(mapClientErrors(Effect.failCause(exit.cause)));
                  this.#notify(mapped);
                }
              }),
          },
        }).pipe(
          Effect.tap((coordinator) =>
            Effect.sync(() => {
              this.#coordinator = coordinator;
            }),
          ),
          Effect.provideService(Scope.Scope, this.#scope),
        ),
        this.#options.signal,
      )
      .then((coordinator) => {
        this.#coordinator = coordinator;
        return coordinator;
      });
    return this.#acquisition;
  }
  #notify(exit: Exit.Exit<never, DurableStreamError>) {
    try {
      unwrapClientExit(exit);
    } catch (error) {
      if (Predicate.isError(error) && error instanceof DurableStreamError) {
        try {
          this.#options.onError?.(error);
        } catch {
          Effect.runSync(Effect.logWarning("Producer onError callback threw"));
        }
      }
    }
  }
  append(body: SerializedBody) {
    if (this.#closed) throw new DurableStreamError({ code: "ALREADY_CLOSED" });
    const prepared = runClientSync(
      prepareSerializedBody({
        value: body,
        contentType: this.#binding.connection.contentType ?? "application/octet-stream",
        complete: false,
      }),
    );
    const acquisition = this.#acquire();
    if (this.#coordinator !== undefined && this.#waiting === 0) {
      const admission = this.#binding.runtime.run(
        this.#coordinator.admitPrepared(prepared),
        this.#options.signal,
      );
      this.#ingress = Promise.all([this.#ingress, admission])
        .then(() => undefined)
        .catch((error) => {
          if (error instanceof DurableStreamError) this.#notify(Exit.fail(error));
        });
      void acquisition.catch(() => undefined);
      return;
    }
    this.#waiting++;
    this.#ingress = this.#ingress
      .then(async () => {
        const coordinator = await acquisition;
        await this.#binding.runtime.run(coordinator.admitPrepared(prepared), this.#options.signal);
      })
      .catch((error) => {
        if (error instanceof DurableStreamError) this.#notify(Exit.fail(error));
      })
      .finally(() => {
        this.#waiting--;
      });
  }
  async flush() {
    do {
      await this.#ingress;
      if (this.#coordinator !== undefined)
        await this.#binding.runtime.run(this.#coordinator.flush, this.#options.signal);
    } while (
      this.#waiting > 0 ||
      (this.#coordinator?.snapshot().pendingCount ?? 0) > 0 ||
      (this.#coordinator?.snapshot().inFlightCount ?? 0) > 0
    );
  }
  async detach() {
    if (this.#closed) return;
    this.#closed = true;
    try {
      await this.flush();
    } catch {
      Effect.runSync(Effect.logWarning("Producer detach drain failed"));
    } finally {
      if (this.#coordinator !== undefined)
        await this.#binding.runtime.run(this.#coordinator.releaseWorkers);
      this.#options.signal?.removeEventListener("abort", this.#abort);
    }
  }
  async close(finalMessage?: SerializedBody) {
    const alreadyClosed = this.#closed;
    if (!this.#closeStarted) {
      this.#finalBody =
        finalMessage === undefined || alreadyClosed
          ? undefined
          : runClientSync(
              prepareSerializedBody({
                value: finalMessage,
                contentType: this.#binding.connection.contentType ?? "application/octet-stream",
                complete: false,
              }),
            );
      this.#closeStarted = true;
    }
    this.#closed = true;
    await this.flush();
    const coordinator = await this.#acquire();
    try {
      return await this.#binding.runtime.run(
        coordinator.closePrepared(this.#finalBody),
        this.#options.signal,
      );
    } finally {
      await this.#binding.runtime.run(coordinator.releaseWorkers);
      this.#options.signal?.removeEventListener("abort", this.#abort);
    }
  }
  async restart() {
    await this.flush();
    const coordinator = await this.#acquire();
    await this.#binding.runtime.run(coordinator.restart, this.#options.signal);
  }
  get epoch() {
    return this.#coordinator?.snapshot().epoch ?? this.#input.epoch ?? 0;
  }
  get nextSeq() {
    return this.#coordinator?.snapshot().nextSeq ?? 0;
  }
  get pendingCount() {
    return this.#waiting + (this.#coordinator?.snapshot().pendingCount ?? 0);
  }
  get inFlightCount() {
    return this.#coordinator?.snapshot().inFlightCount ?? 0;
  }
  get lastSuccessfulOffset() {
    return this.#coordinator?.snapshot().lastSuccessfulOffset;
  }
}
