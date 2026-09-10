import { Array as Arr, Deferred, Effect, Exit, Queue, Schema, Scope, Stream } from "effect";
import { allocateRead, type ReadBatch, type ReadBoundary } from "./read.js";
import { allocateTextDecoder, decodeJson } from "./encoding.js";
import { PayloadDecodeError } from "./errors.js";
import type { DurableStreamsConnection } from "./model.js";
import { AbortError, DurableStreamError, unwrapClientExit } from "./client-errors.js";
import type { createClientRuntime } from "./client-runtime.js";
import type { LiveMode } from "./async-await.js";

export type BatchMeta = ReadBoundary;
export type JsonBatch<A> = BatchMeta & { readonly items: ReadonlyArray<A> };
export type ByteChunk = BatchMeta & { readonly data: Uint8Array };
export type TextChunk = BatchMeta & { readonly text: string };
export type ReadableStreamAsyncIterable<A> = ReadableStream<A> & AsyncIterable<A>;
type ResponseState = {
  consumed: boolean;
  terminal: boolean;
  active: boolean;
  json: boolean;
  metadata: ReadBoundary;
  contentType: string | undefined;
};
export type StreamResponse<A> = {
  readonly url: string;
  readonly contentType: string | undefined;
  readonly live: LiveMode;
  readonly startOffset: string;
  readonly headers: Headers;
  readonly status: number;
  readonly statusText: string;
  readonly ok: boolean;
  readonly isLoading: false;
  readonly offset: string;
  readonly cursor: string | undefined;
  readonly upToDate: boolean;
  readonly streamClosed: boolean;
  readonly closed: Promise<void>;
  body(): Promise<Uint8Array>;
  text(): Promise<string>;
  json(): Promise<A[]>;
  bodyStream(): ReadableStreamAsyncIterable<Uint8Array>;
  textStream(): ReadableStreamAsyncIterable<string>;
  jsonStream(): ReadableStreamAsyncIterable<A>;
  subscribeJson(callback: (batch: JsonBatch<A>) => void | Promise<void>): () => void;
  subscribeBytes(callback: (chunk: ByteChunk) => void | Promise<void>): () => void;
  subscribeText(callback: (chunk: TextChunk) => void | Promise<void>): () => void;
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- SAFETY: Web cancellation reasons are ignored, never interpreted or exposed as diagnostics.
  cancel(reason?: unknown): void;
};

const _concat = (chunks: ReadonlyArray<Uint8Array>) => {
  const bytes = new Uint8Array(chunks.reduce((size, chunk) => size + chunk.length, 0));
  const cursor = { offset: 0 };
  for (const chunk of chunks) {
    bytes.set(chunk, cursor.offset);
    cursor.offset += chunk.length;
  }
  return bytes;
};

export const decodeReadJson = <S extends Schema.Top>({
  batch,
  schema,
}: {
  readonly batch: ReadBatch;
  readonly schema: S;
}) =>
  !Arr.isReadonlyArrayNonEmpty(batch.chunks) && (batch.sse || batch.bodyless)
    ? Effect.succeed<Array<S["Type"]>>([])
    : (batch.sse
        ? Stream.fromIterable(batch.chunks).pipe(
            Stream.flatMap((chunk) => decodeJson({ source: Stream.succeed(chunk), schema })),
          )
        : decodeJson({ source: Stream.fromIterable(batch.chunks), schema })
      ).pipe(Stream.runCollect);

const _callback = <A>(value: A, fn: (value: A) => void | Promise<void>) =>
  Effect.tryPromise({
    try: async () => fn(value),
    catch: () => new PayloadDecodeError({ component: "subscription callback" }),
  });
const _metadata = (batch: ResponseState["metadata"]) =>
  ({
    offset: batch.offset,
    cursor: batch.cursor,
    upToDate: batch.upToDate,
    streamClosed: batch.streamClosed,
  }) satisfies BatchMeta;

export const openStreamResponse = async <A>(input: {
  readonly connection: DurableStreamsConnection;
  readonly runtime: ReturnType<typeof createClientRuntime>;
  readonly signal: AbortSignal | undefined;
  readonly live: LiveMode;
  readonly requireJson: boolean;
  readonly decodeJson: (batch: ReadBatch) => Effect.Effect<Array<A>, PayloadDecodeError>;
}) => {
  const signal =
    input.runtime.signal === undefined
      ? input.signal
      : AbortSignal.any(
          [input.runtime.signal, input.signal].filter((value) => value !== undefined),
        );
  const scope = Scope.makeUnsafe();
  input.runtime.own(scope);
  const controller = new AbortController();
  const completed = Deferred.makeUnsafe<void, DurableStreamError>();
  const completion = {
    promise: Effect.runPromiseExit(Deferred.await(completed)).then(unwrapClientExit),
  };
  void completion.promise.catch(() => undefined);
  const state: ResponseState = {
    consumed: false,
    terminal: false,
    active: false,
    json: input.requireJson,
    contentType: undefined,
    metadata: {
      offset: input.connection.offset ?? "-1",
      cursor: undefined,
      upToDate: false,
      streamClosed: false,
    },
  };
  const finish = async (error?: DurableStreamError) => {
    if (state.terminal)
      return completion.promise.then(
        () => undefined,
        () => undefined,
      );
    state.terminal = true;
    signal?.removeEventListener("abort", cancel);
    await input.runtime.closeScope(scope);
    Deferred.doneUnsafe(completed, error === undefined ? Exit.void : Exit.fail(error));
    return undefined;
  };
  const cancel = () => {
    if (state.terminal) return;
    controller.abort();
    if (!state.active) void finish();
  };
  signal?.addEventListener("abort", cancel, { once: true });
  if (signal?.aborted) controller.abort();
  const read = await input.runtime.run(
    allocateRead({
      connection: input.connection,
      schema: Schema.Json,
      hasSchema: input.requireJson,
    }),
  );
  try {
    const initial = await input.runtime.run(
      read.acquireInitial.pipe(Effect.provideService(Scope.Scope, scope)),
      controller.signal,
    );
    state.contentType = initial.headers["content-type"];
  } catch (error) {
    await finish(error instanceof DurableStreamError ? error : undefined);
    throw error;
  }
  const claim = () => {
    if (state.consumed) throw new DurableStreamError({ code: "ALREADY_CONSUMED" });
    state.consumed = true;
    if (controller.signal.aborted) throw new AbortError();
  };
  const textDecoder = allocateTextDecoder();
  const textState = { selected: false, finite: false, pending: "" };
  const complete = Effect.suspend(() =>
    textState.selected ? textDecoder.complete : Effect.succeed(true),
  );
  const consume = async (options: {
    readonly stopAtTail: boolean;
    readonly incremental?: boolean;
    readonly deliver: (batch: ReadBatch) => Effect.Effect<void, PayloadDecodeError>;
  }) => {
    state.active = true;
    textState.finite = options.stopAtTail || input.live === false;
    try {
      await input.runtime.run(
        read
          .batches({
            stopAtTail: options.stopAtTail,
            complete,
            requireJson: state.json,
            incremental: options.incremental,
          })
          .pipe(
            Stream.runForEach((batch) =>
              options.deliver(batch).pipe(
                Effect.andThen(
                  complete.pipe(
                    Effect.map((ready) => {
                      if (ready && !batch.partial) state.metadata = _metadata(batch);
                    }),
                  ),
                ),
              ),
            ),
            Effect.provideService(Scope.Scope, scope),
          ),
        controller.signal,
      );
      await finish();
    } catch (error) {
      await finish(
        error instanceof AbortError
          ? undefined
          : error instanceof DurableStreamError
            ? error
            : new DurableStreamError({ code: "INTERNAL_ERROR" }),
      );
      throw error;
    }
  };
  const ensureJson = () => {
    state.json = true;
    if (state.contentType?.split(";")[0]?.trim().toLowerCase() !== "application/json") {
      const error = new DurableStreamError({
        code: "PROTOCOL_ERROR",
        component: "read content-type",
      });
      void finish(error);
      throw error;
    }
  };
  const subscribe = (
    deliver: (batch: ReadBatch) => Effect.Effect<void, PayloadDecodeError>,
    options: { readonly text?: boolean; readonly json?: boolean } = {},
  ) => {
    claim();
    if (options.json) ensureJson();
    textState.selected = options.text ?? false;
    void consume({ stopAtTail: false, deliver }).catch(() => undefined);
    return cancel;
  };
  const web = <A>(
    decode: (batch: ReadBatch) => Effect.Effect<ReadonlyArray<A>, PayloadDecodeError>,
    json = false,
  ) => {
    claim();
    if (json) ensureJson();
    const messages = Effect.runSync(
      Queue.make<{ readonly items: ReadonlyArray<A>; readonly ack: Deferred.Deferred<void> }>(),
    );
    const done = Deferred.makeUnsafe<void, DurableStreamError>();
    const launch = () => {
      const running = consume({
        stopAtTail: false,
        incremental: !json,
        deliver: (batch) =>
          decode(batch).pipe(
            Effect.flatMap((items) =>
              Effect.gen(function* () {
                if (!Arr.isReadonlyArrayNonEmpty(items)) return;
                const ack = yield* Deferred.make<void>();
                yield* Queue.offer(messages, { items, ack });
                yield* Deferred.await(ack);
              }),
            ),
          ),
      });
      void running.then(
        () => Deferred.doneUnsafe(done, Exit.void),
        (error) =>
          Deferred.doneUnsafe(
            done,
            Exit.fail(
              error instanceof DurableStreamError
                ? error
                : new DurableStreamError({ code: "INTERNAL_ERROR" }),
            ),
          ),
      );
    };
    const iterator = (async function* () {
      launch();
      try {
        while (true) {
          const next = await Effect.runPromiseExit(
            Effect.raceFirst(
              Queue.take(messages).pipe(Effect.map((message) => ({ message }))),
              Deferred.await(done).pipe(Effect.as({ message: undefined })),
            ),
          );
          const result = unwrapClientExit(next);
          if (result.message === undefined) return;
          for (const [index, item] of result.message.items.entries()) {
            if (controller.signal.aborted) throw new AbortError();
            yield {
              item,
              ack: index === result.message.items.length - 1 ? result.message.ack : undefined,
            };
          }
          Deferred.doneUnsafe(result.message.ack, Exit.void);
        }
      } finally {
        cancel();
        await completion.promise;
      }
    })();
    const readable = new ReadableStream<A>(
      {
        async pull(target) {
          try {
            const next = await iterator.next();
            if (next.done) target.close();
            else {
              if (controller.signal.aborted) throw new AbortError();
              target.enqueue(next.value.item);
              if (next.value.ack !== undefined) Deferred.doneUnsafe(next.value.ack, Exit.void);
            }
          } catch (error) {
            target.error(error);
          }
        },
        async cancel() {
          cancel();
          await iterator.return();
          await completion.promise;
        },
      },
      { highWaterMark: 0 },
    );
    return Object.assign(readable, {
      async *[Symbol.asyncIterator]() {
        const reader = readable.getReader();
        try {
          while (true) {
            const next = await reader.read();
            if (next.done) return;
            yield next.value;
          }
        } finally {
          await reader.cancel();
          reader.releaseLock();
        }
      },
    });
  };
  const decodeText = (batch: ReadBatch) =>
    textDecoder
      .decode({
        source: Stream.fromIterable(batch.chunks),
        final: !batch.partial && (batch.streamClosed || (textState.finite && batch.upToDate)),
      })
      .pipe(
        Stream.runCollect,
        Effect.map((parts) => {
          textState.pending += parts.join("");
          return textState.pending;
        }),
        Effect.flatMap((text) =>
          textDecoder.complete.pipe(
            Effect.map((ready) => {
              if (!ready) return "";
              textState.pending = "";
              return text;
            }),
          ),
        ),
      );
  const response: StreamResponse<A> = {
    url: input.connection.url.href,
    contentType: state.contentType,
    live: input.live,
    startOffset: input.connection.offset ?? "-1",
    get headers() {
      return new Headers(read.transport.response?.headers);
    },
    get status() {
      return read.transport.response?.status ?? 0;
    },
    statusText: "",
    get ok() {
      const status = read.transport.response?.status ?? 0;
      return status >= 200 && status < 300;
    },
    isLoading: false,
    get offset() {
      return state.metadata.offset;
    },
    get cursor() {
      return state.metadata.cursor;
    },
    get upToDate() {
      return state.metadata.upToDate;
    },
    get streamClosed() {
      return state.metadata.streamClosed;
    },
    closed: completion.promise,
    cancel,
    async body() {
      claim();
      const chunks: Array<Uint8Array> = [];
      await consume({
        stopAtTail: true,
        deliver: (batch) =>
          Effect.sync(() => {
            for (const chunk of batch.chunks) chunks.push(chunk);
          }),
      });
      return _concat(chunks);
    },
    async text() {
      claim();
      textState.selected = true;
      const chunks: Array<string> = [];
      await consume({
        stopAtTail: true,
        deliver: (batch) =>
          decodeText(batch).pipe(
            Effect.map((text) => {
              chunks.push(text);
            }),
          ),
      });
      return chunks.join("");
    },
    async json() {
      claim();
      state.json = true;
      if (state.contentType?.split(";")[0]?.trim().toLowerCase() !== "application/json") {
        const error = new DurableStreamError({
          code: "PROTOCOL_ERROR",
          component: "read content-type",
        });
        await finish(error);
        throw error;
      }
      const items: Array<A> = [];
      await consume({
        stopAtTail: true,
        deliver: (batch) =>
          input.decodeJson(batch).pipe(
            Effect.map((values) => {
              for (const value of values) items.push(value);
            }),
          ),
      });
      return items;
    },
    bodyStream: () => web((batch) => Effect.succeed(batch.chunks)),
    textStream: () => {
      const readable = web((batch) =>
        decodeText(batch).pipe(Effect.map((text) => (text === "" ? [] : [text]))),
      );
      textState.selected = true;
      return readable;
    },
    jsonStream: () => {
      return web(input.decodeJson, true);
    },
    subscribeBytes: (fn) =>
      subscribe((batch) => _callback({ ..._metadata(batch), data: _concat(batch.chunks) }, fn)),
    subscribeText: (fn) => {
      return subscribe(
        (batch) =>
          decodeText(batch).pipe(
            Effect.flatMap((text) =>
              textDecoder.complete.pipe(
                Effect.flatMap((ready) =>
                  ready ? _callback({ ..._metadata(batch), text }, fn) : Effect.void,
                ),
              ),
            ),
          ),
        { text: true },
      );
    },
    subscribeJson: (fn) => {
      return subscribe(
        (batch) =>
          input
            .decodeJson(batch)
            .pipe(Effect.flatMap((items) => _callback({ ..._metadata(batch), items }, fn))),
        { json: true },
      );
    },
  };
  return response;
};
