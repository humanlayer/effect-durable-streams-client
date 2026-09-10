import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import packageJson from "../../package.json" with { type: "json" };
import {
  Array as Arr,
  Data,
  Deferred,
  Duration,
  Effect,
  Layer,
  Logger,
  Match,
  Option,
  Predicate,
  Record,
  Ref,
  Schema,
  Stream,
} from "effect";
import { FetchHttpClient, HttpClient } from "effect/unstable/http";
import type { TestResult } from "@durable-streams/client-conformance-tests/protocol";
import { DurableStreamsClient, StreamLifetime, StreamMetadata } from "../../src/index.ts";
import { AdapterState } from "./adapter-state.ts";
import { readLive } from "./adapter-live-read.ts";
import {
  ProducerCommand,
  ValidateCommand,
  handleProducer,
  validateOptions,
} from "./adapter-producer.ts";

export class AdapterInputError extends Data.TaggedError("AdapterInputError")<{
  readonly cause: unknown;
}> {}

class AdapterReadTimeout extends Data.TaggedError("AdapterReadTimeout") {}

export const AdapterCommand = Schema.Union([
  ...ProducerCommand.members,
  ValidateCommand,
  Schema.Struct({ type: Schema.Literal("init"), serverUrl: Schema.String }),
  Schema.Struct({
    type: Schema.Literal("read"),
    path: Schema.String,
    offset: Schema.optionalKey(Schema.String),
    live: Schema.optionalKey(Schema.Union([Schema.Boolean, Schema.Literals(["long-poll", "sse"])])),
    timeoutMs: Schema.optionalKey(Schema.Finite),
    maxChunks: Schema.optionalKey(Schema.Int),
    waitForUpToDate: Schema.optionalKey(Schema.Boolean),
    headers: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  }),
  Schema.Struct({
    type: Schema.Literal("head"),
    path: Schema.String,
    headers: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  }),
  Schema.Struct({
    type: Schema.Literal("connect"),
    path: Schema.String,
    headers: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  }),
  Schema.Struct({
    type: Schema.Literal("create"),
    path: Schema.String,
    contentType: Schema.optionalKey(Schema.String),
    ttlSeconds: Schema.optionalKey(Schema.Finite),
    expiresAt: Schema.optionalKey(Schema.String),
    headers: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
    closed: Schema.optionalKey(Schema.Boolean),
    data: Schema.optionalKey(Schema.String),
  }),
  Schema.Struct({
    type: Schema.Literal("append"),
    path: Schema.String,
    data: Schema.String,
    binary: Schema.optionalKey(Schema.Boolean),
    seq: Schema.optionalKey(Schema.Finite),
    headers: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  }),
  Schema.Struct({
    type: Schema.Literal("append-batch"),
    path: Schema.String,
    items: Schema.Array(Schema.String),
    headers: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  }),
  Schema.Struct({
    type: Schema.Literal("close"),
    path: Schema.String,
    data: Schema.optionalKey(Schema.String),
    contentType: Schema.optionalKey(Schema.String),
  }),
  Schema.Struct({
    type: Schema.Literal("delete"),
    path: Schema.String,
    headers: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  }),
  Schema.Struct({
    type: Schema.Literal("set-dynamic-header"),
    name: Schema.String,
    valueType: Schema.Literals(["counter", "timestamp", "token"]),
    initialValue: Schema.optionalKey(Schema.String),
  }),
  Schema.Struct({
    type: Schema.Literal("set-dynamic-param"),
    name: Schema.String,
    valueType: Schema.Literals(["counter", "timestamp"]),
  }),
  Schema.Struct({ type: Schema.Literal("clear-dynamic") }),
  Schema.Struct({ type: Schema.Literal("shutdown") }),
]).pipe(Schema.toTaggedUnion("type"));
export type AdapterCommand = typeof AdapterCommand.Type;

export const handleCommand = (command: AdapterCommand) =>
  Effect.gen(function* () {
    const state = yield* AdapterState;
    return yield* AdapterCommand.match(command, {
      init: (input) =>
        state.initialize(input).pipe(
          Effect.map(
            () =>
              ({
                type: "init",
                success: true,
                clientName: packageJson.name,
                clientVersion: packageJson.version,
                features: {
                  batching: true,
                  sse: true,
                  longPoll: true,
                  auto: false,
                  streaming: true,
                  dynamicHeaders: true,
                  retryOptions: true,
                  batchItems: false,
                  strictZeroValidation: true,
                },
              }) satisfies TestResult,
          ),
        ),
      shutdown: () => Effect.succeed({ type: "shutdown", success: true } satisfies TestResult),
      "idempotent-append": handleProducer,
      "idempotent-append-batch": handleProducer,
      "idempotent-close": handleProducer,
      "idempotent-detach": handleProducer,
      validate: validateOptions,
      head: (input) => _inspect(input),
      connect: (input) => _inspect(input),
      read: (input) =>
        Effect.gen(function* () {
          if (input.live === "long-poll" || input.live === "sse") return yield* readLive(input);
          if (input.live !== undefined && input.live)
            return yield* _commandError({ command, errorCode: "NOT_SUPPORTED" });
          const acquired = yield* Deferred.make<void>();
          const streamClosed = yield* Ref.make(false);
          const deadline = Deferred.await(acquired).pipe(
            Effect.timeoutOrElse({
              duration: Duration.millis(input.timeoutMs ?? 5000),
              orElse: () => Effect.fail(new AdapterReadTimeout()),
            }),
            Effect.andThen(Effect.never),
          );
          return yield* Effect.gen(function* () {
            const url = yield* state.location(input);
            const contentType = yield* state.contentType(input);
            const client = yield* DurableStreamsClient.make({
              url,
              ...Record.filter(
                { offset: input.offset, headers: input.headers },
                Predicate.isNotUndefined,
              ),
            });
            const metadata = contentType === undefined ? yield* client.connect : undefined;
            const discovered =
              metadata !== undefined && StreamMetadata.guards.Existing(metadata)
                ? metadata.contentType
                : contentType;
            const data = _isJson(discovered)
              ? yield* client.json.pipe(
                  Stream.runCollect,
                  Effect.flatMap((items) =>
                    !Arr.isArrayNonEmpty(items)
                      ? Effect.succeed("")
                      : Schema.encodeEffect(Schema.fromJsonString(Schema.Array(Schema.Json)))(
                          items,
                        ),
                  ),
                )
              : yield* client.bytes.pipe(
                  Stream.runCollect,
                  Effect.map((chunks) => {
                    const bytes = new Uint8Array(
                      chunks.reduce((size, chunk) => size + chunk.length, 0),
                    );
                    const position = { offset: 0 };
                    for (const chunk of chunks) {
                      bytes.set(chunk, position.offset);
                      position.offset += chunk.length;
                    }
                    return new TextDecoder().decode(bytes);
                  }),
                );
            const offset = Option.getOrUndefined(yield* client.offset);
            const position = Record.filter({ offset }, Predicate.isNotUndefined);
            return {
              type: "read",
              success: true,
              status: 200,
              chunks: data.length === 0 ? [] : [{ data, ...position }],
              ...position,
              upToDate: true,
              streamClosed: yield* Ref.get(streamClosed),
              ...(yield* state.sent),
            } satisfies TestResult;
          }).pipe(
            Effect.provideServiceEffect(
              HttpClient.HttpClient,
              Effect.map(HttpClient.HttpClient, (http) =>
                http.pipe(
                  HttpClient.tap((response) =>
                    response.request.method === "GET" && response.status === 200
                      ? Ref.set(streamClosed, response.headers["stream-closed"] === "true").pipe(
                          Effect.andThen(Deferred.succeed(acquired, undefined)),
                        )
                      : Effect.void,
                  ),
                ),
              ),
            ),
            Effect.raceFirst(deadline),
            Effect.catchTag("AdapterReadTimeout", () =>
              state.sent.pipe(
                Effect.map(
                  (sent) =>
                    ({
                      type: "read",
                      success: true,
                      status: 200,
                      chunks: [],
                      offset: input.offset ?? "-1",
                      upToDate: true,
                      ...sent,
                    }) satisfies TestResult,
                ),
              ),
            ),
          );
        }),
      create: (input) => _mutate(input),
      append: (input) => _mutate(input),
      "append-batch": (input) =>
        Effect.gen(function* () {
          const url = yield* state.location(input);
          const contentType = yield* state.contentType(input);
          const client = yield* DurableStreamsClient.make({
            url,
            ...Record.filter({ contentType, headers: input.headers }, Predicate.isNotUndefined),
          });
          const metadata = contentType === undefined ? yield* client.connect : undefined;
          const discovered =
            metadata !== undefined && StreamMetadata.guards.Existing(metadata)
              ? metadata.contentType
              : contentType;
          const values = yield* Effect.forEach(input.items, (item) =>
            _isJson(discovered)
              ? Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(item)
              : Effect.succeed(item),
          );
          const results = yield* Effect.forEach(values, (value) => client.append({ value }), {
            concurrency: "unbounded",
          });
          return {
            type: "append-batch" as const,
            success: true as const,
            status: 200,
            offsets: results.map((result) => result.offset),
          };
        }),
      close: (input) => _mutate(input),
      delete: (input) => _mutate(input),
      "set-dynamic-header": (input) =>
        state
          .setHeader(input)
          .pipe(Effect.as({ type: "set-dynamic-header", success: true } satisfies TestResult)),
      "set-dynamic-param": (input) =>
        state
          .setParam(input)
          .pipe(Effect.as({ type: "set-dynamic-param", success: true } satisfies TestResult)),
      "clear-dynamic": () =>
        state.clear.pipe(Effect.as({ type: "clear-dynamic", success: true } satisfies TestResult)),
    });
  }).pipe(
    Effect.catchTags({
      ProducerClosedError: () => _commandError({ command, errorCode: "ALREADY_CLOSED" }),
      ProducerFencedError: (error) =>
        _commandError({ command, errorCode: "STALE_EPOCH", status: error.response.status }),
      ProducerSequenceGapError: (error) =>
        _commandError({ command, errorCode: "SEQUENCE_GAP", status: error.response.status }),
      InvalidDurableStreamsConfigError: () =>
        _commandError({ command, errorCode: "INVALID_ARGUMENT" }),
      PayloadEncodeError: () => _commandError({ command, errorCode: "INVALID_ARGUMENT" }),
      PayloadDecodeError: () => _commandError({ command, errorCode: "PARSE_ERROR" }),
      AlreadyConsumedError: () => _commandError({ command, errorCode: "ALREADY_CONSUMED" }),
      CreateConflictError: (error) =>
        _commandError({ command, errorCode: "CONFLICT", status: error.response.status }),
      AppendConflictError: (error) =>
        _commandError({ command, errorCode: "SEQUENCE_CONFLICT", status: error.response.status }),
      StreamClosedError: (error) =>
        _commandError({ command, errorCode: "STREAM_CLOSED", status: error.response.status }),
      StreamNotFoundError: (error) =>
        _commandError({ command, errorCode: "NOT_FOUND", status: error.response.status }),
      InvalidRequestError: (error) =>
        _commandError({
          command,
          errorCode:
            command.type === "read" && command.offset !== undefined
              ? "INVALID_OFFSET"
              : "INVALID_ARGUMENT",
          status: error.response.status,
        }),
      PayloadTooLargeError: (error) =>
        _commandError({ command, errorCode: "PAYLOAD_TOO_LARGE", status: error.response.status }),
      OperationNotSupportedError: (error) =>
        _commandError({ command, errorCode: "NOT_SUPPORTED", status: error.response.status }),
      UnauthorizedError: (error) =>
        _commandError({ command, errorCode: "UNAUTHORIZED", status: error.response.status }),
      ForbiddenError: (error) =>
        _commandError({ command, errorCode: "FORBIDDEN", status: error.response.status }),
      StreamGoneError: (error) =>
        _commandError({ command, errorCode: "GONE", status: error.response.status }),
      RateLimitedError: (error) =>
        _commandError({ command, errorCode: "RATE_LIMITED", status: error.response.status }),
      StreamUnavailableError: (error) =>
        _commandError({ command, errorCode: "NETWORK_ERROR", status: error.response?.status }),
      AppendOutcomeUnknownError: (error) =>
        _commandError({ command, errorCode: "NETWORK_ERROR", status: error.response?.status }),
      ProtocolViolationError: (error) =>
        _commandError({ command, errorCode: "PARSE_ERROR", status: error.response?.status }),
      AdapterNotInitialized: () =>
        Effect.succeed({
          type: "error",
          success: false,
          commandType: command.type,
          errorCode: "INVALID_ARGUMENT",
          message: "Initialize the adapter first",
        }),
      SchemaError: () =>
        Effect.succeed({
          type: "error",
          success: false,
          commandType: command.type,
          errorCode: "INVALID_ARGUMENT",
          message: "Invalid server URL",
        }),
    }),
    Effect.provideServiceEffect(
      HttpClient.HttpClient,
      Effect.gen(function* () {
        const state = yield* AdapterState;
        const http = yield* HttpClient.HttpClient;
        return http.pipe(HttpClient.mapRequestEffect(state.transform));
      }),
    ),
  );

const _commandError = (input: {
  readonly command: AdapterCommand;
  readonly errorCode: string;
  readonly status?: number;
}) =>
  Effect.succeed({
    type: "error",
    success: false,
    commandType: input.command.type,
    errorCode: input.errorCode,
    message: Match.value(input.command).pipe(
      Match.when({ type: "read" }, (command) => `${input.errorCode}: ${command.path}`),
      Match.when({ type: "append" }, (command) => `${input.errorCode}: ${command.path}`),
      Match.when(
        { type: "validate" },
        (command) =>
          `${input.errorCode}: invalid ${Object.keys(command.target)
            .filter((key) => key !== "target")
            .join(", ")}`,
      ),
      Match.orElse(() => input.errorCode),
    ),
    ...Record.filter({ status: input.status }, Predicate.isNotUndefined),
  });

const _mutate = (
  command: Extract<AdapterCommand, { readonly type: "create" | "append" | "close" | "delete" }>,
) =>
  Effect.gen(function* () {
    const state = yield* AdapterState;
    const url = yield* state.location(command);
    const headers = command.type !== "close" ? command.headers : undefined;
    const contentType = yield* Match.value(command).pipe(
      Match.discriminatorsExhaustive("type")({
        create: (input) => Effect.succeed(input.contentType ?? "application/octet-stream"),
        close: (input) =>
          state.contentType(input).pipe(Effect.map((cached) => input.contentType ?? cached)),
        append: (input) => state.contentType(input),
        delete: (input) => state.contentType(input),
      }),
    );
    const client = yield* DurableStreamsClient.make({
      url,
      ...Record.filter({ headers, contentType }, Predicate.isNotUndefined),
      batching: false,
    });
    return yield* Match.value(command).pipe(
      Match.discriminatorsExhaustive("type")({
        create: (input) =>
          Effect.gen(function* () {
            if (input.ttlSeconds !== undefined && input.expiresAt !== undefined)
              return yield* _commandError({ command, errorCode: "INVALID_ARGUMENT" });
            const lifetime =
              input.ttlSeconds !== undefined
                ? yield* StreamLifetime.cases.Ttl.makeEffect({ ttlSeconds: input.ttlSeconds })
                : input.expiresAt !== undefined
                  ? yield* StreamLifetime.cases.ExpiresAt.makeEffect({ expiresAt: input.expiresAt })
                  : undefined;
            const parsed =
              input.data === undefined
                ? undefined
                : _isJson(contentType)
                  ? yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(input.data)
                  : input.data;
            const result = yield* client.create({
              ...Record.filter({ lifetime, closed: input.closed }, Predicate.isNotUndefined),
              ...(Array.isArray(parsed)
                ? { values: parsed }
                : Record.filter({ value: parsed }, Predicate.isNotUndefined)),
            });
            yield* state.remember({ path: input.path, contentType: result.contentType });
            return {
              type: "create",
              success: true,
              status: result.status,
              offset: result.offset,
            } satisfies TestResult;
          }),
        append: (input) =>
          Effect.gen(function* () {
            const metadata = contentType === undefined ? yield* client.connect : undefined;
            const discovered =
              metadata !== undefined && StreamMetadata.guards.Existing(metadata)
                ? metadata.contentType
                : contentType;
            const json = _isJson(discovered);
            const body = input.binary
              ? yield* Schema.decodeEffect(Schema.Uint8ArrayFromBase64)(input.data)
              : input.data;
            const value = json
              ? yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(
                  Predicate.isString(body) ? body : new TextDecoder().decode(body),
                )
              : body;
            const result = yield* client.append({
              value,
              ...Record.filter(
                { seq: input.seq === undefined ? undefined : String(input.seq) },
                Predicate.isNotUndefined,
              ),
            });
            return {
              type: "append",
              success: true,
              status: 200,
              offset: result.offset,
              ...(yield* state.sent),
            } satisfies TestResult;
          }),
        close: (input) =>
          Effect.gen(function* () {
            const metadata =
              contentType === undefined && input.data !== undefined
                ? yield* client.connect
                : undefined;
            const discovered =
              metadata !== undefined && StreamMetadata.guards.Existing(metadata)
                ? metadata.contentType
                : contentType;
            const json = _isJson(discovered);
            const value =
              input.data === undefined
                ? undefined
                : json
                  ? yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(input.data)
                  : input.data;
            const result = yield* client.close({
              ...Record.filter({ value }, Predicate.isNotUndefined),
            });
            return {
              type: "close",
              success: true,
              finalOffset: result.finalOffset,
            } satisfies TestResult;
          }),
        delete: () =>
          client.delete.pipe(
            Effect.tap(() => state.forget(command)),
            Effect.as({ type: "delete", success: true, status: 200 } satisfies TestResult),
          ),
      }),
    );
  });

const _isJson = (contentType: string | undefined) =>
  contentType?.split(";")[0]?.trim().toLowerCase() === "application/json";

const _inspect = (command: Extract<AdapterCommand, { readonly type: "head" | "connect" }>) =>
  Effect.gen(function* () {
    const state = yield* AdapterState;
    const url = yield* state.location(command);
    const client = yield* DurableStreamsClient.make({
      url,
      ...Record.filter({ headers: command.headers }, Predicate.isNotUndefined),
    });
    const metadata = yield* command.type === "head" ? client.head : client.connect;
    return Match.value(metadata).pipe(
      Match.tagsExhaustive({
        Missing: () =>
          ({
            type: "error",
            success: false,
            commandType: command.type,
            status: 404,
            errorCode: "NOT_FOUND",
            message: `Stream not found: ${command.path}`,
          }) satisfies TestResult,
        Existing: (value) =>
          ({
            type: command.type,
            success: true,
            status: 200,
            offset: value.offset,
            contentType: value.contentType,
            streamClosed: value.closed,
            ...Record.filter(
              { ttlSeconds: value.ttlSeconds, expiresAt: value.expiresAt },
              Predicate.isNotUndefined,
            ),
          }) satisfies TestResult,
      }),
    );
  }).pipe(
    Effect.catchTags({
      InvalidDurableStreamsConfigError: () =>
        Effect.succeed({
          type: "error",
          success: false,
          commandType: command.type,
          errorCode: "INVALID_ARGUMENT",
          message: "Invalid stream URL",
        } satisfies TestResult),
      UnauthorizedError: (error) =>
        Effect.succeed({
          type: "error",
          success: false,
          commandType: command.type,
          status: error.response.status,
          errorCode: "UNAUTHORIZED",
          message: error.message,
        } satisfies TestResult),
      ForbiddenError: (error) =>
        Effect.succeed({
          type: "error",
          success: false,
          commandType: command.type,
          status: error.response.status,
          errorCode: "FORBIDDEN",
          message: error.message,
        } satisfies TestResult),
      StreamGoneError: (error) =>
        Effect.succeed({
          type: "error",
          success: false,
          commandType: command.type,
          status: error.response.status,
          errorCode: "GONE",
          message: error.message,
        } satisfies TestResult),
      RateLimitedError: (error) =>
        Effect.succeed({
          type: "error",
          success: false,
          commandType: command.type,
          status: error.response.status,
          errorCode: "RATE_LIMITED",
          message: error.message,
        } satisfies TestResult),
      StreamUnavailableError: (error) =>
        Effect.succeed({
          type: "error",
          success: false,
          commandType: command.type,
          errorCode: "NETWORK_ERROR",
          message: error.message,
          ...Record.filter({ status: error.response?.status }, Predicate.isNotUndefined),
        } satisfies TestResult),
      ProtocolViolationError: (error) =>
        Effect.succeed({
          type: "error",
          success: false,
          commandType: command.type,
          errorCode: "PARSE_ERROR",
          message: error.message,
          ...Record.filter({ status: error.response?.status }, Predicate.isNotUndefined),
        } satisfies TestResult),
    }),
  );

const _processLine = (line: string) =>
  Schema.decodeEffect(Schema.fromJsonString(AdapterCommand))(line).pipe(
    Effect.flatMap(handleCommand),
    Effect.catchTag("SchemaError", () =>
      Schema.decodeEffect(Schema.fromJsonString(Schema.Struct({ type: Schema.String })))(line).pipe(
        Effect.map((command) => command.type),
        Effect.catchTag("SchemaError", () => Effect.succeed("unknown")),
        Effect.map((commandType) => ({
          type: "error",
          success: false,
          commandType,
          errorCode: "INVALID_ARGUMENT",
          message: "Malformed or unsupported command",
        })),
      ),
    ),
    Effect.flatMap((result) =>
      Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(result).pipe(
        Effect.map((encoded) => ({
          result: encoded.replace(/\u2028/g, "\\u2028").replace(/\u2029/g, "\\u2029"),
          shutdown: result.type === "shutdown" && result.success,
        })),
      ),
    ),
  );

export const processLine = (line: string) =>
  _processLine(line).pipe(Effect.map((result) => result.result));

export const runAdapter = Effect.gen(function* () {
  const lines = yield* Effect.acquireRelease(
    Effect.sync(() => createInterface({ input: process.stdin, crlfDelay: Infinity })),
    (input) => Effect.sync(() => input.close()),
  );
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      process.stderr.write("adapter scope closed\n");
    }),
  );
  yield* Stream.fromAsyncIterable(lines, (cause) => new AdapterInputError({ cause })).pipe(
    Stream.mapEffect(_processLine),
    Stream.takeUntil(({ shutdown }) => shutdown),
    Stream.runForEach(({ result }) =>
      Effect.sync(() => {
        process.stdout.write(result + "\n");
      }),
    ),
  );
}).pipe(
  // oxlint-disable-next-line effecttsgo/strict-effect-provide -- SAFETY: this is the adapter application composition root.
  Effect.provide(
    Layer.mergeAll(
      FetchHttpClient.layer,
      Layer.effect(AdapterState, AdapterState.make),
      Logger.layer([
        Logger.make(({ message }) => {
          process.stderr.write(String(message) + "\n");
        }),
      ]),
    ),
  ),
  Effect.scoped,
);

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  Effect.runPromise(runAdapter).catch(() => {
    process.stderr.write("adapter terminated unexpectedly\n");
    process.exitCode = 1;
  });
}
