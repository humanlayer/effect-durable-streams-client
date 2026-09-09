import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { Data, Effect, Layer, Logger, Match, Predicate, Record, Schema, Stream } from "effect";
import { FetchHttpClient, HttpClient } from "effect/unstable/http";
import type { TestResult } from "@durable-streams/client-conformance-tests/protocol";
import { DurableStreamsClient, StreamLifetime, StreamMetadata } from "../../src/index.ts";
import { AdapterState } from "./adapter-state.ts";

export class AdapterInputError extends Data.TaggedError("AdapterInputError")<{
  readonly cause: unknown;
}> {}

export const AdapterCommand = Schema.Union([
  Schema.Struct({ type: Schema.Literal("init"), serverUrl: Schema.String }),
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
    ttlSeconds: Schema.optionalKey(Schema.Number),
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
    seq: Schema.optionalKey(Schema.Number),
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
                clientName: "effect-durable-streams-client",
                clientVersion: "0.0.0",
                features: {
                  batching: false,
                  sse: false,
                  longPoll: false,
                  auto: false,
                  streaming: false,
                  dynamicHeaders: true,
                  retryOptions: false,
                  batchItems: false,
                  strictZeroValidation: false,
                },
              }) satisfies TestResult,
          ),
        ),
      shutdown: () => Effect.succeed({ type: "shutdown", success: true } satisfies TestResult),
      head: (input) => _inspect(input),
      connect: (input) => _inspect(input),
      create: (input) => _mutate(input),
      append: (input) => _mutate(input),
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
      InvalidDurableStreamsConfigError: () =>
        _commandError({ command, errorCode: "INVALID_ARGUMENT" }),
      PayloadEncodeError: () => _commandError({ command, errorCode: "INVALID_ARGUMENT" }),
      CreateConflictError: (error) =>
        _commandError({ command, errorCode: "CONFLICT", status: error.response.status }),
      AppendConflictError: (error) =>
        _commandError({ command, errorCode: "SEQUENCE_CONFLICT", status: error.response.status }),
      StreamClosedError: (error) =>
        _commandError({ command, errorCode: "STREAM_CLOSED", status: error.response.status }),
      StreamNotFoundError: (error) =>
        _commandError({ command, errorCode: "NOT_FOUND", status: error.response.status }),
      InvalidRequestError: (error) =>
        _commandError({ command, errorCode: "INVALID_ARGUMENT", status: error.response.status }),
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
        } satisfies TestResult),
      SchemaError: () =>
        Effect.succeed({
          type: "error",
          success: false,
          commandType: command.type,
          errorCode: "INVALID_ARGUMENT",
          message: "Invalid server URL",
        } satisfies TestResult),
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
    message: input.errorCode,
    ...Record.filter({ status: input.status }, Predicate.isNotUndefined),
  } satisfies TestResult);

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
            Effect.as({ type: "delete", success: true, status: 204 } satisfies TestResult),
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
            message: "Stream not found",
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
          message: "Malformed or unsupported Phase 2 command",
        })),
      ),
    ),
    Effect.flatMap((result) =>
      Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(result).pipe(
        Effect.map((encoded) => ({
          result: encoded,
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
