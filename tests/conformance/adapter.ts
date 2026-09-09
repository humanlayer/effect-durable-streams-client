import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { Data, Effect, Layer, Logger, Match, Predicate, Record, Schema, Stream } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import type { TestResult } from "@durable-streams/client-conformance-tests/protocol";
import { DurableStreamsClient } from "../../src/index.ts";
import { AdapterState } from "./adapter-state.ts";

export class AdapterInputError extends Data.TaggedError("AdapterInputError")<{
  readonly cause: unknown;
}> {}

export const AdapterCommand = Schema.Union([
  Schema.Struct({ type: Schema.Literal("init"), serverUrl: Schema.String }),
  Schema.Struct({ type: Schema.Literal("head"), path: Schema.String }),
  Schema.Struct({ type: Schema.Literal("connect"), path: Schema.String }),
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
                  dynamicHeaders: false,
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
    });
  }).pipe(
    Effect.catchTags({
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
  );

const _inspect = (command: Extract<AdapterCommand, { readonly path: string }>) =>
  Effect.gen(function* () {
    const state = yield* AdapterState;
    const url = yield* state.location(command);
    const client = yield* DurableStreamsClient.make({ url });
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
          message: "Malformed or unsupported Phase 1 command",
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
