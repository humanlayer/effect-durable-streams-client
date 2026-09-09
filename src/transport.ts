import {
  Clock,
  Data,
  DateTime,
  Duration,
  Effect,
  Match,
  Option,
  Predicate,
  Record,
  Schema,
  Stream,
} from "effect";
import { HttpClient, type HttpClientError, type HttpClientResponse } from "effect/unstable/http";
import {
  ErrorResponse,
  ErrorResponseBody,
  ForbiddenError,
  ProtocolViolationError,
  RateLimitedError,
  StreamGoneError,
  StreamUnavailableError,
  UnauthorizedError,
  type HeadError,
} from "./errors.ts";
import { StreamMetadata, type DurableStreamsConnection } from "./model.ts";
import { parseHeadMetadata } from "./protocol.ts";

class HeadTransportFailure extends Data.TaggedError("HeadTransportFailure")<{
  readonly cause: HttpClientError.HttpClientError;
}> {}

class HeadResponseFailure extends Data.TaggedError("HeadResponseFailure")<{
  readonly response: HttpClientResponse.HttpClientResponse;
}> {}

export type SnapshotInput = {
  readonly response: HttpClientResponse.HttpClientResponse;
  readonly url: string;
};

const _snapshotResponse = (input: SnapshotInput) =>
  Effect.gen(function* () {
    const cap = 64 * 1024;
    const bytes = new Uint8Array(cap + 1);
    let length = 0;
    let interruptedBody = false;
    yield* input.response.stream.pipe(
      Stream.takeUntil((chunk) => {
        const part = chunk.subarray(0, bytes.length - length);
        bytes.set(part, length);
        length += part.length;
        return length > cap;
      }),
      Stream.runDrain,
      Effect.catchTag("HttpClientError", () =>
        Effect.gen(function* () {
          interruptedBody = true;
          yield* Effect.logWarning("Unable to finish error response snapshot", {
            operation: "HEAD",
            status: input.response.status,
            url: input.url,
          });
        }),
      ),
    );
    const responseHeaders: Readonly<Record<string, string>> = input.response.headers;
    const headers = Record.mapEntries(responseHeaders, (value, name) => [
      name.toLowerCase(),
      /^(?:set-cookie|cookie|authorization|proxy-authorization|authentication-info|proxy-authentication-info|x-api-key|x-auth-token)$/i.test(
        name,
      )
        ? "[REDACTED]"
        : value,
    ]);
    const contentType = input.response.headers["content-type"];
    const url = yield* Schema.decodeEffect(Schema.URLFromString)(input.response.request.url).pipe(
      Effect.map((url) => url.origin + url.pathname),
      Effect.catchTag("SchemaError", () => Effect.succeed(input.url)),
    );
    return ErrorResponse.make({
      status: input.response.status,
      url,
      headers,
      body:
        length === 0 && !interruptedBody
          ? ErrorResponseBody.cases.Empty.make({})
          : ErrorResponseBody.cases.Bytes.make({
              value: bytes.slice(0, Math.min(length, cap)),
              truncated: length > cap || interruptedBody,
              ...Record.filter({ contentType }, Predicate.isNotUndefined),
            }),
    });
  });

export type HeadInput = {
  readonly connection: DurableStreamsConnection;
  readonly operation: "head" | "connect";
  readonly hasSchema?: boolean;
};

export const inspectStream = (
  input: HeadInput,
): Effect.Effect<StreamMetadata, HeadError, HttpClient.HttpClient> => {
  const url = input.connection.url.origin + input.connection.url.pathname;
  return Effect.gen(function* () {
    const http = yield* HttpClient.HttpClient;
    const response = yield* HttpClient.withScope(http)
      .head(input.connection.url.href)
      .pipe(
        Effect.provideService(HttpClient.TracerDisabledWhen, () => true),
        Effect.mapError((cause) => new HeadTransportFailure({ cause })),
      );
    if (response.status === 404) return StreamMetadata.cases.Missing.make({});
    if (response.status !== 200) return yield* new HeadResponseFailure({ response });
    return yield* parseHeadMetadata({
      headers: response.headers,
      requiresJson: input.operation === "connect" && input.hasSchema === true,
    }).pipe(
      Effect.tapError((failure) =>
        Effect.logWarning("Invalid stream metadata", {
          operation: input.operation,
          url,
          status: response.status,
          component: failure.component,
        }),
      ),
      Effect.catchTag("HeadMetadataFailure", (failure) =>
        Effect.gen(function* () {
          const snapshot = yield* _snapshotResponse({ response, url });
          return yield* new ProtocolViolationError({
            component: failure.component,
            response: snapshot,
          });
        }),
      ),
    );
  }).pipe(
    Effect.catchTags({
      HeadTransportFailure: (failure) =>
        Effect.logWarning("Stream transport failed", {
          operation: input.operation,
          url,
          transport: failure.cause.reason._tag,
        }).pipe(Effect.andThen(Effect.fail(new StreamUnavailableError({})))),
      HeadResponseFailure: (failure) =>
        Effect.gen(function* () {
          yield* Effect.logWarning("Stream request rejected", {
            operation: input.operation,
            url,
            status: failure.response.status,
          });
          const response = yield* _snapshotResponse({ response: failure.response, url });
          const now = yield* Clock.currentTimeMillis;
          const rawRetryAfter = failure.response.headers["retry-after"];
          const retryAfter = Option.fromUndefinedOr(rawRetryAfter).pipe(
            Option.flatMap((raw) => {
              if (/^\d+$/.test(raw) && Number.isSafeInteger(Number(raw))) {
                return Option.some(Duration.seconds(Number(raw)));
              }
              if (!/^[A-Z][a-z]{2}, \d{2} [A-Z][a-z]{2} \d{4} \d{2}:\d{2}:\d{2} GMT$/.test(raw))
                return Option.none();
              return DateTime.make(raw).pipe(
                Option.map((date) =>
                  Duration.millis(Math.max(0, DateTime.toEpochMillis(date) - now)),
                ),
              );
            }),
          );
          return yield* Match.value(response.status).pipe(
            Match.when(401, () => Effect.fail(new UnauthorizedError({ response }))),
            Match.when(403, () => Effect.fail(new ForbiddenError({ response }))),
            Match.when(410, () => Effect.fail(new StreamGoneError({ response }))),
            Match.when(429, () =>
              Effect.fail(
                new RateLimitedError({
                  response,
                  ...Record.filter(
                    { retryAfter: Option.getOrUndefined(retryAfter) },
                    Predicate.isNotUndefined,
                  ),
                }),
              ),
            ),
            Match.when(
              (status) => status >= 500 || status === 408,
              () => Effect.fail(new StreamUnavailableError({ response })),
            ),
            Match.orElse(() =>
              Effect.fail(new ProtocolViolationError({ component: "HEAD status", response })),
            ),
          );
        }),
    }),
    Effect.tapError((error) =>
      Effect.sync(() => {
        if (error.response !== undefined) {
          if (ErrorResponseBody.guards.Bytes(error.response.body)) {
            const captured = error.response.body.value.slice();
            Object.defineProperty(error.response.body, "value", {
              enumerable: true,
              get: () => captured.slice(),
            });
          }
          Object.freeze(error.response.headers);
          Object.freeze(error.response.body);
          Object.freeze(error.response);
        }
      }),
    ),
    Effect.scoped,
    Effect.withSpan(`durable_streams.${input.operation}`, {
      attributes: { operation: input.operation, url },
    }),
  );
};
