import {
  Clock,
  Data,
  DateTime,
  Duration,
  Effect,
  Exit,
  Match,
  Option,
  Predicate,
  Record,
  Schema,
  Scope,
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
import { buildRequest, type RequestInput } from "./request.ts";
import { parseRetryAfter, requestRetrySchedule } from "./retry.ts";
import { captureSchemaFailure } from "./encoding.ts";

class HeadTransportFailure extends Data.TaggedError("HeadTransportFailure")<{
  readonly cause: HttpClientError.HttpClientError;
}> {}

class HeadResponseFailure extends Data.TaggedError("HeadResponseFailure")<{
  readonly response: HttpClientResponse.HttpClientResponse;
}> {}

export type SnapshotInput = {
  readonly response: HttpClientResponse.HttpClientResponse;
  readonly url: string;
  readonly operation?: string;
};

const _snapshotResponse = (input: SnapshotInput) =>
  Effect.gen(function* () {
    const cap = 64 * 1024;
    const body = { bytes: new Uint8Array(0), length: 0 };
    const interruptedBody = yield* input.response.stream.pipe(
      Stream.takeUntil((chunk) => {
        if (chunk.length > 0 && body.bytes.length === 0) body.bytes = new Uint8Array(cap + 1);
        const part = chunk.subarray(0, body.bytes.length - body.length);
        body.bytes.set(part, body.length);
        body.length += part.length;
        return body.length > cap;
      }),
      Stream.runDrain,
      Effect.as(false),
      Effect.catchReason("HttpClientError", "EmptyBodyError", () => Effect.succeed(false)),
      Effect.catchTag("HttpClientError", () =>
        Effect.gen(function* () {
          yield* Effect.logWarning("Unable to finish error response snapshot", {
            operation: input.operation ?? "HEAD",
            status: input.response.status,
            url: input.url,
          });
          return true;
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
    const snapshot = ErrorResponse.make({
      status: input.response.status,
      url,
      headers,
      body:
        body.length === 0 && !interruptedBody
          ? ErrorResponseBody.cases.Empty.make({})
          : ErrorResponseBody.cases.Bytes.make({
              value: body.bytes.slice(0, Math.min(body.length, cap)),
              truncated: body.length > cap || interruptedBody,
              ...Record.filter({ contentType }, Predicate.isNotUndefined),
            }),
    });
    return snapshot;
  });

export const protocolViolation = (input: {
  readonly response: HttpClientResponse.HttpClientResponse;
  readonly component: string;
}) =>
  _snapshotResponse({
    response: input.response,
    url: "[REDACTED]",
    operation: input.component,
  }).pipe(
    Effect.flatMap((response) =>
      Effect.fail(new ProtocolViolationError({ component: input.component, response })),
    ),
  );

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
      .execute(buildRequest({ connection: input.connection, method: "HEAD" }))
      .pipe(
        Effect.provideService(HttpClient.TracerDisabledWhen, () => true),
        Effect.catchReason("HttpClientError", "StatusCodeError", (reason) =>
          Effect.succeed(reason.response),
        ),
        Effect.mapError((cause) => new HeadTransportFailure({ cause })),
      );
    if (response.status === 404) return StreamMetadata.cases.Missing.make({});
    if (response.status !== 200) return yield* new HeadResponseFailure({ response });
    return yield* parseHeadMetadata({
      headers: response.headers,
      requiresJson: input.operation === "connect" && input.hasSchema === true,
    }).pipe(
      Effect.tapError((failure) =>
        captureSchemaFailure({
          cause: failure.cause,
          operation: input.operation,
          component: failure.component,
          metadata: true,
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
    Effect.tapError(freezeErrorResponse),
    Effect.scoped,
    Effect.withSpan(`durable_streams.${input.operation}`, {
      attributes: { operation: input.operation, url },
    }),
  );
};

export class MutationFailure extends Data.TaggedError("MutationFailure")<{
  readonly response?: ErrorResponse;
  readonly cause?: HttpClientError.HttpClientError;
  readonly retryAfter?: Duration.Duration;
  readonly retryable: boolean;
}> {}

export const freezeErrorResponse = (error: { readonly response?: ErrorResponse }) =>
  Effect.sync(() => {
    const response = error.response;
    if (response === undefined) return;
    if (ErrorResponseBody.guards.Bytes(response.body)) {
      const captured = response.body.value.slice();
      Object.defineProperty(response.body, "value", {
        enumerable: true,
        get: () => captured.slice(),
      });
    }
    Object.freeze(response.headers);
    Object.freeze(response.body);
    Object.freeze(response);
  });

export type MutationRequest = RequestInput & {
  readonly operation: "create" | "append" | "close" | "delete";
  readonly safe: boolean;
};

export const sendMutation = (input: MutationRequest) => {
  const url = input.connection.url.origin + input.connection.url.pathname;
  return Effect.gen(function* () {
    const parentScope = yield* Effect.scope;
    const attemptScope = yield* Scope.fork(parentScope);
    return yield* Effect.gen(function* () {
      const http = yield* HttpClient.HttpClient;
      const response = yield* HttpClient.withScope(http)
        .execute(buildRequest(input))
        .pipe(
          Effect.provideService(HttpClient.TracerDisabledWhen, () => true),
          Effect.catchReason("HttpClientError", "StatusCodeError", (reason) =>
            Effect.succeed(reason.response),
          ),
          Effect.mapError((cause) => new MutationFailure({ cause, retryable: true })),
        );
      if (response.status >= 200 && response.status < 300) return response;
      const snapshot = yield* _snapshotResponse({ response, url, operation: input.operation });
      const retryAfter = yield* parseRetryAfter(response.headers["retry-after"]);
      return yield* new MutationFailure({
        response: snapshot,
        ...Record.filter(
          { retryAfter: Option.getOrUndefined(retryAfter) },
          Predicate.isNotUndefined,
        ),
        retryable:
          (response.status === 429 || response.status >= 500 || response.status < 400) &&
          !(response.status === 501 && input.operation !== "create"),
      });
    }).pipe(
      Effect.provideService(Scope.Scope, attemptScope),
      Effect.onExit((exit) =>
        Exit.isFailure(exit) ? Scope.close(attemptScope, exit) : Effect.void,
      ),
    );
  }).pipe(
    Effect.tapError((failure) =>
      Effect.logWarning("Stream mutation request failed", {
        operation: input.operation,
        url,
        status: failure.response?.status,
        transport: failure.cause?.reason._tag,
      }),
    ),
    Effect.retry({
      while: (failure) => input.safe && failure.retryable,
      schedule: requestRetrySchedule(input.connection),
    }),
  );
};
