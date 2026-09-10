import { Effect, Match, Option, Predicate, Record, Schema } from "effect";
import type { HttpClient } from "effect/unstable/http";
import * as Errors from "./errors.js";
import {
  captureSchemaFailure,
  encodePayload,
  encodePayloads,
  type PreparedBody,
} from "./encoding.js";
import {
  AppendResult,
  CloseResult,
  CreateOptions,
  CreateResult,
  StreamLifetime,
  type AppendInput,
  type CloseInput,
  type CreateInput,
  type DurableStreamsConnection,
} from "./model.js";
import { parseHeadMetadata, WriteHeaders } from "./protocol.js";
import {
  freezeErrorResponse,
  protocolViolation,
  sendMutation,
  type MutationFailure,
} from "./transport.js";
import { FieldValue } from "./headers.js";
import type { Stream } from "effect";
import { isRequestMetadataFailure } from "./request.js";

const _commonRejection = (failure: MutationFailure) => {
  if (failure.cause !== undefined && isRequestMetadataFailure(failure.cause))
    return Effect.fail(new Errors.ProtocolViolationError({ component: "request metadata" }));
  const response = failure.response;
  if (response === undefined) return Effect.fail(new Errors.StreamUnavailableError({}));
  return Match.value(response.status).pipe(
    Match.when(400, () => Effect.fail(new Errors.InvalidRequestError({ response }))),
    Match.when(401, () => Effect.fail(new Errors.UnauthorizedError({ response }))),
    Match.when(403, () => Effect.fail(new Errors.ForbiddenError({ response }))),
    Match.when(410, () => Effect.fail(new Errors.StreamGoneError({ response }))),
    Match.when(429, () =>
      Effect.fail(
        new Errors.RateLimitedError({
          response,
          ...Record.filter({ retryAfter: failure.retryAfter }, Predicate.isNotUndefined),
        }),
      ),
    ),
    Match.when(
      (status) => status >= 500 || status < 400 || status === 408,
      () => Effect.fail(new Errors.StreamUnavailableError({ response })),
    ),
    Match.orElse(() =>
      Effect.fail(new Errors.ProtocolViolationError({ component: "mutation status", response })),
    ),
  );
};

export type LifecycleContext<S extends Schema.Top> = {
  readonly connection: DurableStreamsConnection;
  readonly schema?: S;
};

export const createStream = <S extends Schema.Top>(
  context: LifecycleContext<S> & {
    readonly input: CreateInput<S["Type"] | Uint8Array>;
    readonly prepared?: PreparedBody;
  },
): Effect.Effect<CreateResult, Errors.CreateError, HttpClient.HttpClient | S["EncodingServices"]> =>
  Effect.gen(function* () {
    const input = context.input;
    if (Predicate.hasProperty(input, "value") && Predicate.hasProperty(input, "values"))
      return yield* new Errors.PayloadEncodeError({ component: "create payload exclusivity" });
    const options = yield* Schema.decodeEffect(CreateOptions)(input);
    const contentType =
      context.prepared?.contentType ??
      options.contentType ??
      context.connection.contentType ??
      (context.schema === undefined ? "application/octet-stream" : "application/json");
    if (
      context.schema !== undefined &&
      contentType.split(";")[0]?.trim().toLowerCase() !== "application/json"
    )
      return yield* new Errors.PayloadEncodeError({ component: "content-type" });
    const lifetime =
      options.lifetime === undefined
        ? {}
        : StreamLifetime.match(options.lifetime, {
            Ttl: ({ ttlSeconds }): Readonly<Record<string, string>> => ({
              "stream-ttl": String(ttlSeconds),
            }),
            ExpiresAt: ({ expiresAt }): Readonly<Record<string, string>> => ({
              "stream-expires-at": expiresAt,
            }),
          });
    if (options.lifetime !== undefined && StreamLifetime.guards.ExpiresAt(options.lifetime)) {
      yield* Schema.decodeEffect(Schema.DateTimeUtcFromString)(options.lifetime.expiresAt);
    }
    const body =
      context.prepared !== undefined
        ? context.prepared.body
        : Predicate.hasProperty(input, "values")
          ? yield* encodePayloads({
              ...context,
              values: yield* Schema.decodeEffect(Schema.Array(Schema.Unknown))(input.values),
              contentType,
              operation: "create",
            })
          : Predicate.hasProperty(input, "value")
            ? yield* encodePayload({
                ...context,
                value: input.value,
                contentType,
                operation: "create",
              })
            : undefined;
    const response = yield* sendMutation({
      connection: context.connection,
      operation: "create",
      method: "PUT",
      headers: {
        ...lifetime,
        "content-type": contentType,
        "stream-closed": options.closed ? "true" : undefined,
      },
      ...Record.filter({ body }, Predicate.isNotUndefined),
    });
    if (response.status !== 200 && response.status !== 201)
      return yield* protocolViolation({ component: "create status", response });
    const metadata = yield* parseHeadMetadata({
      headers: response.headers,
      requiresJson: context.schema !== undefined,
    }).pipe(
      Effect.tapError((failure) =>
        captureSchemaFailure({
          cause: failure.cause,
          operation: "create",
          component: failure.component,
          metadata: true,
        }),
      ),
      Effect.catchTag("HeadMetadataFailure", () =>
        protocolViolation({ component: "create headers", response }),
      ),
    );
    return CreateResult.make({
      status: response.status,
      contentType: metadata.contentType,
      offset: metadata.offset,
      closed: metadata.closed,
    });
  }).pipe(
    Effect.tapErrorTag("SchemaError", (cause) =>
      captureSchemaFailure({ cause, operation: "create", component: "create options" }),
    ),
    Effect.catchTags({
      SchemaError: () =>
        Effect.fail(new Errors.PayloadEncodeError({ component: "create options" })),
      MutationFailure: (failure) => {
        if (failure.response?.status === 409)
          return Effect.fail(new Errors.CreateConflictError({ response: failure.response }));
        if (failure.response?.status === 413)
          return Effect.fail(new Errors.PayloadTooLargeError({ response: failure.response }));
        return _commonRejection(failure);
      },
    }),
    Effect.tapError((error) =>
      Predicate.hasProperty(error, "response") ? freezeErrorResponse(error) : Effect.void,
    ),
    Effect.scoped,
    Effect.withSpan("durable_streams.create"),
  );

export const deleteStream = (
  connection: DurableStreamsConnection,
): Effect.Effect<void, Errors.DeleteError, HttpClient.HttpClient> =>
  sendMutation({ connection, operation: "delete", method: "DELETE" }).pipe(
    Effect.flatMap((response) =>
      response.status === 204
        ? Effect.void
        : protocolViolation({ component: "delete status", response }),
    ),
    Effect.catchTag("MutationFailure", (failure): Effect.Effect<never, Errors.DeleteError> => {
      if (failure.response?.status === 404)
        return Effect.fail(new Errors.StreamNotFoundError({ response: failure.response }));
      if (failure.response?.status === 405 || failure.response?.status === 501)
        return Effect.fail(
          new Errors.OperationNotSupportedError({
            operation: "delete",
            response: failure.response,
          }),
        );
      return _commonRejection(failure);
    }),
    Effect.tapError(freezeErrorResponse),
    Effect.scoped,
    Effect.withSpan("durable_streams.delete"),
  );

export type WriteRequest<S extends Schema.Top> = LifecycleContext<S> & {
  readonly input: CloseInput<S["Type"] | Uint8Array>;
  readonly operation: "append" | "close";
  readonly prepared?: {
    readonly body?: Uint8Array;
    readonly bodyStream?: Stream.Stream<Uint8Array, unknown>;
    readonly contentType: string;
  };
};

const _write = <S extends Schema.Top>(request: WriteRequest<S>) =>
  Effect.gen(function* () {
    const { input, operation, connection } = request;
    const contentType =
      request.prepared?.contentType ??
      connection.contentType ??
      (request.schema === undefined ? "application/octet-stream" : "application/json");
    const body =
      request.prepared !== undefined
        ? request.prepared.body
        : operation === "close" && !Predicate.hasProperty(input, "value")
          ? undefined
          : yield* encodePayload({ ...request, value: input.value, contentType });
    if (input.seq !== undefined) yield* Schema.decodeEffect(FieldValue)(input.seq);
    const response = yield* sendMutation({
      connection,
      method: "POST",
      operation,
      headers: {
        "content-type":
          body === undefined && request.prepared?.bodyStream === undefined
            ? undefined
            : contentType,
        "stream-seq": input.seq,
        "stream-closed": operation === "close" ? "true" : undefined,
      },
      ...Record.filter({ body }, Predicate.isNotUndefined),
      ...Record.filter({ bodyStream: request.prepared?.bodyStream }, Predicate.isNotUndefined),
    });
    if (response.status !== 204)
      return yield* protocolViolation({
        component: `${operation} status`,
        response,
      });
    const headers = yield* Schema.decodeUnknownEffect(WriteHeaders)(response.headers).pipe(
      Effect.tapError((cause) =>
        captureSchemaFailure({ cause, operation, component: "write headers", metadata: true }),
      ),
      Effect.catchTag("SchemaError", () =>
        protocolViolation({ component: `${operation} headers`, response }),
      ),
    );
    if (operation === "close" && headers["stream-closed"] !== "true")
      return yield* protocolViolation({
        component: "close confirmation",
        response,
      });
    return AppendResult.make({
      offset: headers["stream-next-offset"],
      closed: headers["stream-closed"] === "true",
    });
  }).pipe(
    Effect.tapErrorTag("SchemaError", (cause) =>
      captureSchemaFailure({ cause, operation: request.operation, component: "write options" }),
    ),
    Effect.catchTags({
      SchemaError: () => Effect.fail(new Errors.PayloadEncodeError({ component: "write options" })),
      MutationFailure: (failure) => {
        const response = failure.response;
        if (response?.status === 404)
          return Effect.fail(new Errors.StreamNotFoundError({ response }));
        if (response?.status === 413)
          return Effect.fail(new Errors.PayloadTooLargeError({ response }));
        if (response?.status === 405 || response?.status === 501)
          return Effect.fail(
            new Errors.OperationNotSupportedError({ operation: request.operation, response }),
          );
        if (response?.status === 409) {
          if (response.headers["stream-closed"] === "true") {
            const finalOffset = Schema.decodeUnknownOption(WriteHeaders)(response.headers).pipe(
              Option.map((headers) => headers["stream-next-offset"]),
            );
            return Effect.fail(
              new Errors.StreamClosedError({
                response,
                ...Record.filter(
                  { finalOffset: Option.getOrUndefined(finalOffset) },
                  Predicate.isNotUndefined,
                ),
              }),
            );
          }
          return Effect.fail(new Errors.AppendConflictError({ response }));
        }
        return _commonRejection(failure);
      },
    }),
    Effect.catchTag(
      "StreamUnavailableError",
      (
        error,
      ): Effect.Effect<never, Errors.StreamUnavailableError | Errors.AppendOutcomeUnknownError> =>
        request.operation === "close" && !Predicate.hasProperty(request.input, "value")
          ? Effect.fail(error)
          : Effect.fail(
              new Errors.AppendOutcomeUnknownError({
                ...Record.filter({ response: error.response }, Predicate.isNotUndefined),
              }),
            ),
    ),
    Effect.tapError((error) =>
      Predicate.hasProperty(error, "response") ? freezeErrorResponse(error) : Effect.void,
    ),
    Effect.scoped,
    Effect.withSpan(`durable_streams.${request.operation}`),
  );

export const appendStreamValue = <S extends Schema.Top>(
  context: LifecycleContext<S> & {
    readonly input: AppendInput<S["Type"] | Uint8Array>;
    readonly prepared?: WriteRequest<S>["prepared"];
  },
): Effect.Effect<AppendResult, Errors.AppendError, HttpClient.HttpClient | S["EncodingServices"]> =>
  _write({ ...context, operation: "append" }).pipe(
    Effect.catchTag("StreamUnavailableError", (error) =>
      Effect.fail(
        new Errors.AppendOutcomeUnknownError({
          ...Record.filter({ response: error.response }, Predicate.isNotUndefined),
        }),
      ),
    ),
  );

export const closeStream = <S extends Schema.Top>(
  context: LifecycleContext<S> & {
    readonly input: CloseInput<S["Type"] | Uint8Array>;
    readonly prepared?: WriteRequest<S>["prepared"];
  },
): Effect.Effect<CloseResult, Errors.CloseError, HttpClient.HttpClient | S["EncodingServices"]> =>
  _write({ ...context, operation: "close" }).pipe(
    Effect.map((result) => CloseResult.make({ finalOffset: result.offset })),
  );
