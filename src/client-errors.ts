import { Cause, Duration, Effect, Exit, Option } from "effect";
import type * as Native from "./errors";

export type DurableStreamErrorCode =
  | "BAD_REQUEST"
  | "UNAUTHORIZED"
  | "FORBIDDEN"
  | "NOT_FOUND"
  | "GONE"
  | "CONFLICT_EXISTS"
  | "CONFLICT_SEQ"
  | "STREAM_CLOSED"
  | "ALREADY_CONSUMED"
  | "ALREADY_CLOSED"
  | "RATE_LIMITED"
  | "BUSY"
  | "APPEND_OUTCOME_UNKNOWN"
  | "PAYLOAD_TOO_LARGE"
  | "OPERATION_NOT_SUPPORTED"
  | "PARSE_ERROR"
  | "PROTOCOL_ERROR"
  | "STALE_EPOCH"
  | "SEQUENCE_GAP"
  | "REQUEST_METADATA_ERROR"
  | "CALLBACK_ERROR"
  | "SOURCE_ERROR"
  | "ABORTED"
  | "INTERNAL_ERROR";

export type ErrorResponseSnapshot = Native.ErrorResponse;

export class DurableStreamError extends Error {
  readonly code: DurableStreamErrorCode;
  readonly status: number | undefined;
  readonly response: ErrorResponseSnapshot | undefined;
  readonly component: string | undefined;
  readonly retryAfter: number | undefined;
  constructor(input: {
    readonly code: DurableStreamErrorCode;
    readonly response?: ErrorResponseSnapshot;
    readonly component?: string;
    readonly retryAfter?: number;
  }) {
    super(`Durable Streams: ${input.code}`);
    this.name = "DurableStreamError";
    this.code = input.code;
    this.response = input.response;
    this.status = input.response?.status;
    this.component = input.component;
    this.retryAfter = input.retryAfter;
  }
}

export class InvalidClientOptionsError extends DurableStreamError {
  constructor() {
    super({ code: "BAD_REQUEST" });
    this.name = "InvalidClientOptionsError";
  }
}
export class AbortError extends DurableStreamError {
  constructor() {
    super({ code: "ABORTED" });
    this.name = "AbortError";
  }
}
export class ClientInternalError extends DurableStreamError {
  constructor() {
    super({ code: "INTERNAL_ERROR" });
    this.name = "ClientInternalError";
  }
}
export class StreamClosedError extends DurableStreamError {
  readonly streamClosed = true;
  readonly finalOffset: string | undefined;
  constructor(input: { readonly finalOffset?: string; readonly response?: ErrorResponseSnapshot }) {
    super({ code: "STREAM_CLOSED", response: input.response });
    this.name = "StreamClosedError";
    this.finalOffset = input.finalOffset;
  }
}
export class StaleEpochError extends DurableStreamError {
  readonly currentEpoch: number;
  constructor(input: { readonly currentEpoch: number; readonly response?: ErrorResponseSnapshot }) {
    super({ code: "STALE_EPOCH", response: input.response });
    this.name = "StaleEpochError";
    this.currentEpoch = input.currentEpoch;
  }
}
export class SequenceGapError extends DurableStreamError {
  readonly expectedSeq: number;
  readonly receivedSeq: number;
  constructor(input: {
    readonly expectedSeq: number;
    readonly receivedSeq: number;
    readonly response?: ErrorResponseSnapshot;
  }) {
    super({ code: "SEQUENCE_GAP", response: input.response });
    this.name = "SequenceGapError";
    this.expectedSeq = input.expectedSeq;
    this.receivedSeq = input.receivedSeq;
  }
}

export type NativeClientError =
  | Native.InvalidDurableStreamsConfigError
  | Native.CreateError
  | Native.DeleteError
  | Native.CloseError
  | Native.ReadError
  | Native.ProducerError;

export const mapClientErrors = <A, R>(effect: Effect.Effect<A, NativeClientError, R>) =>
  effect.pipe(
    Effect.catchTags({
      InvalidDurableStreamsConfigError: () => Effect.fail(new InvalidClientOptionsError()),
      UnauthorizedError: (e) =>
        Effect.fail(new DurableStreamError({ code: "UNAUTHORIZED", response: e.response })),
      ForbiddenError: (e) =>
        Effect.fail(new DurableStreamError({ code: "FORBIDDEN", response: e.response })),
      StreamNotFoundError: (e) =>
        Effect.fail(new DurableStreamError({ code: "NOT_FOUND", response: e.response })),
      StreamGoneError: (e) =>
        Effect.fail(new DurableStreamError({ code: "GONE", response: e.response })),
      CreateConflictError: (e) =>
        Effect.fail(new DurableStreamError({ code: "CONFLICT_EXISTS", response: e.response })),
      AppendConflictError: (e) =>
        Effect.fail(new DurableStreamError({ code: "CONFLICT_SEQ", response: e.response })),
      StreamClosedError: (e) => Effect.fail(new StreamClosedError(e)),
      AlreadyConsumedError: () => Effect.fail(new DurableStreamError({ code: "ALREADY_CONSUMED" })),
      ProducerClosedError: () => Effect.fail(new DurableStreamError({ code: "ALREADY_CLOSED" })),
      InvalidRequestError: (e) =>
        Effect.fail(new DurableStreamError({ code: "BAD_REQUEST", response: e.response })),
      PayloadEncodeError: (e) =>
        Effect.fail(
          new DurableStreamError({
            code:
              e.component === "upload source" || e.component === "append body"
                ? "SOURCE_ERROR"
                : "BAD_REQUEST",
            component: e.component,
          }),
        ),
      PayloadDecodeError: (e) =>
        Effect.fail(
          new DurableStreamError({
            code: e.component === "subscription callback" ? "CALLBACK_ERROR" : "PARSE_ERROR",
            component: e.component,
          }),
        ),
      ProtocolViolationError: (e) =>
        Effect.fail(
          new DurableStreamError({
            code: e.component === "request metadata" ? "REQUEST_METADATA_ERROR" : "PROTOCOL_ERROR",
            response: e.response,
            component: e.component,
          }),
        ),
      RateLimitedError: (e) =>
        Effect.fail(
          new DurableStreamError({
            code: "RATE_LIMITED",
            response: e.response,
            retryAfter: e.retryAfter === undefined ? undefined : Duration.toMillis(e.retryAfter),
          }),
        ),
      StreamUnavailableError: (e) =>
        Effect.fail(new DurableStreamError({ code: "BUSY", response: e.response })),
      AppendOutcomeUnknownError: (e) =>
        Effect.fail(
          new DurableStreamError({ code: "APPEND_OUTCOME_UNKNOWN", response: e.response }),
        ),
      PayloadTooLargeError: (e) =>
        Effect.fail(new DurableStreamError({ code: "PAYLOAD_TOO_LARGE", response: e.response })),
      OperationNotSupportedError: (e) =>
        Effect.fail(
          new DurableStreamError({ code: "OPERATION_NOT_SUPPORTED", response: e.response }),
        ),
      ProducerFencedError: (e) => Effect.fail(new StaleEpochError(e)),
      ProducerSequenceGapError: (e) =>
        Effect.fail(
          new SequenceGapError({
            expectedSeq: e.expectedSequence,
            receivedSeq: e.receivedSequence,
            response: e.response,
          }),
        ),
    }),
  );

export const unwrapClientExit = <A>(exit: Exit.Exit<A, DurableStreamError>) => {
  if (Exit.isSuccess(exit)) return exit.value;
  const error = Cause.findErrorOption(exit.cause);
  if (Option.isSome(error)) throw error.value;
  if (Cause.hasInterruptsOnly(exit.cause)) throw new AbortError();
  Effect.runSync(Effect.logError("Client interpreter defect captured"));
  throw new ClientInternalError();
};
