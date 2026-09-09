import { Schema } from "effect";

export const ErrorResponseBody = Schema.TaggedUnion({
  Empty: {},
  Bytes: {
    value: Schema.Uint8Array,
    truncated: Schema.Boolean,
    contentType: Schema.optionalKey(Schema.String),
  },
});
export type ErrorResponseBody = typeof ErrorResponseBody.Type;

export const ErrorResponse = Schema.Struct({
  status: Schema.Int,
  url: Schema.String,
  headers: Schema.Record(Schema.String, Schema.String),
  body: ErrorResponseBody,
});
export type ErrorResponse = typeof ErrorResponse.Type;

export class InvalidDurableStreamsConfigError extends Schema.TaggedError<InvalidDurableStreamsConfigError>()(
  "InvalidDurableStreamsConfigError",
  { field: Schema.String, issues: Schema.Array(Schema.String) },
) {
  override get message() {
    return "Invalid Durable Streams configuration";
  }
}

export class UnauthorizedError extends Schema.TaggedError<UnauthorizedError>()(
  "UnauthorizedError",
  { response: ErrorResponse },
) {
  override get message() {
    return "Stream authentication required";
  }
}

export class ForbiddenError extends Schema.TaggedError<ForbiddenError>()("ForbiddenError", {
  response: ErrorResponse,
}) {
  override get message() {
    return "Stream access forbidden";
  }
}

export class StreamGoneError extends Schema.TaggedError<StreamGoneError>()("StreamGoneError", {
  response: ErrorResponse,
}) {
  override get message() {
    return "Stream is gone";
  }
}

export class RateLimitedError extends Schema.TaggedError<RateLimitedError>()("RateLimitedError", {
  response: ErrorResponse,
  retryAfter: Schema.optionalKey(Schema.Duration),
}) {
  override get message() {
    return "Stream request rate limited";
  }
}

export class StreamUnavailableError extends Schema.TaggedError<StreamUnavailableError>()(
  "StreamUnavailableError",
  { response: Schema.optionalKey(ErrorResponse) },
) {
  override get message() {
    return "Stream is unavailable";
  }
}

export class ProtocolViolationError extends Schema.TaggedError<ProtocolViolationError>()(
  "ProtocolViolationError",
  {
    component: Schema.String,
    response: Schema.optionalKey(ErrorResponse),
  },
) {
  override get message() {
    return "Invalid Durable Streams response";
  }
}

export type HeadError =
  | UnauthorizedError
  | ForbiddenError
  | StreamGoneError
  | RateLimitedError
  | StreamUnavailableError
  | ProtocolViolationError;
export type ConnectError = HeadError;

export class StreamNotFoundError extends Schema.TaggedError<StreamNotFoundError>()(
  "StreamNotFoundError",
  { response: ErrorResponse },
) {
  override get message() {
    return "Stream not found";
  }
}
export class CreateConflictError extends Schema.TaggedError<CreateConflictError>()(
  "CreateConflictError",
  { response: ErrorResponse },
) {
  override get message() {
    return "Stream creation conflicts with existing configuration";
  }
}
export class StreamClosedError extends Schema.TaggedError<StreamClosedError>()(
  "StreamClosedError",
  {
    response: ErrorResponse,
    finalOffset: Schema.optionalKey(Schema.String),
  },
) {
  override get message() {
    return "Stream is closed";
  }
}
export class AppendConflictError extends Schema.TaggedError<AppendConflictError>()(
  "AppendConflictError",
  { response: ErrorResponse },
) {
  override get message() {
    return "Stream append conflicts with stream state";
  }
}
export class InvalidRequestError extends Schema.TaggedError<InvalidRequestError>()(
  "InvalidRequestError",
  { response: ErrorResponse },
) {
  override get message() {
    return "Stream request rejected as invalid";
  }
}
export class PayloadTooLargeError extends Schema.TaggedError<PayloadTooLargeError>()(
  "PayloadTooLargeError",
  { response: ErrorResponse },
) {
  override get message() {
    return "Stream payload is too large";
  }
}
export class OperationNotSupportedError extends Schema.TaggedError<OperationNotSupportedError>()(
  "OperationNotSupportedError",
  {
    operation: Schema.Literals(["append", "close", "delete"]),
    response: ErrorResponse,
  },
) {
  override get message() {
    return "Stream operation is not supported";
  }
}
export class AppendOutcomeUnknownError extends Schema.TaggedError<AppendOutcomeUnknownError>()(
  "AppendOutcomeUnknownError",
  { response: Schema.optionalKey(ErrorResponse) },
) {
  override get message() {
    return "Stream append outcome is unknown; reconcile before retrying";
  }
}
export class PayloadEncodeError extends Schema.TaggedError<PayloadEncodeError>()(
  "PayloadEncodeError",
  { component: Schema.String },
) {
  override get message() {
    return "Unable to encode stream request";
  }
}

export type CreateError =
  | UnauthorizedError
  | ForbiddenError
  | StreamGoneError
  | CreateConflictError
  | InvalidRequestError
  | PayloadTooLargeError
  | RateLimitedError
  | StreamUnavailableError
  | PayloadEncodeError
  | ProtocolViolationError;
export type DeleteError =
  | UnauthorizedError
  | ForbiddenError
  | StreamNotFoundError
  | StreamGoneError
  | InvalidRequestError
  | OperationNotSupportedError
  | RateLimitedError
  | StreamUnavailableError
  | ProtocolViolationError;
export type AppendError =
  | UnauthorizedError
  | ForbiddenError
  | StreamNotFoundError
  | StreamGoneError
  | StreamClosedError
  | AppendConflictError
  | InvalidRequestError
  | PayloadTooLargeError
  | OperationNotSupportedError
  | RateLimitedError
  | AppendOutcomeUnknownError
  | PayloadEncodeError
  | ProtocolViolationError;
export type CloseError = AppendError | StreamUnavailableError;

export class AlreadyConsumedError extends Schema.TaggedError<AlreadyConsumedError>()(
  "AlreadyConsumedError",
  {},
) {
  override get message() {
    return "Stream read session has already been consumed";
  }
}

export class PayloadDecodeError extends Schema.TaggedError<PayloadDecodeError>()(
  "PayloadDecodeError",
  { component: Schema.String },
) {
  override get message() {
    return "Unable to decode stream payload";
  }
}

export type ReadError =
  | AlreadyConsumedError
  | UnauthorizedError
  | ForbiddenError
  | StreamNotFoundError
  | StreamGoneError
  | InvalidRequestError
  | RateLimitedError
  | StreamUnavailableError
  | PayloadDecodeError
  | ProtocolViolationError;
