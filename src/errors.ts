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
