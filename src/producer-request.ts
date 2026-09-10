import { Data, Effect, Match, Option, Predicate, Record, Schema } from "effect";
import * as Errors from "./errors.js";
import { ProducerAppendResult, type DurableStreamsConnection } from "./model.js";
import { ProducerGapHeaders, ProducerNumber, ProducerSuccessHeaders } from "./protocol.js";
import { freezeErrorResponse, protocolViolation, sendMutation } from "./transport.js";
import { captureSchemaFailure } from "./encoding.js";
import { isRequestMetadataFailure } from "./request.js";

export type ProducerResponse = Data.TaggedEnum<{
  Delivered: { readonly result: ProducerAppendResult };
  Fenced: { readonly currentEpoch: number; readonly response: Errors.ErrorResponse };
  Gap: {
    readonly expectedSequence: number;
    readonly receivedSequence: number;
    readonly response: Errors.ErrorResponse;
  };
}>;
export const ProducerResponse = Data.taggedEnum<ProducerResponse>();
export type ProducerRequest = {
  readonly connection: DurableStreamsConnection;
  readonly contentType: string;
  readonly producerId: string;
  readonly epoch: number;
  readonly seq: number;
  readonly body?: Uint8Array;
  readonly close: boolean;
};

export const sendProducerRequest = (input: ProducerRequest) =>
  Effect.gen(function* () {
    const response = yield* sendMutation({
      connection: input.connection,
      method: "POST",
      operation: input.close ? "close" : "append",
      producer: true,
      headers: {
        "content-type": input.contentType,
        "producer-id": input.producerId,
        "producer-epoch": String(input.epoch),
        "producer-seq": String(input.seq),
        "stream-closed": input.close ? "true" : undefined,
      },
      ...Record.filter({ body: input.body }, Predicate.isNotUndefined),
    });
    if (response.status !== 200 && response.status !== 204)
      return yield* protocolViolation({ response, component: "producer status" });
    const headers = yield* Schema.decodeUnknownEffect(ProducerSuccessHeaders)(
      response.headers,
    ).pipe(
      Effect.tapError((cause) =>
        captureSchemaFailure({ cause, operation: "producer", component: "producer headers" }),
      ),
      Effect.catchTag("SchemaError", () =>
        protocolViolation({ response, component: "producer headers" }),
      ),
    );
    if (
      headers["producer-epoch"] !== input.epoch ||
      headers["producer-seq"] < input.seq ||
      ((response.status === 200 || input.close) && headers["stream-next-offset"] === undefined) ||
      (input.close && headers["stream-closed"] !== "true")
    )
      return yield* protocolViolation({ response, component: "producer confirmation" });
    return ProducerResponse.Delivered({
      result: ProducerAppendResult.make({
        duplicate: response.status === 204,
        producerSeq: headers["producer-seq"],
        ...Record.filter({ offset: headers["stream-next-offset"] }, Predicate.isNotUndefined),
      }),
    });
  }).pipe(
    Effect.catchTag("MutationFailure", (failure) => {
      if (failure.cause !== undefined && isRequestMetadataFailure(failure.cause))
        return Effect.fail(new Errors.ProtocolViolationError({ component: "request metadata" }));
      const response = failure.response;
      if (response === undefined) return Effect.fail(new Errors.StreamUnavailableError({}));
      return Match.value(response.status).pipe(
        Match.when(400, () => Effect.fail(new Errors.InvalidRequestError({ response }))),
        Match.when(401, () => Effect.fail(new Errors.UnauthorizedError({ response }))),
        Match.when(403, () => {
          const epoch = Schema.decodeOption(ProducerNumber)(response.headers["producer-epoch"]);
          return Option.isSome(epoch)
            ? Effect.succeed(ProducerResponse.Fenced({ currentEpoch: epoch.value, response }))
            : Effect.fail(new Errors.ForbiddenError({ response }));
        }),
        Match.when(404, () => Effect.fail(new Errors.StreamNotFoundError({ response }))),
        Match.when(409, () => {
          if (response.headers["stream-closed"] === "true")
            return Effect.fail(
              new Errors.StreamClosedError({
                response,
                ...Record.filter(
                  { finalOffset: response.headers["stream-next-offset"] },
                  Predicate.isNotUndefined,
                ),
              }),
            );
          return Schema.decodeUnknownEffect(ProducerGapHeaders)(response.headers).pipe(
            Effect.map((headers) =>
              ProducerResponse.Gap({
                expectedSequence: headers["producer-expected-seq"],
                receivedSequence: headers["producer-received-seq"],
                response,
              }),
            ),
            Effect.tapError((cause) =>
              captureSchemaFailure({ cause, operation: "producer", component: "gap headers" }),
            ),
            Effect.catchTag("SchemaError", () =>
              Effect.fail(
                new Errors.ProtocolViolationError({ response, component: "producer gap headers" }),
              ),
            ),
          );
        }),
        Match.when(410, () => Effect.fail(new Errors.StreamGoneError({ response }))),
        Match.when(413, () => Effect.fail(new Errors.PayloadTooLargeError({ response }))),
        Match.when(
          (status) => status === 405 || status === 501,
          () =>
            Effect.fail(
              new Errors.OperationNotSupportedError({
                response,
                operation: input.close ? "close" : "append",
              }),
            ),
        ),
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
          Effect.fail(
            new Errors.ProtocolViolationError({ response, component: "producer status" }),
          ),
        ),
      );
    }),
    Effect.tapError(freezeErrorResponse),
    Effect.scoped,
    Effect.withSpan("durable_streams.producer.request"),
  );
