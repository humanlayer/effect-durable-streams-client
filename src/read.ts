import { Effect, Option, Predicate, Record, Ref, Schema, Stream, Schedule } from "effect";
import * as Errors from "./errors.ts";
import { captureSchemaFailure, decodeJson, allocateTextDecoder } from "./encoding.ts";
import type { DurableStreamsConnection, Offset } from "./model.ts";
import { ReadHeaders } from "./protocol.ts";
import { freezeErrorResponse, protocolViolation, sendCatchupRequest } from "./transport.ts";

export const allocateRead = <S extends Schema.Top>(input: {
  readonly connection: DurableStreamsConnection;
  readonly schema: S;
  readonly hasSchema: boolean;
}) =>
  Effect.gen(function* () {
    const consumed = yield* Ref.make(false);
    const offset = yield* Ref.make<Option.Option<Offset>>(Option.none());
    const view = <A, R>(options: {
      readonly decode: (input: {
        readonly source: Stream.Stream<Uint8Array, Errors.StreamUnavailableError>;
        readonly final: boolean;
      }) => Stream.Stream<A, Errors.ReadError, R>;
      readonly complete: Effect.Effect<boolean>;
      readonly json: boolean;
    }) => {
      type CatchupState = {
        position: { readonly offset: string; readonly cursor?: string };
        done: boolean;
      };
      const state: CatchupState = {
        position: { offset: input.connection.offset ?? "-1" },
        done: false,
      };
      const page = Stream.unwrap(
        Effect.gen(function* () {
          const response = yield* sendCatchupRequest({
            connection: input.connection,
            position: state.position,
          });
          const headers = yield* Schema.decodeUnknownEffect(ReadHeaders)(response.headers).pipe(
            Effect.tapError((cause) =>
              captureSchemaFailure({
                cause,
                operation: "read",
                component: "read headers",
                metadata: true,
              }),
            ),
            Effect.catchTag("SchemaError", () =>
              protocolViolation({ response, component: "read headers" }),
            ),
          );
          if (
            (options.json || input.hasSchema) &&
            headers["content-type"].split(";")[0]?.trim().toLowerCase() !== "application/json"
          )
            return yield* protocolViolation({ response, component: "read content-type" });
          const body = response.stream.pipe(
            Stream.catchReason("HttpClientError", "EmptyBodyError", () => Stream.empty),
            Stream.tapError((failure) =>
              Effect.logWarning("Stream response body failed", {
                operation: "read",
                transport: failure.reason._tag,
              }),
            ),
            Stream.catchTag("HttpClientError", () =>
              Stream.fail(new Errors.StreamUnavailableError({})),
            ),
          );
          return options
            .decode({
              source: body,
              final: headers["stream-up-to-date"] === "true" || headers["stream-closed"] === "true",
            })
            .pipe(
              Stream.concat(
                Stream.fromEffectDrain(
                  Effect.gen(function* () {
                    if (yield* options.complete)
                      yield* Ref.set(offset, Option.some(headers["stream-next-offset"]));
                    state.done =
                      headers["stream-up-to-date"] === "true" ||
                      headers["stream-closed"] === "true";
                    state.position = {
                      offset: headers["stream-next-offset"],
                      ...Record.filter(
                        { cursor: headers["stream-cursor"] ?? state.position.cursor },
                        Predicate.isNotUndefined,
                      ),
                    };
                  }),
                ),
              ),
            );
        }),
      ).pipe(
        Stream.repeat(Schedule.forever.pipe(Schedule.while(() => Effect.succeed(!state.done)))),
      );
      return Stream.unwrap(
        Effect.gen(function* () {
          if (yield* Ref.getAndSet(consumed, true)) return yield* new Errors.AlreadyConsumedError();
          if (input.connection.live !== undefined)
            return yield* Effect.die(
              "Durable Streams live reads are not implemented until Phases 5–6",
            );
          return page;
        }),
      ).pipe(
        Stream.tapError((error) =>
          Predicate.hasProperty(error, "response") ? freezeErrorResponse(error) : Effect.void,
        ),
        Stream.withSpan("durable_streams.read"),
      );
    };
    const textDecoder = allocateTextDecoder();
    return {
      bytes: view({ decode: ({ source }) => source, json: false, complete: Effect.succeed(true) }),
      text: view({
        decode: (input) => textDecoder.decode(input),
        complete: textDecoder.complete,
        json: false,
      }),
      json: view({
        decode: ({ source }) => decodeJson({ source, schema: input.schema }),
        json: true,
        complete: Effect.succeed(true),
      }),
      offset: Ref.get(offset).pipe(Effect.withSpan("durable_streams.offset")),
    };
  });
