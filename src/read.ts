import { Clock, Effect, Option, Predicate, Record, Ref, Schema, Stream, Schedule } from "effect";
import * as Errors from "./errors.ts";
import { captureSchemaFailure, decodeJson, allocateTextDecoder } from "./encoding.ts";
import type { DurableStreamsConnection, Offset } from "./model.ts";
import { LongPollEmptyHeaders, LongPollHeaders, ReadHeaders } from "./protocol.ts";
import { freezeErrorResponse, protocolViolation, sendReadRequest } from "./transport.ts";
import { decodeSseData, parseSse, SseEvent } from "./sse.ts";
import { waitForSseReconnect } from "./retry.ts";

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
        readonly source: Stream.Stream<Uint8Array, Errors.ReadError>;
        readonly final: boolean;
      }) => Stream.Stream<A, Errors.ReadError, R>;
      readonly complete: Effect.Effect<boolean>;
      readonly json: boolean;
    }) => {
      type ReadState = {
        position: { readonly offset: string; readonly cursor?: string };
        done: boolean;
        longPoll: boolean;
        sse: boolean;
        fallback: boolean;
        contentType: string;
        shortConnections: number;
        reconnect: boolean;
      };
      const state: ReadState = {
        position: { offset: input.connection.offset ?? "-1" },
        done: false,
        longPoll: false,
        sse: false,
        fallback: false,
        contentType: "",
        shortConnections: 0,
        reconnect: false,
      };
      const page = Stream.unwrap(
        Effect.gen(function* () {
          const response = yield* sendReadRequest({
            connection: input.connection,
            position: state.position,
            longPoll: state.longPoll,
            sse: state.sse,
          });
          if (state.sse) {
            const started = yield* Clock.currentTimeMillis;
            const base64 = response.headers["stream-sse-data-encoding"] === "base64";
            const text =
              state.contentType === "application/json" || state.contentType.startsWith("text/");
            if (
              response.headers["content-type"]?.split(";")[0]?.trim().toLowerCase() !==
                "text/event-stream" ||
              (!text && !base64) ||
              (response.headers["stream-sse-data-encoding"] !== undefined && !base64)
            )
              return yield* Effect.logWarning("Invalid SSE response headers", {
                operation: "read",
                component: "SSE headers",
                status: response.status,
              }).pipe(
                Effect.andThen(
                  Effect.fail(new Errors.ProtocolViolationError({ component: "SSE headers" })),
                ),
              );
            const pending: Array<string> = [];
            const events = parseSse(
              response.stream.pipe(
                Stream.catchReason("HttpClientError", "EmptyBodyError", () => Stream.empty),
                Stream.tapError((failure) =>
                  Effect.logWarning("SSE response body failed", { transport: failure.reason._tag }),
                ),
              ),
            ).pipe(Stream.catchTag("HttpClientError", () => Stream.empty));
            return events.pipe(
              Stream.takeUntil((event) =>
                SseEvent.$match(event, {
                  Data: () => false,
                  Control: ({ control }) => control.streamClosed === true,
                }),
              ),
              Stream.flatMap((event) =>
                SseEvent.$match(event, {
                  Data: ({ data }) => {
                    pending.push(data);
                    return Stream.empty;
                  },
                  Control: ({ control }) => {
                    const parts = pending.splice(0);
                    const closed = control.streamClosed === true;
                    const source = Stream.fromIterable(parts).pipe(
                      Stream.mapEffect((data) => decodeSseData({ data, base64 })),
                    );
                    const decoded = options.json
                      ? source.pipe(
                          Stream.flatMap((bytes) =>
                            options.decode({ source: Stream.succeed(bytes), final: false }),
                          ),
                        )
                      : options.decode({ source, final: closed });
                    return decoded.pipe(
                      Stream.concat(
                        Stream.fromEffectDrain(
                          Effect.gen(function* () {
                            if (yield* options.complete)
                              yield* Ref.set(offset, Option.some(control.streamNextOffset));
                            state.position = {
                              offset: control.streamNextOffset,
                              ...Record.filter(
                                { cursor: control.streamCursor ?? state.position.cursor },
                                Predicate.isNotUndefined,
                              ),
                            };
                            state.done = closed;
                          }),
                        ),
                      ),
                    );
                  },
                }),
              ),
              Stream.concat(
                Stream.fromEffectDrain(
                  Effect.gen(function* () {
                    if (state.done) return;
                    const duration = (yield* Clock.currentTimeMillis) - started;
                    state.shortConnections =
                      duration < (input.connection.sseResilience?.minConnectionDuration ?? 1000)
                        ? state.shortConnections + 1
                        : 0;
                    if (
                      state.shortConnections >=
                      (input.connection.sseResilience?.maxShortConnections ?? 3)
                    ) {
                      state.fallback = true;
                      state.sse = false;
                      state.longPoll = true;
                      if (input.connection.sseResilience?.logWarnings !== false)
                        yield* Effect.logWarning(
                          "Short SSE connections: falling back to long-poll",
                        );
                    } else state.reconnect = state.shortConnections > 0;
                  }),
                ),
              ),
            );
          }
          const headers = yield* Schema.decodeUnknownEffect(
            response.status === 204
              ? LongPollEmptyHeaders
              : state.longPoll
                ? LongPollHeaders
                : ReadHeaders,
          )(response.headers).pipe(
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
            response.status !== 204 &&
            (options.json || input.hasSchema) &&
            headers["content-type"]?.split(";")[0]?.trim().toLowerCase() !== "application/json"
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
          const done =
            headers["stream-closed"] === "true" ||
            (input.connection.live === undefined && headers["stream-up-to-date"] === "true");
          const decoded =
            response.status === 204 && options.json
              ? Stream.empty
              : options.decode({
                  source: response.status === 204 ? Stream.empty : body,
                  final: done,
                });
          return decoded.pipe(
            Stream.concat(
              Stream.fromEffectDrain(
                Effect.gen(function* () {
                  if (yield* options.complete)
                    yield* Ref.set(offset, Option.some(headers["stream-next-offset"]));
                  state.done = done;
                  state.longPoll =
                    (input.connection.live === "long-poll" || state.fallback) &&
                    headers["stream-up-to-date"] === "true";
                  state.sse =
                    input.connection.live === "sse" &&
                    !state.fallback &&
                    headers["stream-up-to-date"] === "true";
                  state.contentType =
                    headers["content-type"]?.split(";")[0]?.trim().toLowerCase() ??
                    state.contentType;
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
        Stream.concat(
          Stream.fromEffectDrain(
            Effect.suspend(() => {
              if (!state.reconnect) return Effect.void;
              state.reconnect = false;
              return waitForSseReconnect({
                connection: input.connection,
                shortConnections: state.shortConnections,
              });
            }),
          ),
        ),
        Stream.repeat(Schedule.forever.pipe(Schedule.while(() => Effect.succeed(!state.done)))),
      );
      return Stream.unwrap(
        Effect.gen(function* () {
          if (yield* Ref.getAndSet(consumed, true)) return yield* new Errors.AlreadyConsumedError();
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
