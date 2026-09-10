import { Clock, DateTime, Duration, Effect, Option, Random, Schedule } from "effect";
import type { DurableStreamsConnection } from "./model.ts";

export const parseRetryAfter = (raw: string | undefined) =>
  Effect.gen(function* () {
    if (raw === undefined || raw === "") return Option.none<Duration.Duration>();
    const seconds = Number(raw);
    if (Number.isFinite(seconds) && seconds > 0)
      return Option.some(Duration.millis(seconds * 1000));
    const now = yield* Clock.currentTimeMillis;
    return DateTime.make(raw).pipe(
      Option.map((date) =>
        Duration.millis(Math.max(0, Math.min(DateTime.toEpochMillis(date) - now, 3600000))),
      ),
    );
  });

export type RetryDelayInput = { readonly retryAfter?: Duration.Duration };

export const waitForSseReconnect = (input: {
  readonly connection: DurableStreamsConnection;
  readonly shortConnections: number;
}) =>
  Effect.void.pipe(
    Effect.repeat(
      Schedule.recurs(1).pipe(
        Schedule.modifyDelay(() =>
          Random.next.pipe(
            Effect.map((random) =>
              Duration.millis(
                Math.floor(
                  random *
                    Math.min(
                      input.connection.sseResilience?.backoffMaxDelay ?? 5000,
                      (input.connection.sseResilience?.backoffBaseDelay ?? 100) *
                        2 ** input.shortConnections,
                    ),
                ),
              ),
            ),
          ),
        ),
      ),
    ),
    Effect.withSpan("durable_streams.read.sse_reconnect"),
  );

export const requestRetrySchedule = (connection: DurableStreamsConnection) => {
  const options = connection.backoffOptions;
  const initial = options?.initialDelay ?? 100;
  const cap = options?.maxDelay ?? 60000;
  const multiplier = options?.multiplier ?? 1.3;
  return Schedule.recurs(options?.maxRetries ?? Infinity).pipe(
    Schedule.modifyDelay(({ attempt }) =>
      Random.next.pipe(
        Effect.map((random) =>
          Duration.millis(
            Math.min(
              random *
                (attempt === 1 ? initial : Math.min(initial * multiplier ** (attempt - 1), cap)),
              cap,
            ),
          ),
        ),
      ),
    ),
    Schedule.passthrough<number, RetryDelayInput, never, never>,
    Schedule.modifyDelay(({ input: failure, duration: delay }) =>
      Effect.succeed(
        failure.retryAfter === undefined
          ? delay
          : Duration.max(Duration.fromInputUnsafe(delay), failure.retryAfter),
      ),
    ),
  );
};
