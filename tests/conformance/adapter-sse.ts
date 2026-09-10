import { Effect, Stream } from "effect";
import { parseSse, SseEvent } from "../../src/sse.ts";
import type { SseControl } from "../../src/protocol.ts";

export const observeSse = <E, R>(input: {
  readonly source: Stream.Stream<Uint8Array, E, R>;
  readonly before: (control: SseControl) => Effect.Effect<void>;
  readonly after: Effect.Effect<void>;
}) =>
  Stream.suspend(() => {
    const state = { bytes: new Array<number>(), lineLength: 0, afterCr: false };
    const frames = input.source.pipe(
      Stream.flatMap((chunk) => {
        const frames: Array<Uint8Array> = [];
        for (const byte of chunk) {
          if (state.afterCr && byte === 10) {
            state.afterCr = false;
            continue;
          }
          state.afterCr = byte === 13;
          state.bytes.push(byte);
          if (byte !== 10 && byte !== 13) {
            state.lineLength++;
            continue;
          }
          if (state.lineLength === 0) {
            frames.push(new Uint8Array(state.bytes));
            state.bytes = [];
          }
          state.lineLength = 0;
        }
        return Stream.fromIterable(frames);
      }),
      Stream.concat(Stream.suspend(() => Stream.succeed(new Uint8Array(state.bytes)))),
    );
    return frames.pipe(
      Stream.flatMap((frame) =>
        Stream.fromEffectDrain(
          parseSse(Stream.succeed(frame)).pipe(
            Stream.runForEach((event) =>
              SseEvent.$match(event, {
                Data: () => Effect.void,
                Control: ({ control }) => input.before(control),
              }),
            ),
            Effect.catchTag("ProtocolViolationError", () =>
              Effect.logDebug("Leaving invalid SSE framing to the SDK decoder"),
            ),
          ),
        ).pipe(
          Stream.concat(Stream.succeed(frame)),
          Stream.concat(Stream.fromEffectDrain(input.after)),
        ),
      ),
    );
  });
