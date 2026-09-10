import { Array as Arr, Data, Effect, Schema, Stream } from "effect";
import { captureSchemaFailure, decodeText } from "./encoding.js";
import { ProtocolViolationError } from "./errors.js";
import { SseControl } from "./protocol.js";

export type SseEvent = Data.TaggedEnum<{
  Data: { readonly data: string };
  Control: { readonly control: SseControl };
}>;
export const SseEvent = Data.taggedEnum<SseEvent>();

export const parseSse = <E, R>(source: Stream.Stream<Uint8Array, E, R>) =>
  Stream.suspend(() => {
    const state = { line: "", afterCr: false, event: "", data: new Array<string>() };
    const line = () => {
      const value = state.line;
      state.line = "";
      if (value === "") {
        const event = { event: state.event, data: state.data };
        state.event = "";
        state.data = [];
        return [event];
      }
      const colon = value.indexOf(":");
      const field = colon === -1 ? value : value.slice(0, colon);
      const raw = colon === -1 ? "" : value.slice(colon + 1);
      const content = raw.startsWith(" ") ? raw.slice(1) : raw;
      if (field === "event") state.event = content;
      if (field === "data") state.data.push(content);
      return [];
    };
    return decodeText(source).pipe(
      Stream.catchTag("PayloadDecodeError", () =>
        Stream.fail(new ProtocolViolationError({ component: "SSE UTF-8 framing" })),
      ),
      Stream.flatMap((text) => {
        const events: Array<{ readonly event: string; readonly data: ReadonlyArray<string> }> = [];
        for (const char of text) {
          if (state.afterCr && char === "\n") {
            state.afterCr = false;
            continue;
          }
          state.afterCr = char === "\r";
          if (char === "\r" || char === "\n") events.push(...line());
          else state.line += char;
        }
        return Stream.fromIterable(events);
      }),
      Stream.flatMap((event) => {
        if (!Arr.isReadonlyArrayNonEmpty(event.data)) return Stream.empty;
        const data = event.data.join("\n");
        if (event.event === "data") return Stream.succeed<SseEvent>(SseEvent.Data({ data }));
        if (event.event !== "control") return Stream.empty;
        return Stream.fromEffect(
          Schema.decodeEffect(Schema.fromJsonString(SseControl))(data).pipe(
            Effect.tapError((cause) =>
              captureSchemaFailure({
                cause,
                operation: "read",
                component: "SSE control",
                metadata: true,
              }),
            ),
            Effect.catchTag("SchemaError", () =>
              Effect.fail(new ProtocolViolationError({ component: "SSE control" })),
            ),
            Effect.map((control) => SseEvent.Control({ control })),
          ),
        );
      }),
    );
  });

export const decodeSseData = (input: { readonly data: string; readonly base64: boolean }) => {
  if (!input.base64) return Effect.succeed(new TextEncoder().encode(input.data));
  return Schema.decodeEffect(Schema.Uint8ArrayFromBase64)(input.data.replace(/[\r\n]/g, "")).pipe(
    Effect.tapError((cause) =>
      captureSchemaFailure({ cause, operation: "read", component: "SSE base64" }),
    ),
    Effect.catchTag("SchemaError", () =>
      Effect.fail(new ProtocolViolationError({ component: "SSE base64" })),
    ),
  );
};
