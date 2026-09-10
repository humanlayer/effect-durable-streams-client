import { describe, expect, it } from "@effect/vitest";
import { Effect, Stream } from "effect";
import { decodeSseData, parseSse, SseEvent } from "../src/sse.js";

describe("SSE framing", () => {
  for (const ending of ["\n", "\r", "\r\n"]) {
    it.effect(`parses every byte split with newline length ${ending.length}`, () =>
      Effect.gen(function* () {
        const wire = [
          ": comment",
          "id: ignored",
          "retry: 0",
          "event: ignored",
          "data: ignored",
          "",
          "event: data",
          "data: hé",
          "data:  there",
          "",
          "event: control",
          'data: {"streamNextOffset":"Opaque+:%#","streamCursor":"c"}',
          "",
          "",
        ].join(ending);
        const bytes = new TextEncoder().encode(wire);
        for (const split of Array.from({ length: bytes.length + 1 }, (_, n) => n)) {
          expect(
            yield* parseSse(Stream.make(bytes.slice(0, split), bytes.slice(split))).pipe(
              Stream.runCollect,
            ),
          ).toEqual([
            SseEvent.Data({ data: "hé\n there" }),
            SseEvent.Control({ control: { streamNextOffset: "Opaque+:%#", streamCursor: "c" } }),
          ]);
        }
      }),
    );
  }
  it.effect("discards unterminated events and accepts empty data fields", () =>
    Effect.gen(function* () {
      expect(
        yield* parseSse(
          Stream.make(new TextEncoder().encode("event: data\ndata\n\nevent: control\ndata: {}")),
        ).pipe(Stream.runCollect),
      ).toEqual([SseEvent.Data({ data: "" })]);
    }),
  );
  for (const control of [
    "{",
    "null",
    "{}",
    '{"streamNextOffset":"now","streamClosed":true}',
    '{"streamNextOffset":"x"}',
    '{"streamNextOffset":"x","streamCursor":"c","upToDate":"true"}',
  ]) {
    it.effect(`rejects invalid control ${control}`, () =>
      Effect.gen(function* () {
        const failure = yield* parseSse(
          Stream.make(new TextEncoder().encode(`event: control\ndata: ${control}\n\n`)),
        ).pipe(Stream.runDrain, Effect.flip);
        expect(failure).toHaveProperty("_tag", "ProtocolViolationError");
      }),
    );
  }
  it.effect("decodes base64 per event with inserted newlines and rejects malformed encoding", () =>
    Effect.gen(function* () {
      expect(yield* decodeSseData({ data: "AQID\r\nBA==", base64: true })).toEqual(
        new Uint8Array([1, 2, 3, 4]),
      );
      expect(yield* decodeSseData({ data: "", base64: true })).toEqual(new Uint8Array());
      for (const data of ["A", "AA", "%%%%", "AQ==AQ==", "AA-_", " AQA="])
        expect(yield* decodeSseData({ data, base64: true }).pipe(Effect.flip)).toHaveProperty(
          "_tag",
          "ProtocolViolationError",
        );
    }),
  );
});
