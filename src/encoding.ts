import { Data, Effect, Match, Predicate, Schema, Stream, type SchemaIssue } from "effect";
import { PayloadDecodeError, PayloadEncodeError } from "./errors.ts";

export type EncodeInput<S extends Schema.Top> = {
  readonly schema?: S;
  readonly value: S["Type"] | Schema.Json | Uint8Array;
  readonly contentType: string;
  readonly operation: "create" | "append" | "close";
};

export const captureSchemaFailure = (input: {
  readonly cause: Schema.SchemaError | SchemaIssue.Issue;
  readonly operation: string;
  readonly component: string;
  readonly metadata?: boolean;
}) => {
  const issues: Array<{ readonly issue: string; readonly path: ReadonlyArray<string> }> = [];
  const pending: Array<{
    readonly issue: SchemaIssue.Issue;
    readonly path: ReadonlyArray<string>;
  }> = [{ issue: Schema.isSchemaError(input.cause) ? input.cause.issue : input.cause, path: [] }];
  let visited = 0;
  while (visited++ < 64) {
    const current = pending.pop();
    if (current === undefined) break;
    const { issue, path } = current;
    issues.push({ issue: issue._tag, path });
    Match.value(issue).pipe(
      Match.tags({
        Pointer: (node) => {
          const segments = node.path
            .slice(0, 16)
            .map((key) =>
              input.metadata === true &&
              Predicate.isString(key) &&
              /^(?:content-type|stream-next-offset|stream-closed|stream-ttl|stream-expires-at|etag|cache-control)$/.test(
                key,
              )
                ? key
                : Predicate.isNumber(key)
                  ? "[index]"
                  : "[key]",
            );
          pending.push({ issue: node.issue, path: [...path, ...segments].slice(0, 16) });
        },
        Filter: (node) => {
          pending.push({ issue: node.issue, path });
        },
        Encoding: (node) => {
          pending.push({ issue: node.issue, path });
        },
        Composite: (node) => {
          pending.push(...node.issues.slice(0, 64).map((issue) => ({ issue, path })));
        },
        AnyOf: (node) => {
          pending.push(...node.issues.slice(0, 64).map((issue) => ({ issue, path })));
        },
      }),
      Match.orElse(() => undefined),
    );
  }
  return Effect.logWarning("Stream schema boundary failed", {
    operation: input.operation,
    component: input.component,
    issues,
  });
};

export const encodePayloads = <S extends Schema.Top>(
  input: Omit<EncodeInput<S>, "value"> & {
    readonly values: ReadonlyArray<EncodeInput<S>["value"]>;
  },
) =>
  Effect.gen(function* () {
    const json = input.contentType.split(";")[0]?.trim().toLowerCase() === "application/json";
    if (input.schema !== undefined && !json)
      return yield* new PayloadEncodeError({ component: "content-type" });
    if (json) {
      const schema = input.schema;
      const values =
        schema === undefined
          ? input.values
          : yield* Effect.forEach(input.values, (value) =>
              Schema.encodeUnknownEffect(schema)(value),
            );
      const encoded = yield* Schema.encodeUnknownEffect(
        Schema.fromJsonString(Schema.Array(Schema.Json)),
      )(values);
      return new TextEncoder().encode(encoded);
    }
    const chunks = yield* Effect.forEach(input.values, (value) =>
      Predicate.isString(value)
        ? Effect.succeed(new TextEncoder().encode(value))
        : Schema.decodeUnknownEffect(Schema.Uint8Array)(value),
    );
    const bytes = new Uint8Array(chunks.reduce((size, chunk) => size + chunk.length, 0));
    let offset = 0;
    for (const chunk of chunks) {
      bytes.set(chunk, offset);
      offset += chunk.length;
    }
    return bytes;
  }).pipe(
    Effect.tapErrorTag("SchemaError", (cause) =>
      captureSchemaFailure({ cause, operation: input.operation, component: "payload" }),
    ),
    Effect.catchTag("SchemaError", () =>
      Effect.fail(new PayloadEncodeError({ component: "payload" })),
    ),
  );

export const encodePayload = <S extends Schema.Top>(input: EncodeInput<S>) =>
  encodePayloads({ ...input, values: [input.value] });

export const combineAppendBodies = (input: {
  readonly bodies: ReadonlyArray<Uint8Array>;
  readonly contentType: string;
}) => {
  const json = input.contentType.split(";")[0]?.trim().toLowerCase() === "application/json";
  const chunks = input.bodies.map((body) => (json ? body.subarray(1, body.length - 1) : body));
  const bytes = new Uint8Array(
    chunks.reduce((size, chunk) => size + chunk.length, 0) + (json ? chunks.length + 1 : 0),
  );
  let offset = 0;
  if (json) bytes[offset++] = 91;
  for (const [index, chunk] of chunks.entries()) {
    if (json && index > 0) bytes[offset++] = 44;
    bytes.set(chunk, offset);
    offset += chunk.length;
  }
  if (json) bytes[offset] = 93;
  return bytes;
};

class Utf8Failure extends Data.TaggedError("Utf8Failure")<{
  readonly cause: unknown;
}> {}

export const allocateTextDecoder = () => {
  const decoder = new TextDecoder("utf-8", { fatal: true });
  const state = { pending: 0 };
  const decode = (bytes: Uint8Array | undefined) =>
    Effect.try({
      try: () => {
        const text = decoder.decode(bytes, { stream: bytes !== undefined });
        for (const byte of bytes ?? []) {
          if (state.pending > 0) state.pending--;
          else if (byte >= 0xf0) state.pending = 3;
          else if (byte >= 0xe0) state.pending = 2;
          else if (byte >= 0xc2) state.pending = 1;
        }
        return text;
      },
      catch: (cause) => new Utf8Failure({ cause }),
    }).pipe(
      Effect.tapError(() =>
        Effect.logWarning("Stream UTF-8 boundary failed", { operation: "read" }),
      ),
      Effect.catchTag("Utf8Failure", () =>
        Effect.fail(new PayloadDecodeError({ component: "UTF-8" })),
      ),
    );
  return {
    complete: Effect.sync(() => state.pending === 0),
    decode: <E, R>(input: {
      readonly source: Stream.Stream<Uint8Array, E, R>;
      readonly final: boolean;
    }) =>
      input.source.pipe(
        Stream.mapEffect(decode),
        Stream.concat(input.final ? Stream.fromEffect(decode(undefined)) : Stream.empty),
        Stream.filter((text) => text.length > 0),
      ),
  };
};

export const decodeText = <E, R>(source: Stream.Stream<Uint8Array, E, R>) =>
  Stream.suspend(() => allocateTextDecoder().decode({ source, final: true }));

const _decodeReadSchema = <S extends Schema.Top>(input: {
  readonly schema: S;
  readonly value: Schema.Json;
}) =>
  Schema.decodeEffect(input.schema)(input.value).pipe(
    Effect.tapError((cause) =>
      captureSchemaFailure({ cause, operation: "read", component: "JSON payload" }),
    ),
    Effect.catchTag("SchemaError", () =>
      Effect.fail(new PayloadDecodeError({ component: "JSON payload" })),
    ),
  );

export const decodeJson = <S extends Schema.Top, E, R>(input: {
  readonly source: Stream.Stream<Uint8Array, E, R>;
  readonly schema: S;
}) =>
  Stream.unwrap(
    decodeText(input.source).pipe(
      Stream.runFold(
        () => "",
        (text, chunk) => text + chunk,
      ),
      Effect.flatMap((value) =>
        _decodeReadSchema({ schema: Schema.fromJsonString(Schema.Array(Schema.Json)), value }),
      ),
      Effect.map((items) =>
        Stream.fromIterable(items).pipe(
          Stream.mapEffect((value) => _decodeReadSchema({ schema: input.schema, value })),
        ),
      ),
    ),
  );
