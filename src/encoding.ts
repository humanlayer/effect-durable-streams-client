import { Effect, Match, Predicate, Schema, type SchemaIssue } from "effect";
import { PayloadEncodeError } from "./errors.ts";

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
