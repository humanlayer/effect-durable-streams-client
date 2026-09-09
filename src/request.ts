import { Effect, Predicate, Record, Schema } from "effect";
import { HttpClientRequest } from "effect/unstable/http";
import { InvalidDurableStreamsConfigError } from "./errors.ts";
import type { DurableStreamsConnection } from "./model.ts";
import { FieldValue } from "./headers.ts";

const RESERVED_PARAMS = new Set(["offset", "live", "cursor"]);

export const checkExtensions = (connection: DurableStreamsConnection) =>
  Effect.gen(function* () {
    const headers = Object.keys(connection.headers ?? {});
    const params = [...Object.keys(connection.params ?? {}), ...connection.url.searchParams.keys()];
    if (
      headers.some((name) =>
        /^(stream-|producer-|content-type$|content-length$|transfer-encoding$)/i.test(name),
      ) ||
      params.some((key) => RESERVED_PARAMS.has(key))
    ) {
      return yield* new InvalidDurableStreamsConfigError({
        field: "extensions",
        issues: ["Protocol headers and query parameters are reserved"],
      });
    }
    if (
      Object.entries(connection.headers ?? {}).some(
        ([name, value]) => !/^[!#$%&'*+.^_`|~\w-]+$/.test(name) || !Schema.is(FieldValue)(value),
      )
    ) {
      return yield* new InvalidDurableStreamsConfigError({
        field: "headers",
        issues: ["Invalid HTTP header"],
      });
    }
  });

export type RequestInput = {
  readonly connection: DurableStreamsConnection;
  readonly method: "HEAD" | "PUT" | "POST" | "DELETE";
  readonly headers?: Readonly<Record<string, string | undefined>>;
  readonly body?: Uint8Array;
};

export const buildRequest = (input: RequestInput) => {
  const url = new URL(input.connection.url.href);
  for (const [key, value] of Object.entries(input.connection.params ?? {}))
    url.searchParams.set(key, value);
  url.searchParams.sort();
  const headers = {
    ...input.connection.headers,
    ...Record.filter(input.headers ?? {}, Predicate.isNotUndefined),
  };
  const request = HttpClientRequest.make(input.method)(url.href).pipe(
    HttpClientRequest.setHeaders(headers),
  );
  return input.body === undefined
    ? request
    : request.pipe(HttpClientRequest.bodyUint8Array(input.body, headers["content-type"]));
};
