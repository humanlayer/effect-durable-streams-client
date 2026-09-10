import { Data, Effect, Predicate, Record, Schema, Stream } from "effect";
import { HttpClientRequest } from "effect/unstable/http";
import { InvalidDurableStreamsConfigError } from "./errors.js";
import type { DurableStreamsConnection } from "./model.js";
import { FieldValue } from "./headers.js";

const RESERVED_PARAMS = new Set(["offset", "live", "cursor"]);

export class RequestMetadataFailure extends Data.TaggedError("RequestMetadataFailure")<{}> {}

export const isRequestMetadataFailure = (failure: { readonly reason: object }) =>
  Predicate.hasProperty(failure.reason, "cause") &&
  Predicate.isTagged(failure.reason.cause, "RequestMetadataFailure");

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
    return undefined;
  });

export type RequestInput = {
  readonly connection: DurableStreamsConnection;
  readonly method: "HEAD" | "GET" | "PUT" | "POST" | "DELETE";
  readonly readPosition?: { readonly offset: string; readonly cursor?: string };
  readonly longPoll?: boolean;
  readonly sse?: boolean;
  readonly headers?: Readonly<Record<string, string | undefined>>;
  readonly body?: Uint8Array;
  readonly bodyStream?: Stream.Stream<Uint8Array, unknown>;
};

export const buildRequest = (input: RequestInput) => {
  const url = new URL(input.connection.url.href);
  for (const [key, value] of Object.entries(input.connection.params ?? {}))
    url.searchParams.set(key, value);
  if (input.readPosition !== undefined) {
    url.searchParams.set("offset", input.readPosition.offset);
    if (input.readPosition.cursor !== undefined)
      url.searchParams.set("cursor", input.readPosition.cursor);
  }
  if (input.longPoll) url.searchParams.set("live", "long-poll");
  if (input.sse) url.searchParams.set("live", "sse");
  url.searchParams.sort();
  const headers = {
    ...input.connection.headers,
    ...Record.filter(input.headers ?? {}, Predicate.isNotUndefined),
  };
  const request = HttpClientRequest.make(input.method)(url.href).pipe(
    HttpClientRequest.setHeaders(headers),
  );
  if (input.bodyStream !== undefined)
    return request.pipe(
      HttpClientRequest.bodyStream(input.bodyStream, { contentType: headers["content-type"] }),
    );
  return input.body === undefined
    ? request
    : request.pipe(HttpClientRequest.bodyUint8Array(input.body, headers["content-type"]));
};
