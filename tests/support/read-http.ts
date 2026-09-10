import { Effect, Layer, Predicate, Queue, Record, Stream } from "effect";
import {
  HttpClient,
  type HttpClientError,
  type HttpClientRequest,
  HttpClientResponse,
} from "effect/unstable/http";

export type ReadReply = {
  readonly status: number;
  readonly headers: Readonly<Record<string, string>>;
  readonly body: Stream.Stream<Uint8Array, HttpClientError.HttpClientError>;
};

export const makeReadHttp = Effect.gen(function* () {
  const replies = yield* Queue.unbounded<ReadReply>();
  const requests = yield* Queue.unbounded<{
    readonly request: HttpClientRequest.HttpClientRequest;
    readonly signal: AbortSignal;
  }>();
  const client = HttpClient.make((request, _url, signal) =>
    Effect.gen(function* () {
      yield* Queue.offer(requests, { request, signal });
      const reply = yield* Queue.take(replies);
      const response = HttpClientResponse.fromWeb(
        request,
        new Response(null, { status: reply.status, headers: reply.headers }),
      );
      Object.defineProperty(response, "stream", { configurable: true, get: () => reply.body });
      return response;
    }),
  );
  return { replies, requests, layer: Layer.succeed(HttpClient.HttpClient, client) };
});

export const readReply = (input: {
  readonly offset: string;
  readonly text: string;
  readonly contentType?: string;
  readonly upToDate?: boolean;
  readonly closed?: boolean;
}) =>
  ({
    status: 200,
    headers: {
      "content-type": input.contentType ?? "text/plain",
      "stream-next-offset": input.offset,
      ...Record.filter(
        {
          "stream-up-to-date": input.upToDate === true ? "true" : undefined,
          "stream-closed": input.closed === true ? "true" : undefined,
        },
        Predicate.isNotUndefined,
      ),
    },
    body:
      input.text.length === 0 ? Stream.empty : Stream.make(new TextEncoder().encode(input.text)),
  }) satisfies ReadReply;
