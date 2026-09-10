import { Deferred, Effect, Layer, Logger, Match, Queue, References } from "effect";
import { HttpClient, HttpClientError, HttpClientResponse } from "effect/unstable/http";
import { ScriptedResponse } from "./http-client.ts";

export const producerReply = (input: {
  readonly seq: number;
  readonly epoch?: number;
  readonly duplicate?: boolean;
  readonly closed?: boolean;
}) =>
  ScriptedResponse.Response({
    status: input.duplicate ? 204 : 200,
    headers: {
      "producer-epoch": String(input.epoch ?? 0),
      "producer-seq": String(input.seq),
      "stream-next-offset": `offset${input.seq}`,
      "stream-closed": input.closed ? "true" : "false",
    },
  });
export const makeProducerHttp = Effect.gen(function* () {
  const admitted = yield* Queue.unbounded<void>();
  const requests = yield* Queue.unbounded<{
    readonly url: string;
    readonly body: Uint8Array;
    readonly headers: Readonly<Record<string, string>>;
    readonly signal: AbortSignal;
    readonly reply: Deferred.Deferred<ScriptedResponse>;
  }>();
  const client = HttpClient.make((request, url, signal) =>
    Effect.gen(function* () {
      const reply = yield* Deferred.make<ScriptedResponse>();
      const body = Match.value(request.body).pipe(
        Match.tag("Uint8Array", (body) => body.body),
        Match.orElse(() => new Uint8Array()),
      );
      yield* Queue.offer(requests, {
        url: url.href,
        body,
        headers: request.headers,
        signal,
        reply,
      });
      return yield* ScriptedResponse.$match(yield* Deferred.await(reply), {
        Response: (response) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              new Response(response.body ?? null, {
                status: response.status,
                headers: response.headers,
              }),
            ),
          ),
        TransportFailure: () =>
          Effect.fail(
            new HttpClientError.HttpClientError({
              reason: new HttpClientError.TransportError({
                request,
                description: "private producer failure",
              }),
            }),
          ),
      });
    }),
  );
  const logger = Logger.make(({ message }) => {
    if (
      message === "Producer append admitted" ||
      (Array.isArray(message) && message.includes("Producer append admitted"))
    )
      Queue.offerUnsafe(admitted, undefined);
  });
  return {
    requests,
    admitted,
    layer: Layer.mergeAll(
      Layer.succeed(HttpClient.HttpClient, client),
      Logger.layer([logger]),
      Layer.succeed(References.MinimumLogLevel, "Debug"),
    ),
  };
});
