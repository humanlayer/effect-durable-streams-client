import { Data, Effect, Layer, Queue, Ref } from "effect";
import {
  HttpClient,
  HttpClientError,
  type HttpClientRequest,
  HttpClientResponse,
} from "effect/unstable/http";

export type ScriptedResponse = Data.TaggedEnum<{
  Response: {
    readonly status: number;
    readonly headers: Readonly<Record<string, string>>;
    readonly body?: string;
  };
  TransportFailure: {};
}>;
export const ScriptedResponse = Data.taggedEnum<ScriptedResponse>();

export const makeScriptedHttpClient = Effect.gen(function* () {
  const requests = yield* Queue.unbounded<HttpClientRequest.HttpClientRequest>();
  const responses = yield* Queue.unbounded<ScriptedResponse>();
  const active = yield* Ref.make(0);
  const client = HttpClient.make((request) =>
    Effect.gen(function* () {
      yield* Ref.update(active, (n) => n + 1);
      yield* Queue.offer(requests, request);
      const response = yield* Queue.take(responses);
      return yield* ScriptedResponse.$match(response, {
        Response: (reply) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              new Response(reply.body ?? null, {
                status: reply.status,
                headers: reply.headers,
              }),
            ),
          ),
        TransportFailure: () =>
          Effect.fail(
            new HttpClientError.HttpClientError({
              reason: new HttpClientError.TransportError({
                request,
                description: "scripted private failure",
              }),
            }),
          ),
      });
    }).pipe(Effect.ensuring(Ref.update(active, (n) => n - 1))),
  );
  return {
    requests,
    active,
    respond: (response: ScriptedResponse) => Queue.offer(responses, response),
    layer: Layer.succeed(HttpClient.HttpClient, client),
  };
});
