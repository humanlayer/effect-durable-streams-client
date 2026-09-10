import { describe, expect, expectTypeOf, it } from "@effect/vitest";
import { Effect, Queue } from "effect";
import type { HttpClient } from "effect/unstable/http";
import {
  DurableStreamsClient,
  type AppendError,
  type AppendConflictError,
  type AppendOutcomeUnknownError,
  type CloseError,
  type CreateError,
  type CreateConflictError,
  type DeleteError,
  type StreamNotFoundError,
  type StreamUnavailableError,
  type HeadError,
  type UnauthorizedError,
} from "../src/index.js";
import { makeScriptedHttpClient, ScriptedResponse } from "./support/http-client.js";

describe("application recovery through the public root API", () => {
  it.effect("narrows only handled tags and preserves ambient HTTP requirements", () =>
    Effect.gen(function* () {
      const http = yield* makeScriptedHttpClient;
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/orders",
        contentType: "text/plain",
        backoffOptions: { maxRetries: 0 },
      });
      const create = client.create({}).pipe(
        Effect.catchTags({
          CreateConflictError: (error) => {
            expectTypeOf<typeof error>().toEqualTypeOf<CreateConflictError>();
            return Effect.succeed("choose another stream");
          },
        }),
      );
      expectTypeOf<Effect.Error<typeof create>>().toEqualTypeOf<
        Exclude<CreateError, CreateConflictError>
      >();
      yield* http.respond(ScriptedResponse.Response({ status: 409, headers: {} }));
      expect(yield* create.pipe(Effect.provide(http.layer))).toBe("choose another stream");
      expect((yield* Queue.take(http.requests)).method).toBe("PUT");

      const append = client.append({ value: "event" }).pipe(
        Effect.catchTags({
          AppendConflictError: (error) => {
            expectTypeOf<typeof error>().toEqualTypeOf<AppendConflictError>();
            return Effect.succeed("correct conflicting append");
          },
          AppendOutcomeUnknownError: (error) => {
            expectTypeOf<typeof error>().toEqualTypeOf<AppendOutcomeUnknownError>();
            return Effect.succeed("reconcile before retrying");
          },
        }),
      );
      expectTypeOf<Effect.Error<typeof append>>().toEqualTypeOf<
        Exclude<AppendError, AppendConflictError | AppendOutcomeUnknownError>
      >();
      expectTypeOf<Effect.Services<typeof append>>().toEqualTypeOf<HttpClient.HttpClient>();
      const providedAppend = append.pipe(Effect.provide(http.layer));
      expectTypeOf<Effect.Services<typeof providedAppend>>().toEqualTypeOf<never>();
      for (const [response, decision] of [
        [ScriptedResponse.Response({ status: 409, headers: {} }), "correct conflicting append"],
        [ScriptedResponse.TransportFailure(), "reconcile before retrying"],
        [ScriptedResponse.Response({ status: 503, headers: {} }), "reconcile before retrying"],
      ] as const) {
        yield* http.respond(response);
        expect(yield* providedAppend).toBe(decision);
        expect((yield* Queue.take(http.requests)).method).toBe("POST");
        expect(yield* Queue.size(http.requests)).toBe(0);
      }

      for (const input of [{}, { value: "final event" }]) {
        const close = client.close(input).pipe(
          Effect.catchTags({
            StreamUnavailableError: () => Effect.succeed("retry idempotent close later"),
            AppendOutcomeUnknownError: () => Effect.succeed("reconcile final append first"),
          }),
        );
        expectTypeOf<Effect.Error<typeof close>>().toEqualTypeOf<
          Exclude<CloseError, StreamUnavailableError | AppendOutcomeUnknownError>
        >();
        yield* http.respond(ScriptedResponse.TransportFailure());
        expect(yield* close.pipe(Effect.provide(http.layer))).toBe(
          input.value === undefined
            ? "retry idempotent close later"
            : "reconcile final append first",
        );
        expect((yield* Queue.take(http.requests)).method).toBe("POST");
        expect(yield* Queue.size(http.requests)).toBe(0);
      }

      const remove = client.delete.pipe(
        Effect.catchTags({ StreamNotFoundError: () => Effect.succeed("already absent") }),
      );
      expectTypeOf<Effect.Error<typeof remove>>().toEqualTypeOf<
        Exclude<DeleteError, StreamNotFoundError>
      >();
      yield* http.respond(ScriptedResponse.Response({ status: 404, headers: {} }));
      expect(yield* remove.pipe(Effect.provide(http.layer))).toBe("already absent");
      expect((yield* Queue.take(http.requests)).method).toBe("DELETE");

      const head = client.head.pipe(
        Effect.catchTags({ UnauthorizedError: () => Effect.succeed("request sign-in") }),
      );
      expectTypeOf<Effect.Error<typeof head>>().toEqualTypeOf<
        Exclude<HeadError, UnauthorizedError>
      >();
      yield* http.respond(ScriptedResponse.Response({ status: 401, headers: {} }));
      expect(yield* head.pipe(Effect.provide(http.layer))).toBe("request sign-in");
      expect((yield* Queue.take(http.requests)).method).toBe("HEAD");
      expect(yield* Queue.size(http.requests)).toBe(0);
    }),
  );
});
