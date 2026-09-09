import { DurableStreamTestServer } from "@durable-streams/server";
import { Data, Effect } from "effect";

export class TestServerStartError extends Data.TaggedError("TestServerStartError")<{
  readonly cause: unknown;
}> {}
export class TestServerStopError extends Data.TaggedError("TestServerStopError")<{
  readonly cause: unknown;
}> {}

export const acquireDurableStreamServer = Effect.acquireRelease(
  Effect.gen(function* () {
    const server = yield* Effect.try({
      try: () => new DurableStreamTestServer({ port: 0, longPollTimeout: 500 }),
      catch: (cause) => new TestServerStartError({ cause }),
    });
    const baseUrl = yield* Effect.tryPromise({
      try: () => server.start(),
      catch: (cause) => new TestServerStartError({ cause }),
    });
    return { server, baseUrl };
  }),
  ({ server }) =>
    Effect.tryPromise({
      try: () => server.stop(),
      catch: (cause) => new TestServerStopError({ cause }),
    }).pipe(Effect.orDie),
);
