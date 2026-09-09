import { DurableStreamTestServer } from "@durable-streams/server";
import { Data, Effect } from "effect";

export class TestServerStartError extends Data.TaggedError("TestServerStartError")<{
  readonly cause: unknown;
}> {}
export class TestServerStopError extends Data.TaggedError("TestServerStopError")<{
  readonly cause: unknown;
}> {}

export const acquireDurableStreamServer = Effect.acquireRelease(
  Effect.tryPromise({
    try: async () => {
      const server = new DurableStreamTestServer({ port: 0, longPollTimeout: 500 });
      const baseUrl = await server.start();
      return { server, baseUrl };
    },
    catch: (cause) => new TestServerStartError({ cause }),
  }),
  ({ server }) =>
    Effect.tryPromise({
      try: () => server.stop(),
      catch: (cause) => new TestServerStopError({ cause }),
    }).pipe(Effect.orDie),
);
