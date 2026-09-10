import { NodeServices } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Fiber, Layer, Schema, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { FetchHttpClient } from "effect/unstable/http";
import { acquireDurableStreamServer } from "../support/server.ts";
import { AdapterState } from "./adapter-state.ts";
import { AdapterCommand, processLine } from "./adapter.ts";

export type AdapterInvocation = {
  readonly commands: ReadonlyArray<AdapterCommand>;
  readonly closeInput: boolean;
};

const _invokeAdapter = (input: AdapterInvocation) =>
  Effect.gen(function* () {
    const encoded = yield* Effect.forEach(input.commands, (command) =>
      Schema.encodeEffect(Schema.fromJsonString(AdapterCommand))(command),
    );
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const child = yield* spawner.spawn(
      ChildProcess.make("./tests/conformance/run-adapter.sh", [], {
        stdin: {
          stream: Stream.make(new TextEncoder().encode(encoded.join("\n") + "\n")),
          endOnDone: input.closeInput,
        },
      }),
    );
    return yield* Effect.all(
      {
        code: child.exitCode,
        stdout: child.stdout.pipe(
          Stream.decodeText(),
          Stream.runFold(
            () => "",
            (a, b) => a + b,
          ),
        ),
        stderr: child.stderr.pipe(
          Stream.decodeText(),
          Stream.runFold(
            () => "",
            (a, b) => a + b,
          ),
        ),
      },
      { concurrency: 3 },
    );
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer));

describe("Phase 1 conformance adapter", () => {
  it.effect("terminates the adapter when its owning fiber is interrupted", () =>
    Effect.gen(function* () {
      const ready = yield* Deferred.make<ChildProcessSpawner.ChildProcessHandle>();
      const owner = yield* Effect.gen(function* () {
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const child = yield* spawner.spawn(
          ChildProcess.make("./tests/conformance/run-adapter.sh", [], {
            stdin: {
              stream: Stream.make(
                new TextEncoder().encode('{"type":"init","serverUrl":"http://localhost:1"}\n'),
              ),
              endOnDone: false,
            },
          }),
        );
        yield* child.stdout.pipe(
          Stream.decodeText(),
          Stream.splitLines,
          Stream.take(1),
          Stream.runDrain,
        );
        yield* Deferred.succeed(ready, child);
        return yield* Effect.never;
      }).pipe(Effect.scoped, Effect.forkChild);
      const child = yield* Deferred.await(ready);
      expect(yield* child.isRunning).toBe(true);
      yield* Fiber.interrupt(owner);
      expect(yield* child.isRunning).toBe(false);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "parses and serializes commands, rejects malformed input, and stays uninitialized until init",
    () =>
      Effect.gen(function* () {
        const before = yield* processLine('{"type":"head","path":"/missing"}');
        const invalid = yield* processLine("not JSON");
        const unsupported = yield* processLine('{"type":"create","path":"/not-implemented"}');
        for (const line of [before, invalid, unsupported]) {
          const result = yield* Schema.decodeEffect(
            Schema.fromJsonString(
              Schema.Struct({
                type: Schema.Literal("error"),
                success: Schema.Literal(false),
                commandType: Schema.String,
              }),
            ),
          )(line);
          expect(result.success).toBe(false);
        }
        expect(unsupported).toContain('"commandType":"create"');
      }).pipe(
        Effect.provide(
          Layer.merge(FetchHttpClient.layer, Layer.effect(AdapterState, AdapterState.make)),
        ),
      ),
  );

  it.effect("emits exactly one result per command and shuts down without waiting for EOF", () =>
    Effect.gen(function* () {
      const { baseUrl } = yield* acquireDurableStreamServer;
      const result = yield* _invokeAdapter({
        commands: [
          { type: "init", serverUrl: baseUrl },
          { type: "head", path: "/missing" },
          { type: "connect", path: "/missing" },
          { type: "shutdown" },
        ],
        closeInput: false,
      });
      expect(result.code).toBe(0);
      const lines = result.stdout.trim().split("\n");
      expect(lines).toHaveLength(4);
      const results = yield* Effect.forEach(lines, (line) =>
        Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(line),
      );
      expect(results[0]).toMatchObject({
        type: "init",
        success: true,
        features: { batching: true, streaming: true, sse: true, longPoll: true },
      });
      expect(results[1]).toMatchObject({
        type: "error",
        commandType: "head",
        status: 404,
        errorCode: "NOT_FOUND",
      });
      expect(results[2]).toMatchObject({
        type: "error",
        commandType: "connect",
        status: 404,
        errorCode: "NOT_FOUND",
      });
      expect(results[3]).toEqual({ type: "shutdown", success: true });
      expect(result.stderr).toBe("adapter scope closed\n");
    }),
  );

  it.effect("closes its scope on EOF without a shutdown command", () =>
    Effect.gen(function* () {
      const { baseUrl } = yield* acquireDurableStreamServer;
      const result = yield* _invokeAdapter({
        commands: [{ type: "init", serverUrl: baseUrl }],
        closeInput: true,
      });
      expect(result.code).toBe(0);
      expect(result.stdout.trim().split("\n")).toHaveLength(1);
      expect(result.stderr).toBe("adapter scope closed\n");
    }),
  );

  it.effect("keeps transport diagnostics off stdout", () =>
    Effect.gen(function* () {
      const baseUrl = yield* Effect.scoped(
        acquireDurableStreamServer.pipe(Effect.map((fixture) => fixture.baseUrl)),
      );
      const result = yield* _invokeAdapter({
        commands: [
          { type: "init", serverUrl: baseUrl },
          { type: "head", path: "/missing" },
          { type: "shutdown" },
        ],
        closeInput: true,
      });
      expect(result.code).toBe(0);
      const lines = result.stdout.trim().split("\n");
      expect(lines).toHaveLength(3);
      const results = yield* Effect.forEach(lines, (line) =>
        Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(line),
      );
      expect(results[1]).toMatchObject({ type: "error", errorCode: "NETWORK_ERROR" });
      expect(result.stderr).toContain("Stream transport failed");
      expect(result.stderr).toContain("adapter scope closed");
    }),
  );
});
