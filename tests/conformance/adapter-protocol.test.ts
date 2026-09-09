import { spawn } from "node:child_process";
import { describe, expect, it } from "@effect/vitest";
import { Data, Effect, Layer, Schema } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import { acquireDurableStreamServer } from "../support/server.ts";
import { AdapterState } from "./adapter-state.ts";
import { AdapterCommand, processLine } from "./adapter.ts";

export class AdapterProcessError extends Data.TaggedError("AdapterProcessError")<{
  readonly cause: unknown;
}> {}

export type AdapterInvocation = {
  readonly commands: ReadonlyArray<AdapterCommand>;
  readonly closeInput: boolean;
};

const _invokeAdapter = (input: AdapterInvocation) =>
  Effect.gen(function* () {
    const encoded = yield* Effect.forEach(input.commands, (command) =>
      Schema.encodeEffect(Schema.fromJsonString(AdapterCommand))(command),
    );
    return yield* Effect.callback<
      { readonly code: number | null; readonly stdout: string; readonly stderr: string },
      AdapterProcessError
    >((resume) => {
      const process = spawn("./tests/conformance/run-adapter.sh", [], { stdio: "pipe" });
      let stdout = "";
      let stderr = "";
      process.stdout.setEncoding("utf8");
      process.stderr.setEncoding("utf8");
      process.stdout.on("data", (chunk: string) => {
        stdout += chunk;
      });
      process.stderr.on("data", (chunk: string) => {
        stderr += chunk;
      });
      process.on("error", (cause) => resume(Effect.fail(new AdapterProcessError({ cause }))));
      process.stdin.on("error", (cause) => resume(Effect.fail(new AdapterProcessError({ cause }))));
      process.on("close", (code) => resume(Effect.succeed({ code, stdout, stderr })));
      process.stdin.write(encoded.join("\n") + "\n");
      if (input.closeInput) process.stdin.end();
      return Effect.sync(() => {
        process.kill();
      });
    });
  });

describe("Phase 1 conformance adapter", () => {
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
        features: { batching: false, streaming: false, sse: false, longPoll: false },
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
