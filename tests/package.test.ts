import { NodeServices } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { acquireDurableStreamServer } from "./support/server.js";

describe("published package", () => {
  it.effect(
    "checks release tags and conformance success/failure cleanup",
    () =>
      Effect.gen(function* () {
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const child = yield* spawner.spawn(ChildProcess.make("bash", ["scripts/test-release.sh"]));
        const result = yield* Effect.all(
          {
            code: child.exitCode,
            stdout: child.stdout.pipe(Stream.decodeText(), Stream.runCollect),
            stderr: child.stderr.pipe(Stream.decodeText(), Stream.runCollect),
          },
          { concurrency: 3 },
        );
        expect(result.code, [...result.stdout, ...result.stderr].join("")).toBe(0);
        expect(result.stdout.join("")).toContain("Release validation passed");
      }).pipe(Effect.provide(NodeServices.layer)),
    { timeout: 30000 },
  );

  it.effect(
    "consumes the tarball as ESM and typechecks README and public contracts",
    () =>
      Effect.gen(function* () {
        const { baseUrl } = yield* acquireDurableStreamServer;
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const child = yield* spawner.spawn(
          ChildProcess.make("bash", ["scripts/test-package.sh", baseUrl]),
        );
        const result = yield* Effect.all(
          {
            code: child.exitCode,
            stdout: child.stdout.pipe(Stream.decodeText(), Stream.runCollect),
            stderr: child.stderr.pipe(Stream.decodeText(), Stream.runCollect),
          },
          { concurrency: 3 },
        );
        expect(result.code, [...result.stdout, ...result.stderr].join("")).toBe(0);
        expect(result.stdout.join("")).toContain("Package consumption passed");
      }).pipe(Effect.provide(NodeServices.layer)),
    { timeout: 120000 },
  );
});
