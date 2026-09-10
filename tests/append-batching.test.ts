import { describe, expect, it } from "@effect/vitest";
import {
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Logger,
  Match,
  Queue,
  Ref,
  References,
  Schema,
  SchemaGetter,
} from "effect";
import { HttpClient, HttpClientRequest } from "effect/unstable/http";
import { TestClock } from "effect/testing";
import { DurableStreamsClient } from "../src/index.ts";
import { makeScriptedHttpClient, ScriptedResponse } from "./support/http-client.ts";

const setup = Effect.gen(function* () {
  const http = yield* makeScriptedHttpClient;
  const admissions = yield* Queue.unbounded<void>();
  const logs = Logger.layer([
    Logger.make(({ message }) => {
      if (
        message === "Ordinary append admitted" ||
        (Array.isArray(message) && message.includes("Ordinary append admitted"))
      )
        Queue.offerUnsafe(admissions, undefined);
    }),
  ]);
  return {
    ...http,
    admissions,
    layer: Layer.mergeAll(http.layer, logs, Layer.succeed(References.MinimumLogLevel, "Debug")),
    accept: (offset: string) =>
      http.respond(
        ScriptedResponse.Response({ status: 204, headers: { "stream-next-offset": offset } }),
      ),
  };
});

const _body = (request: { readonly body: import("effect/unstable/http/HttpBody").HttpBody }) =>
  Match.value(request.body).pipe(
    Match.tag("Uint8Array", (body) => body.body),
    Match.orElse(() => new Uint8Array()),
  );

describe("reference ordinary batching", () => {
  it.effect("keeps the active caller lease until interrupted HTTP cleanup completes", () =>
    Effect.gen(function* () {
      const http = yield* setup;
      const cleaning = yield* Deferred.make<void>();
      const cleaned = yield* Deferred.make<void>();
      const released = yield* Ref.make(false);
      yield* Effect.gen(function* () {
        const base = yield* HttpClient.HttpClient;
        const transport = HttpClient.make((request) =>
          base
            .execute(request)
            .pipe(
              Effect.ensuring(
                Deferred.succeed(cleaning, undefined).pipe(Effect.andThen(Deferred.await(cleaned))),
              ),
            ),
        );
        const client = yield* DurableStreamsClient.make({ url: "https://streams.test/bytes" });
        const active = yield* Effect.gen(function* () {
          yield* Effect.acquireRelease(Effect.void, () => Ref.set(released, true));
          return yield* client
            .append({ value: "active" })
            .pipe(Effect.provideService(HttpClient.HttpClient, transport));
        }).pipe(Effect.scoped, Effect.forkChild);
        yield* Queue.take(http.admissions);
        yield* Queue.take(http.requests);
        const pending = yield* client
          .append({ value: "pending" })
          .pipe(Effect.flip, Effect.forkChild);
        yield* Queue.take(http.admissions);
        const interrupt = yield* Fiber.interrupt(active).pipe(Effect.forkChild);
        yield* Deferred.await(cleaning);
        expect(yield* Ref.get(released)).toBe(false);
        yield* Deferred.succeed(cleaned, undefined);
        yield* Fiber.join(interrupt);
        expect(yield* Ref.get(released)).toBe(true);
        expect(yield* Fiber.join(pending)).toHaveProperty("_tag", "AppendOutcomeUnknownError");
        expect(yield* Ref.get(http.active)).toBe(0);
        expect(yield* Queue.size(http.requests)).toBe(0);
      }).pipe(Effect.provide(http.layer));
    }),
  );

  for (const owners of [
    ["A", "B"],
    ["A", "B", "A"],
  ]) {
    it.effect(`preserves distinct HTTP owners and contiguous order: ${owners.join("/")}`, () =>
      Effect.gen(function* () {
        const http = yield* setup;
        yield* Effect.gen(function* () {
          const base = yield* HttpClient.HttpClient;
          const clients = new Map(
            ["A", "B"].map((owner) => [
              owner,
              base.pipe(HttpClient.mapRequest(HttpClientRequest.setHeader("x-owner", owner))),
            ]),
          );
          const client = yield* DurableStreamsClient.make({ url: "https://streams.test/bytes" });
          const seed = yield* client.append({ value: "seed" }).pipe(Effect.forkChild);
          yield* Queue.take(http.admissions);
          yield* Queue.take(http.requests);
          const fibers = [];
          for (const owner of owners) {
            const transport = clients.get(owner);
            if (transport === undefined) return yield* Effect.die("Missing test transport");
            fibers.push(
              yield* client
                .append({ value: owner })
                .pipe(Effect.provideService(HttpClient.HttpClient, transport), Effect.forkChild),
            );
            yield* Queue.take(http.admissions);
          }
          yield* http.accept("seed");
          yield* Fiber.join(seed);
          for (const owner of owners) {
            const request = yield* Queue.take(http.requests);
            expect(request.headers["x-owner"]).toBe(owner);
            expect(new TextDecoder().decode(_body(request))).toBe(owner);
            expect(yield* Ref.get(http.active)).toBe(1);
            expect(yield* Queue.size(http.requests)).toBe(0);
            yield* http.accept(owner);
          }
          for (const fiber of fibers) yield* Fiber.join(fiber);
          expect(yield* Ref.get(http.active)).toBe(0);
          return undefined;
        }).pipe(Effect.provide(http.layer));
      }),
    );
  }

  it.effect("same HTTP object honors dynamic context auth and coalesces equal bindings", () =>
    Effect.gen(function* () {
      const Auth = Context.Reference<string>("test/AppendAuth", {
        defaultValue: () => "seed",
      });
      const http = yield* setup;
      yield* Effect.gen(function* () {
        const base = yield* HttpClient.HttpClient;
        const authorized = base.pipe(
          HttpClient.mapRequestEffect((request) =>
            Effect.map(Auth, (auth) => HttpClientRequest.setHeader(request, "authorization", auth)),
          ),
        );
        yield* Effect.gen(function* () {
          const client = yield* DurableStreamsClient.make({ url: "https://streams.test/bytes" });
          const seed = yield* client
            .append({ value: "seed" })
            .pipe(Effect.provideService(Auth, "initial"), Effect.forkChild);
          yield* Queue.take(http.admissions);
          yield* Queue.take(http.requests);
          const fibers = [];
          for (const auth of ["A", "A", "B", "seed", "A"]) {
            const append = client.append({ value: auth });
            fibers.push(
              yield* (
                auth === "seed" ? append : append.pipe(Effect.provideService(Auth, auth))
              ).pipe(Effect.forkChild),
            );
            yield* Queue.take(http.admissions);
          }
          yield* http.accept("seed");
          yield* Fiber.join(seed);
          for (const [auth, body] of [
            ["A", "AA"],
            ["B", "B"],
            ["seed", "seed"],
            ["A", "A"],
          ]) {
            const request = yield* Queue.take(http.requests);
            expect(request.headers.authorization).toBe(auth);
            expect(new TextDecoder().decode(_body(request))).toBe(body);
            expect(yield* Ref.get(http.active)).toBe(1);
            expect(yield* Queue.size(http.requests)).toBe(0);
            yield* http.accept(body ?? "missing");
          }
          for (const fiber of fibers) yield* Fiber.join(fiber);
        }).pipe(Effect.provideService(HttpClient.HttpClient, authorized));
      }).pipe(Effect.provide(http.layer));
    }),
  );

  it.effect(
    "a cancelled buffered entry waits for promotion and never dispatches its cancelled batch",
    () =>
      Effect.gen(function* () {
        const http = yield* setup;
        yield* Effect.gen(function* () {
          const client = yield* DurableStreamsClient.make({ url: "https://streams.test/bytes" });
          const a = yield* client.append({ value: "a" }).pipe(Effect.forkChild);
          yield* Queue.take(http.admissions);
          yield* Queue.take(http.requests);
          const released = yield* Ref.make(false);
          const b = yield* Effect.gen(function* () {
            yield* Effect.acquireRelease(Effect.void, () => Ref.set(released, true));
            return yield* client.append({ value: "b" });
          }).pipe(Effect.scoped, Effect.forkChild);
          yield* Queue.take(http.admissions);
          const c = yield* client.append({ value: "c" }).pipe(Effect.flip, Effect.forkChild);
          yield* Queue.take(http.admissions);
          yield* Fiber.interrupt(b);
          expect(yield* Ref.get(released)).toBe(true);
          expect(yield* Ref.get(http.active)).toBe(1);
          yield* http.accept("first");
          expect(yield* Fiber.join(a)).toHaveProperty("offset", "first");
          expect(yield* Fiber.join(c)).toHaveProperty("_tag", "AppendOutcomeUnknownError");
          expect(yield* Queue.size(http.requests)).toBe(0);
          const last = yield* client.append({ value: "last" }).pipe(Effect.forkChild);
          yield* Queue.take(http.admissions);
          yield* Queue.take(http.requests);
          yield* Fiber.interrupt(last);
          expect(yield* Ref.get(http.active)).toBe(0);
          yield* http.accept("new");
          expect(yield* client.append({ value: "new" })).toHaveProperty("offset", "new");
        }).pipe(Effect.provide(http.layer));
      }),
  );
  it.effect(
    "starts immediately, promotes only overlap, selects last defined seq and shares receipts",
    () =>
      Effect.gen(function* () {
        const http = yield* setup;
        yield* Effect.gen(function* () {
          const client = yield* DurableStreamsClient.make({
            url: "https://streams.test/json",
            contentType: "application/json",
          });
          const a = yield* client.append({ value: [1, 2], seq: "01" }).pipe(Effect.forkChild);
          yield* Queue.take(http.admissions);
          expect(new TextDecoder().decode(_body(yield* Queue.take(http.requests)))).toBe("[[1,2]]");
          const b = yield* client.append({ value: "b", seq: "02" }).pipe(Effect.forkChild);
          yield* Queue.take(http.admissions);
          const c = yield* client.append({ value: { c: 3 }, seq: "04" }).pipe(Effect.forkChild);
          yield* Queue.take(http.admissions);
          const d = yield* client.append({ value: null }).pipe(Effect.forkChild);
          yield* Queue.take(http.admissions);
          expect(yield* Queue.size(http.requests)).toBe(0);
          expect(yield* Ref.get(http.active)).toBe(1);
          yield* http.accept("first");
          expect(yield* Fiber.join(a)).toEqual({ offset: "first", closed: false });
          const next = yield* Queue.take(http.requests);
          expect(new TextDecoder().decode(_body(next))).toBe('["b",{"c":3},null]');
          expect(next.headers["stream-seq"]).toBe("04");
          const e = yield* client.append({ value: "later" }).pipe(Effect.forkChild);
          yield* Queue.take(http.admissions);
          yield* http.respond(
            ScriptedResponse.Response({ status: 503, headers: { "retry-after": "2" } }),
          );
          yield* TestClock.adjust("2 seconds");
          const retry = yield* Queue.take(http.requests);
          expect(_body(retry)).toEqual(_body(next));
          expect(retry.headers["stream-seq"]).toBe("04");
          expect(yield* Ref.get(http.active)).toBe(1);
          expect(yield* Queue.size(http.requests)).toBe(0);
          yield* http.accept("second");
          for (const fiber of [b, c, d])
            expect(yield* Fiber.join(fiber)).toEqual({ offset: "second", closed: false });
          expect(new TextDecoder().decode(_body(yield* Queue.take(http.requests)))).toBe(
            '["later"]',
          );
          yield* http.accept("third");
          yield* Fiber.join(e);
          expect(yield* Ref.get(http.active)).toBe(0);
        }).pipe(Effect.provide(http.layer));
      }),
  );

  it.effect(
    "concatenates mixed UTF-8 and binary without delimiters; sequential awaits stay separate",
    () =>
      Effect.gen(function* () {
        const http = yield* setup;
        yield* Effect.gen(function* () {
          const client = yield* DurableStreamsClient.make({ url: "https://streams.test/bytes" });
          const a = yield* client.append({ value: "first" }).pipe(Effect.forkChild);
          yield* Queue.take(http.admissions);
          yield* Queue.take(http.requests);
          const b = yield* client.append({ value: "hé" }).pipe(Effect.forkChild);
          yield* Queue.take(http.admissions);
          const c = yield* client
            .append({ value: new Uint8Array([0, 255]) })
            .pipe(Effect.forkChild);
          yield* Queue.take(http.admissions);
          yield* http.accept("a");
          yield* Fiber.join(a);
          expect(_body(yield* Queue.take(http.requests))).toEqual(
            new Uint8Array([104, 195, 169, 0, 255]),
          );
          yield* http.accept("bc");
          yield* Fiber.join(b);
          yield* Fiber.join(c);
          for (const value of ["one", "two"]) {
            yield* http.accept(value);
            yield* client.append({ value });
            expect(new TextDecoder().decode(_body(yield* Queue.take(http.requests)))).toBe(value);
          }
        }).pipe(Effect.provide(http.layer));
      }),
  );

  it.effect("admits schema values in encoding-completion order, not invocation order", () =>
    Effect.gen(function* () {
      const http = yield* setup;
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const codec = Schema.String.pipe(
        Schema.decodeTo(Schema.String, {
          decode: SchemaGetter.passthrough(),
          encode: SchemaGetter.transformOrFail((value) =>
            value === "slow"
              ? Deferred.succeed(entered, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.as(value),
                )
              : Effect.succeed(value),
          ),
        }),
      );
      yield* Effect.gen(function* () {
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/json",
          schema: codec,
        });
        const first = yield* client.append({ value: "first" }).pipe(Effect.forkChild);
        yield* Queue.take(http.admissions);
        yield* Queue.take(http.requests);
        const slow = yield* client.append({ value: "slow" }).pipe(Effect.forkChild);
        yield* Deferred.await(entered);
        const fast = yield* client.append({ value: "fast" }).pipe(Effect.forkChild);
        yield* Queue.take(http.admissions);
        yield* Deferred.succeed(release, undefined);
        yield* Queue.take(http.admissions);
        yield* http.accept("first");
        yield* Fiber.join(first);
        expect(new TextDecoder().decode(_body(yield* Queue.take(http.requests)))).toBe(
          '["fast","slow"]',
        );
        yield* http.accept("rest");
        yield* Fiber.join(slow);
        yield* Fiber.join(fast);
      }).pipe(Effect.provide(http.layer));
    }),
  );

  it.effect("fails active and buffered callers with the identical rejection, then recovers", () =>
    Effect.gen(function* () {
      const http = yield* setup;
      yield* Effect.gen(function* () {
        const client = yield* DurableStreamsClient.make({ url: "https://streams.test/bytes" });
        const a = yield* client.append({ value: "a" }).pipe(Effect.flip, Effect.forkChild);
        yield* Queue.take(http.admissions);
        yield* Queue.take(http.requests);
        const b = yield* client.append({ value: "b" }).pipe(Effect.flip, Effect.forkChild);
        yield* Queue.take(http.admissions);
        yield* http.respond(ScriptedResponse.Response({ status: 409, headers: {} }));
        const error = yield* Fiber.join(a);
        expect(error).toHaveProperty("_tag", "AppendConflictError");
        expect(yield* Fiber.join(b)).toBe(error);
        expect(yield* Queue.size(http.requests)).toBe(0);
        yield* http.accept("recovered");
        expect(yield* client.append({ value: "c" })).toHaveProperty("offset", "recovered");
        yield* Queue.take(http.requests);
        expect(yield* Ref.get(http.active)).toBe(0);
      }).pipe(Effect.provide(http.layer));
    }),
  );

  it.effect("exhausted retries fail the active and buffered callers once, then recover", () =>
    Effect.gen(function* () {
      const http = yield* setup;
      yield* Effect.gen(function* () {
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/bytes",
          backoffOptions: { maxRetries: 1 },
        });
        const a = yield* client.append({ value: "a" }).pipe(Effect.flip, Effect.forkChild);
        yield* Queue.take(http.admissions);
        const first = yield* Queue.take(http.requests);
        const b = yield* client.append({ value: "b" }).pipe(Effect.flip, Effect.forkChild);
        yield* Queue.take(http.admissions);
        yield* http.respond(ScriptedResponse.TransportFailure());
        yield* TestClock.adjust("100 millis");
        expect(_body(yield* Queue.take(http.requests))).toEqual(_body(first));
        yield* http.respond(ScriptedResponse.Response({ status: 503, headers: {} }));
        const error = yield* Fiber.join(a);
        expect(error).toHaveProperty("_tag", "AppendOutcomeUnknownError");
        expect(yield* Fiber.join(b)).toBe(error);
        expect(yield* Queue.size(http.requests)).toBe(0);
        yield* http.accept("recovered");
        expect(yield* client.append({ value: "c" })).toHaveProperty("offset", "recovered");
        yield* Queue.take(http.requests);
        expect(yield* Ref.get(http.active)).toBe(0);
      }).pipe(Effect.provide(http.layer));
    }),
  );

  for (const duringBackoff of [false, true]) {
    it.effect(
      `interrupts shared work from any participant and fails peers and buffer, backoff=${duringBackoff}`,
      () =>
        Effect.gen(function* () {
          const http = yield* setup;
          yield* Effect.gen(function* () {
            const client = yield* DurableStreamsClient.make({ url: "https://streams.test/bytes" });
            const a = yield* client.append({ value: "a" }).pipe(Effect.forkChild);
            yield* Queue.take(http.admissions);
            yield* Queue.take(http.requests);
            const b = yield* client.append({ value: "b" }).pipe(Effect.forkChild);
            yield* Queue.take(http.admissions);
            const c = yield* client.append({ value: "c" }).pipe(Effect.flip, Effect.forkChild);
            yield* Queue.take(http.admissions);
            yield* http.accept("a");
            yield* Fiber.join(a);
            yield* Queue.take(http.requests);
            const d = yield* client.append({ value: "d" }).pipe(Effect.flip, Effect.forkChild);
            yield* Queue.take(http.admissions);
            if (duringBackoff) {
              yield* http.respond(
                ScriptedResponse.Response({ status: 503, headers: { "retry-after": "60" } }),
              );
              yield* TestClock.adjust("1 millis");
            }
            yield* Fiber.interrupt(b);
            expect(Exit.hasInterrupts(yield* Fiber.await(b))).toBe(true);
            const error = yield* Fiber.join(c);
            expect(error).toHaveProperty("_tag", "AppendOutcomeUnknownError");
            expect(yield* Fiber.join(d)).toBe(error);
            expect(yield* Ref.get(http.active)).toBe(0);
            yield* TestClock.adjust("1 hour");
            expect(yield* Queue.size(http.requests)).toBe(0);
            yield* http.accept("recovered");
            yield* client.append({ value: "new" });
          }).pipe(Effect.provide(http.layer));
        }),
    );
  }

  it.effect("disabled batching permits independent in-flight requests", () =>
    Effect.gen(function* () {
      const http = yield* setup;
      yield* Effect.gen(function* () {
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/bytes",
          batching: false,
        });
        const a = yield* client.append({ value: "a" }).pipe(Effect.forkChild);
        expect(new TextDecoder().decode(_body(yield* Queue.take(http.requests)))).toBe("a");
        const b = yield* client.append({ value: "b" }).pipe(Effect.forkChild);
        expect(new TextDecoder().decode(_body(yield* Queue.take(http.requests)))).toBe("b");
        expect(yield* Ref.get(http.active)).toBe(2);
        yield* http.accept("a");
        yield* http.accept("b");
        yield* Fiber.join(a);
        yield* Fiber.join(b);
      }).pipe(Effect.provide(http.layer));
    }),
  );
});
