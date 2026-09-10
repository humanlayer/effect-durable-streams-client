import { describe, expect, it } from "@effect/vitest";
import { Context, Deferred, Effect, Exit, Fiber, Schema, Scope } from "effect";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";
import { DurableStream, IdempotentProducer, makeEffectClient } from "../src/async-await.js";

const _gate = <A>() => {
  const deferred = Deferred.makeUnsafe<A>();
  return {
    promise: Effect.runPromise(Deferred.await(deferred)),
    resolve: (value: A) => Deferred.doneUnsafe(deferred, Exit.succeed(value)),
  };
};
const _headers = {
  "content-type": "application/json",
  "stream-next-offset": "tail",
  "stream-up-to-date": "true",
};
const _requestCleanup = (cleanup: Effect.Effect<void>) =>
  Effect.withFiber((fiber) =>
    Scope.addFinalizer(Context.getUnsafe(fiber.context, Scope.Scope), cleanup),
  );
const _handle = (fetch: typeof globalThis.fetch) =>
  new DurableStream({ url: "https://example.test/a", fetch });
const _accepted = (init: RequestInit | undefined) =>
  new Response(null, {
    status: 200,
    headers: {
      "producer-epoch": new Headers(init?.headers).get("producer-epoch") ?? "0",
      "producer-seq": new Headers(init?.headers).get("producer-seq") ?? "0",
      "stream-next-offset": "tail",
      "stream-closed": new Headers(init?.headers).get("stream-closed") ?? "false",
    },
  });

describe("confirmed async review regressions", () => {
  for (const autoClaim of [false, true]) {
    it(`synchronous diagnostics reflect threshold emission and autoClaim=${autoClaim}`, async () => {
      const release = _gate<void>();
      const producer = new IdempotentProducer(
        _handle(async (_url, init) => {
          await release.promise;
          return _accepted(init);
        }),
        "writer",
        { maxBatchBytes: 2, autoClaim },
      );
      producer.append("x");
      expect([producer.pendingCount, producer.inFlightCount, producer.nextSeq]).toEqual([1, 0, 0]);
      producer.append("y");
      expect([producer.pendingCount, producer.inFlightCount, producer.nextSeq]).toEqual([0, 1, 1]);
      producer.append("zz");
      expect([producer.pendingCount, producer.inFlightCount, producer.nextSeq]).toEqual([
        0,
        2,
        autoClaim ? 1 : 2,
      ]);
      release.resolve();
      await producer.detach();
    });
  }
  it("rejects buffered JSON after cancellation without advancing its checkpoint", async () => {
    const response = await _handle(async () => new Response("[1,2]", { headers: _headers })).stream(
      { live: false },
    );
    const reader = response.jsonStream().getReader();
    expect((await reader.read()).value).toBe(1);
    response.cancel();
    await response.closed;
    await expect(reader.read()).rejects.toMatchObject({ code: "ABORTED" });
    expect(response.offset).toBe("-1");
    reader.releaseLock();
  });

  it("acknowledges the exact final JSON pull and closes without an extra read", async () => {
    const response = await _handle(async () => new Response("[1,2]", { headers: _headers })).stream(
      { live: false },
    );
    const reader = response.jsonStream().getReader();
    expect((await reader.read()).value).toBe(1);
    expect(response.offset).toBe("-1");
    expect((await reader.read()).value).toBe(2);
    await response.closed;
    expect(response.offset).toBe("tail");
    reader.releaseLock();
  });

  it("unread Web cancellation joins gated body cleanup", async () => {
    const entered = _gate<void>();
    const release = _gate<void>();
    const response = await _handle(
      async () =>
        new Response(
          new ReadableStream(
            {
              cancel: async () => {
                entered.resolve();
                await release.promise;
              },
            },
            { highWaterMark: 0 },
          ),
          { headers: _headers },
        ),
    ).stream();
    const state = { settled: false };
    const cancelled = response
      .bodyStream()
      .cancel()
      .then(() => {
        state.settled = true;
      });
    await entered.promise;
    expect(state.settled).toBe(false);
    release.resolve();
    await cancelled;
    await response.closed;
    expect(state.settled).toBe(true);
    expect(response.offset).toBe("-1");
  });

  for (const view of ["bodyStream", "textStream"] as const) {
    it(`${view} delivers before HTTP EOF without an early checkpoint`, async () => {
      const rest = _gate<void>();
      const state = { pulls: 0 };
      const response = await _handle(
        async () =>
          new Response(
            new ReadableStream(
              {
                async pull(target) {
                  if (state.pulls++ === 0) target.enqueue(new TextEncoder().encode("first"));
                  else {
                    await rest.promise;
                    target.enqueue(new TextEncoder().encode("last"));
                    target.close();
                  }
                },
              },
              { highWaterMark: 0 },
            ),
            { headers: { ..._headers, "content-type": "text/plain" } },
          ),
      ).stream({ live: false });
      const reader = response[view]().getReader();
      const first = await reader.read();
      expect(first.value).toEqual(
        view === "textStream" ? "first" : new TextEncoder().encode("first"),
      );
      expect(response.offset).toBe("-1");
      rest.resolve();
      expect((await reader.read()).done).toBe(false);
      await response.closed;
      expect(response.offset).toBe("tail");
      reader.releaseLock();
    });
  }

  it("collects a 150000-message JSON envelope without argument spread overflow", async () => {
    const body = `[${Array.from({ length: 150000 }, () => "1").join(",")}]`;
    const response = await _handle(async () => new Response(body, { headers: _headers })).stream();
    const items = await response.json();
    expect(items).toHaveLength(150000);
    expect(items[149999]).toBe(1);
    expect(response.offset).toBe("tail");
  });

  it("collects 150000 fragmented byte chunks without argument spread overflow", async () => {
    const state = { index: 0 };
    const response = await _handle(
      async () =>
        new Response(
          new ReadableStream({
            pull(target) {
              if (state.index++ < 150000) target.enqueue(new Uint8Array([120]));
              else target.close();
            },
          }),
          { headers: { ..._headers, "content-type": "application/octet-stream" } },
        ),
    ).stream();
    expect(await response.body()).toHaveLength(150000);
    expect(response.offset).toBe("tail");
  });

  it("restarts a failed initial auto-claim with a fresh epoch gate", async () => {
    const requests: Array<string | null> = [];
    const producer = new IdempotentProducer(
      _handle(async (_url, init) => {
        requests.push(new Headers(init?.headers).get("producer-epoch"));
        return requests.length === 1 ? new Response(null, { status: 503 }) : _accepted(init);
      }),
      "writer",
      { autoClaim: true, maxBatchBytes: 1 },
    );
    producer.append("a");
    await producer.flush();
    await producer.restart();
    producer.append("b");
    await producer.flush();
    expect(requests).toEqual(["0", "1"]);
    expect(producer.nextSeq).toBe(1);
    await producer.detach();
  });

  for (const failure of [false, true]) {
    it(`flush joins work admitted during its drain, including failure=${failure}`, async () => {
      const enteredA = _gate<void>();
      const enteredB = _gate<void>();
      const releaseA = _gate<void>();
      const releaseB = _gate<void>();
      const state = { calls: 0, settled: false };
      const errors: Array<string> = [];
      const producer = new IdempotentProducer(
        _handle(async (_url, init) => {
          if (state.calls++ === 0) {
            enteredA.resolve();
            await releaseA.promise;
          } else {
            enteredB.resolve();
            await releaseB.promise;
            if (failure) return new Response(null, { status: 503 });
          }
          return _accepted(init);
        }),
        "writer",
        {
          maxBatchBytes: 2,
          onError: (error) => {
            errors.push(error.code);
          },
        },
      );
      producer.append("a");
      const flushed = producer.flush().then(() => {
        state.settled = true;
      });
      await enteredA.promise;
      producer.append("bb");
      await enteredB.promise;
      releaseA.resolve();
      await _handle(async () => new Response(null, { headers: _headers })).head();
      expect(state.settled).toBe(false);
      releaseB.resolve();
      await flushed;
      expect(producer.inFlightCount).toBe(0);
      expect(errors).toEqual(failure ? ["BUSY"] : []);
      await producer.detach();
    });
  }

  for (const discovery of ["head", "create"] as const) {
    it(`producer observes ${discovery} content discovery after construction`, async () => {
      const bodies: Array<string> = [];
      const handle = _handle(async (_url, init) => {
        if (init?.method !== "POST") return new Response(null, { headers: _headers });
        bodies.push(await new Response(init.body).text());
        expect(new Headers(init.headers).get("content-type")).toBe("application/json");
        return _accepted(init);
      });
      const producer = new IdempotentProducer(handle, "writer");
      if (discovery === "head") await handle.head();
      else await handle.create({ contentType: "application/json" });
      producer.append('{"a":1}');
      await producer.close('{"end":true}');
      expect(bodies).toEqual(['[{"a":1}]', '[{"end":true}]']);
    });
  }

  it("an acquired producer uses later discovery and retains immutable close retry bytes", async () => {
    const requests: Array<{ body: string; contentType: string | null }> = [];
    const handle = _handle(async (_url, init) => {
      if (init?.method === "HEAD") return new Response(null, { headers: _headers });
      requests.push({
        body: await new Response(init?.body).text(),
        contentType: new Headers(init?.headers).get("content-type"),
      });
      return requests.length === 2 ? new Response(null, { status: 503 }) : _accepted(init);
    });
    const producer = new IdempotentProducer(handle, "writer");
    await producer.restart();
    await handle.head();
    producer.append("1");
    await producer.flush();
    await expect(producer.close("2")).rejects.toMatchObject({ code: "BUSY" });
    await producer.close("999");
    expect(requests).toEqual([
      { body: "[1]", contentType: "application/json" },
      { body: "[2]", contentType: "application/json" },
      { body: "[2]", contentType: "application/json" },
    ]);
  });

  it.effect(
    "advanced parent waits for unconsumed response cleanup before borrowed services release",
    () =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const parent = yield* Scope.make();
        const state = { released: false, childReleased: false };
        yield* Scope.addFinalizer(
          parent,
          Effect.sync(() => {
            expect(state.childReleased).toBe(true);
            state.released = true;
          }),
        );
        const http = HttpClient.make((request) =>
          Effect.gen(function* () {
            yield* _requestCleanup(
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.andThen(
                  Effect.sync(() => {
                    state.childReleased = true;
                  }),
                ),
              ),
            );
            return HttpClientResponse.fromWeb(request, new Response("[]", { headers: _headers }));
          }),
        );
        const client = yield* makeEffectClient({
          url: "https://example.test/a",
          schema: Schema.Json,
        }).pipe(
          Effect.provideService(HttpClient.HttpClient, http),
          Effect.provideService(Scope.Scope, parent),
        );
        const response = yield* Effect.promise(() => client.stream());
        const closing = yield* Scope.close(parent, Exit.void).pipe(Effect.forkChild);
        yield* Deferred.await(entered);
        yield* Effect.promise(() =>
          _handle(async () => new Response(null, { headers: _headers })).head(),
        );
        expect(state.released).toBe(false);
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(closing);
        yield* Effect.promise(() => response.closed);
        expect(state.released).toBe(true);
      }),
  );

  it.effect("initial request cleanup finishes before page two dispatch", () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const state = { requests: 0 };
      const http = HttpClient.make((request) =>
        Effect.gen(function* () {
          state.requests++;
          if (state.requests === 1)
            yield* _requestCleanup(
              Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
            );
          const headers = new Headers(_headers);
          if (state.requests === 1) headers.delete("stream-up-to-date");
          return HttpClientResponse.fromWeb(request, new Response("[1]", { headers }));
        }),
      );
      const client = yield* makeEffectClient({
        url: "https://example.test/a",
        schema: Schema.Json,
      }).pipe(Effect.provideService(HttpClient.HttpClient, http));
      const response = yield* Effect.promise(() => client.stream());
      const collecting = yield* Effect.promise(() => response.json()).pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      expect(state.requests).toBe(1);
      yield* Deferred.succeed(release, undefined);
      expect(yield* Fiber.join(collecting)).toEqual([1, 1]);
      expect(state.requests).toBe(2);
    }),
  );
});
