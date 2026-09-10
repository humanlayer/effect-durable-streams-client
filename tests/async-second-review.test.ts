import { describe, expect, it } from "@effect/vitest";
import { getEventListeners } from "node:events";
import { Deferred, Effect, Exit, Predicate, Record, Schema } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import {
  DurableStream,
  IdempotentProducer,
  InvalidClientOptionsError,
  makeEffectClient,
  stream,
} from "../src/async-await";

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
const _accepted = (init: RequestInit | undefined) =>
  new Response(null, {
    status: 200,
    headers: {
      ..._headers,
      "producer-epoch": new Headers(init?.headers).get("producer-epoch") ?? "0",
      "producer-seq": new Headers(init?.headers).get("producer-seq") ?? "0",
      "stream-closed": new Headers(init?.headers).get("stream-closed") ?? "false",
    },
  });

describe("second full-artifact review", () => {
  for (const web of [false, true]) {
    it(`upload source failure is preserved and not retried web=${web}`, async () => {
      const events: Array<string> = [];
      const source = web
        ? new ReadableStream<string>(
            {
              pull: () => {
                events.push("pull");
                throw new Error("source");
              },
            },
            { highWaterMark: 0 },
          )
        : {
            [Symbol.asyncIterator]: () => ({
              next: async () => {
                events.push("pull");
                throw new Error("source");
              },
              return: async () => {
                events.push("return");
                return { done: true as const, value: undefined };
              },
            }),
          };
      const client = new DurableStream({
        url: "https://example.test/a",
        fetch: async (_url, init) => {
          events.push("fetch");
          await new Response(init?.body).text();
          return new Response(null, { status: 204, headers: _headers });
        },
      });
      await expect(client.appendStream(source)).rejects.toMatchObject({ code: "SOURCE_ERROR" });
      expect(events.filter((event) => event === "fetch")).toHaveLength(1);
      expect(events.filter((event) => event === "pull")).toHaveLength(1);
      if (!web) expect(events).toContain("return");
    });

    it(`upload backpressure and cancellation release source web=${web}`, async () => {
      const pulled = _gate<void>();
      const released = _gate<void>();
      const finishFetch = _gate<void>();
      const state = { pulls: 0, requests: 0 };
      const next = () => {
        state.pulls++;
        pulled.resolve();
        return "chunk";
      };
      const source = web
        ? new ReadableStream<string>(
            {
              pull: (controller) => controller.enqueue(next()),
              cancel: () => {
                released.resolve();
              },
            },
            { highWaterMark: 0 },
          )
        : {
            [Symbol.asyncIterator]: () => ({
              next: async () => ({ done: false as const, value: next() }),
              return: async () => {
                released.resolve();
                return { done: true as const, value: undefined };
              },
            }),
          };
      const controller = new AbortController();
      const client = new DurableStream({
        url: "https://example.test/a",
        fetch: async (_url, init) => {
          state.requests++;
          const reader = new Response(init?.body).body?.getReader();
          await reader?.read();
          await finishFetch.promise;
          await reader?.cancel();
          reader?.releaseLock();
          return new Response(null, { status: 204, headers: _headers });
        },
      });
      const upload = client.appendStream(source, { signal: controller.signal });
      await pulled.promise;
      expect(state.pulls).toBeLessThan(100);
      controller.abort();
      await expect(upload).rejects.toMatchObject({ code: "ABORTED" });
      await released.promise;
      const count = state.pulls;
      finishFetch.resolve();
      expect(state.requests).toBe(1);
      expect(state.pulls).toBe(count);
    });
  }

  for (const throwing of [false, true])
    it(`background failures drain with absent/throwing onError throwing=${throwing}`, async () => {
      const errors: Array<string> = [];
      const producer = new IdempotentProducer(
        new DurableStream({
          url: "https://example.test/a",
          fetch: async () => new Response(null, { status: 503 }),
        }),
        "writer",
        {
          maxBatchBytes: 1,
          ...Record.filter(
            {
              onError: throwing
                ? (error: import("../src/async-await").DurableStreamError) => {
                    errors.push(error.code);
                    throw new Error("callback");
                  }
                : undefined,
            },
            Predicate.isNotUndefined,
          ),
        },
      );
      expect(producer.append("x")).toBeUndefined();
      await producer.flush();
      await producer.detach();
      expect(errors).toEqual(throwing ? ["BUSY"] : []);
      expect(producer.inFlightCount).toBe(0);
    });

  it("writable close failure takes precedence over a failed batch", async () => {
    const requests: Array<boolean> = [];
    const writer = new DurableStream({
      url: "https://example.test/a",
      fetch: async (_url, init) => {
        const closing = new Headers(init?.headers).has("stream-closed");
        requests.push(closing);
        return new Response(null, { status: closing ? 404 : 503 });
      },
    })
      .writable({ producerId: "writer" })
      .getWriter();
    await writer.write("x");
    await expect(writer.close()).rejects.toMatchObject({ code: "NOT_FOUND" });
    writer.releaseLock();
    expect(requests).toEqual([false, true]);
  });

  it("writable abort returns before its observed drain and never sends EOF", async () => {
    const entered = _gate<void>();
    const release = _gate<void>();
    const notified = _gate<void>();
    const requests: Array<boolean> = [];
    const writer = new DurableStream({
      url: "https://example.test/a",
      fetch: async (_url, init) => {
        requests.push(new Headers(init?.headers).has("stream-closed"));
        entered.resolve();
        await release.promise;
        return new Response(null, { status: 503 });
      },
    })
      .writable({
        producerId: "writer",
        maxBatchBytes: 1,
        onError: () => {
          notified.resolve();
        },
      })
      .getWriter();
    await writer.write("x");
    await entered.promise;
    await writer.abort();
    expect(requests).toEqual([false]);
    release.resolve();
    await notified.promise;
    writer.releaseLock();
    expect(requests).toEqual([false]);
  });

  it("maps invalid lifetimes before dispatch", async () => {
    const calls: Array<string> = [];
    const client = new DurableStream({
      url: "https://example.test/a",
      fetch: async () => {
        calls.push("fetch");
        return new Response();
      },
    });
    for (const options of [{ ttlSeconds: -1 }, { ttlSeconds: 0.5 }, { expiresAt: "invalid" }])
      await expect(client.create(options)).rejects.toBeInstanceOf(InvalidClientOptionsError);
    expect(calls).toEqual([]);
  });

  it("standalone Promise stream rejects invalid construction without throwing", async () => {
    const pending = stream({ url: "invalid" });
    await expect(pending).rejects.toBeInstanceOf(InvalidClientOptionsError);
  });

  it("rejects unsupported options and malformed signals before listeners or dispatch", async () => {
    const calls: Array<string> = [];
    const options = {
      url: "https://example.test/a",
      fetch: async () => {
        calls.push("fetch");
        return new Response();
      },
    };
    for (const extra of [
      { onError: () => undefined },
      { warnOnHttp: false },
      { backoffOptions: { maxRetries: 0, debug: false } },
    ])
      expect(() => new DurableStream({ ...options, ...extra })).toThrow(InvalidClientOptionsError);
    const client = new DurableStream(options);
    const structuralExtra = { ...options, applicationLabel: "ignored" };
    expect(new DurableStream(structuralExtra).url).toBe(options.url);
    const controller = new AbortController();
    expect(() => new IdempotentProducer(client, "writer", { lingerMs: NaN })).toThrow(
      InvalidClientOptionsError,
    );
    // @ts-expect-error SAFETY: deliberately exercise the foreign invalid callback boundary.
    expect(() => new IdempotentProducer(client, "writer", { onError: 42 })).toThrow(
      InvalidClientOptionsError,
    );
    expect(
      () => new IdempotentProducer(client, "writer", { signal: controller.signal, epoch: -1 }),
    ).toThrow(InvalidClientOptionsError);
    expect(getEventListeners(controller.signal, "abort")).toHaveLength(0);
    // @ts-expect-error SAFETY: deliberately exercise invalid constructor signal validation.
    expect(() => new DurableStream({ ...options, signal: {} })).toThrow(InvalidClientOptionsError);
    // @ts-expect-error SAFETY: deliberately exercise the foreign invalid signal boundary.
    expect(() => new IdempotentProducer(client, "writer", { signal: {} })).toThrow(
      InvalidClientOptionsError,
    );
    // @ts-expect-error SAFETY: deliberately exercise the foreign invalid signal boundary.
    await expect(client.head({ signal: {} })).rejects.toBeInstanceOf(InvalidClientOptionsError);
    // @ts-expect-error SAFETY: deliberately exercise the foreign invalid signal boundary.
    await expect(client.stream({ signal: {} })).rejects.toBeInstanceOf(InvalidClientOptionsError);
    expect(calls).toEqual([]);
  });

  for (const closed of [false, true])
    it(`validates schema compatibility before create closed=${closed}`, async () => {
      const calls: Array<string> = [];
      await expect(
        DurableStream.createWithSchema({
          url: "https://example.test/a",
          schema: Schema.String,
          contentType: "text/plain",
          closed,
          fetch: async () => {
            calls.push("PUT");
            return new Response(null, { headers: _headers });
          },
        }),
      ).rejects.toBeInstanceOf(InvalidClientOptionsError);
      expect(calls).toEqual([]);
    });

  it("rejects invalid typed and upload content types before encoding or source acquisition", async () => {
    const calls: Array<string> = [];
    const client = DurableStream.withSchema({
      url: "https://example.test/a",
      schema: Schema.String,
      fetch: async () => {
        calls.push("fetch");
        return new Response();
      },
    });
    const contentType = "application/json;\r\nx-injected: true";
    await expect(client.appendJson("x", { contentType })).rejects.toMatchObject({
      code: "BAD_REQUEST",
    });
    const source = {
      [Symbol.asyncIterator]: () => {
        calls.push("acquire");
        return { next: async () => ({ done: true as const, value: undefined }) };
      },
    };
    await expect(client.raw.appendStream(source, { contentType })).rejects.toMatchObject({
      code: "BAD_REQUEST",
    });
    expect(calls).toEqual([]);
  });

  it.effect("advanced invalid specialization fails in declared error channel", () =>
    Effect.gen(function* () {
      const error = yield* makeEffectClient({
        url: "https://example.test/a",
        schema: Schema.String,
        contentType: "text/plain",
      }).pipe(Effect.flip);
      expect(error).toBeInstanceOf(InvalidClientOptionsError);
    }).pipe(Effect.provide(FetchHttpClient.layer)),
  );

  it("restart rebuilds failed close tuple but preserves bytes and cached success", async () => {
    const requests: Array<{ epoch: string | null; seq: string | null; body: string }> = [];
    const producer = new IdempotentProducer(
      new DurableStream({
        url: "https://example.test/a",
        contentType: "application/json",
        fetch: async (_url, init) => {
          const headers = new Headers(init?.headers);
          requests.push({
            epoch: headers.get("producer-epoch"),
            seq: headers.get("producer-seq"),
            body: await new Response(init?.body).text(),
          });
          return requests.length === 1 ? new Response(null, { status: 503 }) : _accepted(init);
        },
      }),
      "writer",
    );
    await expect(producer.close("123")).rejects.toMatchObject({ code: "BUSY" });
    await producer.restart();
    expect(await producer.close("456")).toEqual({ finalOffset: "tail" });
    await producer.restart();
    expect(await producer.close()).toEqual({ finalOffset: "tail" });
    expect(requests).toEqual([
      { epoch: "0", seq: "0", body: "[123]" },
      { epoch: "1", seq: "0", body: "[123]" },
    ]);
  });

  it("invalid final preparation allows corrected retry and detach cleanup", async () => {
    const bodies: Array<string> = [];
    const controller = new AbortController();
    const client = new DurableStream({
      url: "https://example.test/a",
      contentType: "application/json",
      signal: controller.signal,
      fetch: async (_url, init) => {
        bodies.push(await new Response(init?.body).text());
        return _accepted(init);
      },
    });
    const producer = new IdempotentProducer(client, "writer");
    expect(getEventListeners(controller.signal, "abort")).toHaveLength(1);
    await expect(producer.close("invalid")).rejects.toMatchObject({ code: "BAD_REQUEST" });
    expect(await producer.close("42")).toEqual({ finalOffset: "tail" });
    expect(getEventListeners(controller.signal, "abort")).toHaveLength(0);
    const detached = new IdempotentProducer(client, "other");
    await expect(detached.close("invalid")).rejects.toMatchObject({ code: "BAD_REQUEST" });
    detached.append("1");
    await detached.detach();
    expect(getEventListeners(controller.signal, "abort")).toHaveLength(0);
    expect(() => detached.append("2")).toThrow();
    controller.abort();
    expect(bodies).toEqual(["[42]", "[1]"]);
  });

  it("rejects BOM serialized JSON at ordinary, producer and final-close boundaries", async () => {
    const calls: Array<string> = [];
    const client = new DurableStream({
      url: "https://example.test/a",
      contentType: "application/json",
      fetch: async () => {
        calls.push("fetch");
        return new Response();
      },
    });
    const producer = new IdempotentProducer(client, "writer");
    for (const body of ["\ufeff1", new Uint8Array([0xef, 0xbb, 0xbf, 49])]) {
      await expect(client.append(body)).rejects.toMatchObject({ code: "BAD_REQUEST" });
      expect(() => producer.append(body)).toThrow();
      await expect(client.close({ body })).rejects.toMatchObject({ code: "BAD_REQUEST" });
      await expect(producer.close(body)).rejects.toMatchObject({ code: "BAD_REQUEST" });
    }
    await producer.detach();
    expect(calls).toEqual([]);
  });

  it("cancels a late noncooperative fetch body without awaiting foreign work", async () => {
    const entered = _gate<void>();
    const response = _gate<Response>();
    const cancelled = _gate<void>();
    const controller = new AbortController();
    const client = new DurableStream({
      url: "https://example.test/a",
      fetch: async () => {
        entered.resolve();
        return response.promise;
      },
    });
    const pending = client.stream({ signal: controller.signal });
    await entered.promise;
    controller.abort();
    await expect(pending).rejects.toMatchObject({ code: "ABORTED" });
    response.resolve(
      new Response(
        new ReadableStream({
          cancel: () => {
            cancelled.resolve();
          },
        }),
        {
          headers: _headers,
        },
      ),
    );
    await cancelled.promise;
  });

  it("same-handle concurrent reads isolate auth offsets and cancellation", async () => {
    const entered = _gate<void>();
    const release = _gate<void>();
    const requests: Array<string> = [];
    const client = new DurableStream({
      url: "https://example.test/a",
      fetch: async (url, init) => {
        requests.push(
          `${new URL(url instanceof Request ? url.url : url).searchParams.get("offset")}:${new Headers(init?.headers).get("authorization")}`,
        );
        return new Response("[1,2]", { headers: _headers });
      },
    });
    const pending = client.stream({
      offset: "one",
      headers: {
        Authorization: async () => {
          entered.resolve();
          await release.promise;
          return "A";
        },
      },
    });
    await entered.promise;
    const second = await client.stream({ offset: "two", headers: { Authorization: "B" } });
    release.resolve();
    const first = await pending;
    first.cancel();
    await first.closed;
    expect(await second.json()).toEqual([1, 2]);
    expect(first.offset).toBe("one");
    expect(second.offset).toBe("tail");
    expect(requests).toEqual(["two:B", "one:A"]);
  });
});
