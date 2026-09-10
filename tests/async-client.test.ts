import { Deferred, Effect, Exit, Predicate, Record } from "effect";
import { describe, expect, it } from "@effect/vitest";
import {
  DurableStream,
  IdempotentProducer,
  InvalidClientOptionsError,
} from "../src/async-await.js";

const _response = (
  body: string,
  options: {
    readonly offset?: string;
    readonly upToDate?: boolean;
    readonly closed?: boolean;
  } = {},
) =>
  new Response(body, {
    headers: {
      "content-type": "application/json",
      "stream-next-offset": options.offset ?? "tail",
      ...Record.filter(
        { "stream-up-to-date": (options.upToDate ?? true) ? "true" : undefined },
        Predicate.isNotUndefined,
      ),
      "stream-closed": String(options.closed ?? false),
    },
  });

const _gate = <A>() => {
  const deferred = Deferred.makeUnsafe<A>();
  return {
    promise: Effect.runPromise(Deferred.await(deferred)),
    resolve: (value: A) => {
      Deferred.doneUnsafe(deferred, Exit.succeed(value));
    },
  };
};

describe("async lifecycle and serialized writes (phase B)", () => {
  it("is cold, rejects invalid configuration and keeps serialized lexemes", async () => {
    const bodies: Array<string> = [];
    const fetchClient: typeof fetch = async (_input, init) => {
      bodies.push(await new Response(init?.body).text());
      return new Response(null, { status: 204, headers: { "stream-next-offset": "tail" } });
    };
    const handle = new DurableStream({
      url: "https://example.test/prefix/a%2Fb",
      contentType: "application/json",
      fetch: fetchClient,
    });
    expect(bodies).toEqual([]);
    expect(() => new DurableStream({ url: "invalid" })).toThrow(InvalidClientOptionsError);
    await handle.append(' {"n":9007199254740993} ');
    await handle.append("[1,2]");
    expect(bodies).toEqual(['[ {"n":9007199254740993} ]', "[[1,2]]"]);
    await expect(handle.append(" ")).rejects.toMatchObject({ code: "BAD_REQUEST" });
    expect(bodies).toHaveLength(2);
  });

  it("copies caller bytes before admission and shares only overlap", async () => {
    const entered = _gate<void>();
    const release = _gate<void>();
    const bodies: Array<string> = [];
    const fetchClient: typeof fetch = async (_input, init) => {
      bodies.push(await new Response(init?.body).text());
      if (bodies.length === 1) {
        entered.resolve();
        await release.promise;
      }
      return new Response(null, { status: 204, headers: { "stream-next-offset": "tail" } });
    };
    const handle = new DurableStream({
      url: "https://example.test/a",
      contentType: "application/json",
      fetch: fetchClient,
    });
    const first = handle.append("0");
    await entered.promise;
    const bytes = new TextEncoder().encode("1");
    const second = handle.append(bytes);
    bytes[0] = 57;
    const third = handle.append("2");
    release.resolve();
    await Promise.all([first, second, third]);
    expect(bodies).toEqual(["[0]", "[1,2]"]);
  });

  it("resolves metadata per retry and never retries metadata rejection", async () => {
    const tokens: Array<string | null> = [];
    const state = { resolutions: 0 };
    const fetchClient: typeof fetch = async (_input, init) => {
      tokens.push(new Headers(init?.headers).get("authorization"));
      return tokens.length === 1
        ? new Response(null, { status: 503 })
        : new Response(null, { status: 204, headers: { "stream-next-offset": "tail" } });
    };
    const handle = new DurableStream({
      url: "https://example.test/a",
      fetch: fetchClient,
      headers: { Authorization: () => String(++state.resolutions) },
      backoffOptions: { maxRetries: 1, initialDelay: 1 },
    });
    await handle.append("bytes");
    expect(tokens).toEqual(["1", "2"]);
    const failing = new DurableStream({
      url: "https://example.test/a",
      fetch: fetchClient,
      headers: { Authorization: () => Promise.reject(new Error("secret")) },
    });
    await expect(failing.append("bytes")).rejects.toMatchObject({ code: "REQUEST_METADATA_ERROR" });
    expect(tokens).toHaveLength(2);
  });
});

describe("async read ownership (phase C)", () => {
  it("does not dispatch an already-aborted acquisition", async () => {
    const requests: Array<string> = [];
    const fetchClient: typeof fetch = async () => {
      requests.push("GET");
      return _response("[]");
    };
    const signal = AbortSignal.abort();
    await expect(
      new DurableStream({ url: "https://example.test/a", fetch: fetchClient }).stream({ signal }),
    ).rejects.toMatchObject({ code: "ABORTED" });
    expect(requests).toEqual([]);
  });

  it("cancels without waiting for a noncooperative callback or acknowledging it", async () => {
    const entered = _gate<void>();
    const release = _gate<void>();
    const requests: Array<string> = [];
    const fetchClient: typeof fetch = async () => {
      requests.push("GET");
      return _response("[]");
    };
    const response = await new DurableStream({
      url: "https://example.test/a",
      fetch: fetchClient,
    }).stream();
    const unsubscribe = response.subscribeJson(async () => {
      entered.resolve();
      await release.promise;
    });
    await entered.promise;
    expect(() => response.textStream()).toThrow();
    unsubscribe();
    await response.closed;
    expect(response.offset).toBe("-1");
    release.resolve();
    await release.promise;
    expect(requests).toEqual(["GET"]);
  });

  it("acquires headers without consuming and cancel joins the request", async () => {
    const state = { pulls: 0, cancelled: false };
    const fetchClient: typeof fetch = async () =>
      new Response(
        new ReadableStream(
          {
            pull() {
              state.pulls++;
            },
            cancel() {
              state.cancelled = true;
            },
          },
          { highWaterMark: 0 },
        ),
        {
          headers: {
            "content-type": "application/json",
            "stream-next-offset": "tail",
            "stream-up-to-date": "true",
          },
        },
      );
    const response = await new DurableStream({
      url: "https://example.test/a",
      fetch: fetchClient,
    }).stream();
    expect(state.pulls).toBe(0);
    expect(response.offset).toBe("-1");
    response.cancel();
    await response.closed;
    expect(state.cancelled).toBe(true);
    await expect(response.json()).rejects.toMatchObject({ code: "ABORTED" });
    expect(state.pulls).toBe(0);
  });

  it("collects multi-page live catch-up without a live request", async () => {
    const urls: Array<string> = [];
    const fetchClient: typeof fetch = async (url) => {
      urls.push(url instanceof Request ? url.url : String(url));
      return urls.length === 1
        ? _response("[1]", { offset: "one", upToDate: false })
        : _response("[2]");
    };
    const response = await new DurableStream({
      url: "https://example.test/a",
      fetch: fetchClient,
    }).stream({ live: "sse" });
    expect(await response.json()).toEqual([1, 2]);
    await response.closed;
    expect(urls).toEqual(["https://example.test/a?offset=-1", "https://example.test/a?offset=one"]);
    expect(response.offset).toBe("tail");
    expect(response.upToDate).toBe(true);
    response.cancel();
    await response.closed;
  });

  it("awaits callbacks, emits empty boundaries and preserves the safe offset on rejection", async () => {
    const entered = _gate<void>();
    const release = _gate<void>();
    const state = { requests: 0 };
    const fetchClient: typeof fetch = async () => {
      state.requests++;
      return _response("[]");
    };
    const response = await new DurableStream({
      url: "https://example.test/a",
      fetch: fetchClient,
    }).stream();
    response.subscribeJson(async (batch) => {
      expect(batch.items).toEqual([]);
      expect(batch.upToDate).toBe(true);
      entered.resolve();
      await release.promise;
      throw new Error("secret callback payload");
    });
    await entered.promise;
    expect(response.offset).toBe("-1");
    expect(state.requests).toBe(1);
    release.resolve();
    await expect(response.closed).rejects.toMatchObject({ code: "CALLBACK_ERROR" });
    expect(response.offset).toBe("-1");
    expect(state.requests).toBe(1);
  });

  it("rejects strict empty JSON and does not expose FiberFailure", async () => {
    const fetchClient: typeof fetch = async () => _response("");
    const response = await new DurableStream({
      url: "https://example.test/a",
      fetch: fetchClient,
    }).stream();
    await expect(response.json()).rejects.toMatchObject({
      code: "PARSE_ERROR",
      name: "DurableStreamError",
    });
    await expect(response.closed).rejects.toMatchObject({ code: "PARSE_ERROR" });
    expect(response.offset).toBe("-1");
  });

  it("Web iteration leaves a partially delivered JSON boundary unacknowledged", async () => {
    const state = { requests: 0 };
    const fetchClient: typeof fetch = async () => {
      state.requests++;
      return _response("[1,2]");
    };
    const response = await new DurableStream({
      url: "https://example.test/a",
      fetch: fetchClient,
    }).stream();
    for await (const item of response.jsonStream()) {
      expect(item).toBe(1);
      break;
    }
    await response.closed;
    expect(response.offset).toBe("-1");
    expect(state.requests).toBe(1);
  });

  it("retains strict UTF-8 across page boundaries", async () => {
    const state = { requests: 0 };
    const fetchClient: typeof fetch = async () => {
      state.requests++;
      return new Response(new Uint8Array(state.requests === 1 ? [0xe2] : [0x82, 0xac]), {
        headers: {
          "content-type": "text/plain",
          "stream-next-offset": state.requests === 1 ? "one" : "tail",
          ...Record.filter(
            { "stream-up-to-date": state.requests === 2 ? "true" : undefined },
            Predicate.isNotUndefined,
          ),
        },
      });
    };
    const response = await new DurableStream({
      url: "https://example.test/a",
      fetch: fetchClient,
    }).stream();
    expect(await response.text()).toBe("€");
    expect(response.offset).toBe("tail");
  });
});

describe("async producer admission (phase D)", () => {
  it("writable close attempts EOF before surfacing the first batch error", async () => {
    const requests: Array<boolean> = [];
    const fetchClient: typeof fetch = async (_url, init) => {
      const headers = new Headers(init?.headers);
      const closed = headers.get("stream-closed") === "true";
      requests.push(closed);
      if (!closed) return new Response(null, { status: 503 });
      return new Response(null, {
        status: 200,
        headers: {
          "producer-epoch": headers.get("producer-epoch") ?? "0",
          "producer-seq": headers.get("producer-seq") ?? "0",
          "stream-closed": "true",
          "stream-next-offset": "tail",
        },
      });
    };
    const writer = new DurableStream({ url: "https://example.test/a", fetch: fetchClient })
      .writable({ producerId: "writer" })
      .getWriter();
    await writer.write("first");
    await expect(writer.close()).rejects.toMatchObject({ code: "BUSY" });
    expect(requests).toEqual([false, true]);
    writer.releaseLock();
  });

  it("returns void before delivery, drains failures and notifies once per batch", async () => {
    const entered = _gate<void>();
    const release = _gate<void>();
    const bodies: Array<string> = [];
    const failures: Array<string> = [];
    const fetchClient: typeof fetch = async (_url, init) => {
      bodies.push(await new Response(init?.body).text());
      entered.resolve();
      await release.promise;
      return new Response(null, { status: 503 });
    };
    const handle = new DurableStream({
      url: "https://example.test/a",
      contentType: "application/json",
      fetch: fetchClient,
    });
    const producer = new IdempotentProducer(handle, "writer", {
      maxBatchBytes: 2,
      onError: (error) => {
        failures.push(error.code);
      },
    });
    expect(producer.append("1")).toBeUndefined();
    expect(producer.append("2")).toBeUndefined();
    const drain = producer.flush();
    await entered.promise;
    expect(failures).toEqual([]);
    expect(bodies).toEqual(["[1,2]"]);
    release.resolve();
    await drain;
    expect(failures).toEqual(["BUSY"]);
    await producer.detach();
    expect(() => producer.append("3")).toThrow();
    expect(bodies).toHaveLength(1);
  });

  it("detach closes admission immediately and a repeated detach does not join", async () => {
    const entered = _gate<void>();
    const release = _gate<void>();
    const fetchClient: typeof fetch = async (_url, init) => {
      entered.resolve();
      await release.promise;
      return new Response(null, {
        status: 200,
        headers: {
          "producer-epoch": new Headers(init?.headers).get("producer-epoch") ?? "0",
          "producer-seq": "0",
          "stream-next-offset": "tail",
        },
      });
    };
    const handle = new DurableStream({ url: "https://example.test/a", fetch: fetchClient });
    const producer = new IdempotentProducer(handle, "writer");
    producer.append("hello");
    const detached = producer.detach();
    expect(() => producer.append("later")).toThrow();
    await entered.promise;
    await producer.detach();
    release.resolve();
    await detached;
    expect(producer.inFlightCount).toBe(0);
  });
});
