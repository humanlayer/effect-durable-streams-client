import { describe, expect, it } from "@effect/vitest";
import { Array as Arr, Effect, Schema } from "effect";
import { DurableStream, IdempotentProducer } from "../../src/async-await";
import { acquireDurableStreamServer } from "../support/server";

describe("async facade against reference server", () => {
  it.effect("follows SSE and long-poll through final payload and EOF", () =>
    Effect.gen(function* () {
      const { baseUrl } = yield* acquireDurableStreamServer;
      yield* Effect.promise(async () => {
        for (const live of ["long-poll", "sse"] as const) {
          const handle = await DurableStream.create({
            url: `${baseUrl}/async-${live}`,
            contentType: "application/json",
          });
          const response = await handle.stream({ live });
          const items: Array<Schema.Json> = [];
          response.subscribeJson(async (batch) => {
            items.push(...batch.items);
            if (batch.upToDate && !batch.streamClosed && !Arr.isArrayNonEmpty(items))
              await handle.close({ body: '"last"' });
          });
          await response.closed;
          expect(items).toEqual(["last"]);
          expect(response.streamClosed).toBe(true);
          expect(response.live).toBe(live);
        }
      });
    }),
  );

  it.effect("round-trips typed messages and auto-claims a fenced producer", () =>
    Effect.gen(function* () {
      const { baseUrl } = yield* acquireDurableStreamServer;
      yield* Effect.promise(async () => {
        const typed = await DurableStream.createWithSchema({
          url: `${baseUrl}/async-typed`,
          schema: Schema.Struct({ id: Schema.String }),
        });
        await typed.appendJsonBatch([{ id: "one" }, { id: "two" }]);
        expect(await (await typed.stream()).json()).toEqual([{ id: "one" }, { id: "two" }]);
        const high = new IdempotentProducer(typed.raw, "writer", { epoch: 5 });
        high.append('{"id":"high"}');
        await high.detach();
        const failures: Array<string> = [];
        const stale = new IdempotentProducer(typed.raw, "writer", {
          epoch: 0,
          onError: (error) => {
            failures.push(error.code);
          },
        });
        stale.append('{"id":"fenced"}');
        await stale.detach();
        expect(failures).toEqual(["STALE_EPOCH"]);
        const claimed = new IdempotentProducer(typed.raw, "writer", { epoch: 0, autoClaim: true });
        claimed.append('{"id":"claimed"}');
        await claimed.flush();
        expect(claimed.epoch).toBe(6);
        await claimed.detach();
        await claimed.close();
        expect(await (await typed.stream()).json()).toEqual([
          { id: "one" },
          { id: "two" },
          { id: "high" },
          { id: "claimed" },
        ]);
      });
    }),
  );

  it.effect("creates, writes serialized JSON, reads independent sessions and closes", () =>
    Effect.gen(function* () {
      const { baseUrl } = yield* acquireDurableStreamServer;
      yield* Effect.promise(async () => {
        const url = `${baseUrl}/async-ordinary`;
        const missing = await DurableStream.connect({ url });
        expect(await missing.head()).toEqual({ exists: false });
        const handle = await DurableStream.create({
          url,
          contentType: "application/json",
          body: "[1,2]",
        });
        await handle.create({ contentType: "application/json" });
        await handle.append('{"x":1}');
        await handle.append("[3,4]");
        const response = await handle.stream();
        expect(response.offset).toBe("-1");
        expect(await response.json()).toEqual([1, 2, { x: 1 }, [3, 4]]);
        await response.closed;
        await expect(response.json()).rejects.toMatchObject({ code: "ALREADY_CONSUMED" });
        const final = await handle.close({ body: '"done"' });
        const second = await handle.stream({ live: false });
        expect(await second.json()).toEqual([1, 2, { x: 1 }, [3, 4], "done"]);
        expect(second.offset).toBe(final.finalOffset);
        await handle.delete();
        expect(await handle.head()).toEqual({ exists: false });
      });
    }),
  );

  it.effect("admits producer input synchronously and drains before remote close", () =>
    Effect.gen(function* () {
      const { baseUrl } = yield* acquireDurableStreamServer;
      yield* Effect.promise(async () => {
        const handle = await DurableStream.create({
          url: `${baseUrl}/async-producer`,
          contentType: "application/json",
        });
        const failures: Array<Error> = [];
        const producer = new IdempotentProducer(handle, "writer", {
          onError: (error) => {
            failures.push(error);
          },
        });
        expect(producer.append('{"a":1}')).toBeUndefined();
        expect(producer.append("[2,3]")).toBeUndefined();
        expect(producer.pendingCount).toBe(2);
        await producer.flush();
        expect(failures).toEqual([]);
        expect(producer.nextSeq).toBe(1);
        const result = await producer.close('"final"');
        expect(await producer.close('"ignored"')).toEqual(result);
        expect(() => producer.append("0")).toThrow();
        const response = await handle.stream({ live: false });
        expect(await response.json()).toEqual([{ a: 1 }, [2, 3], "final"]);
        await producer.restart();
        expect(producer.epoch).toBe(1);
        expect(() => producer.append("0")).toThrow();
      });
    }),
  );
});
