import { describe, expect, expectTypeOf, it } from "@effect/vitest";
import {
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Predicate,
  Queue,
  Record,
  Schema,
  SchemaGetter,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import { HttpClient, HttpClientRequest, HttpClientError } from "effect/unstable/http";
import { DurableStreamsClient } from "../src/index.ts";
import { makeReadHttp, readReply, type ReadReply } from "./support/read-http.ts";

const _poll = (input: {
  readonly offset: string;
  readonly cursor?: string;
  readonly closed?: boolean;
}) =>
  ({
    status: 204,
    headers: {
      "stream-next-offset": input.offset,
      "stream-up-to-date": "true",
      ...Record.filter(
        { "stream-cursor": input.cursor, "stream-closed": input.closed ? "true" : undefined },
        Predicate.isNotUndefined,
      ),
    },
    body: Stream.empty,
  }) satisfies ReadReply;

describe("long-poll read sessions", () => {
  for (const view of ["text", "json"] as const) {
    it.effect(
      `delivers closed 200 ${view} without a cursor before committing the final offset`,
      () =>
        Effect.gen(function* () {
          const http = yield* makeReadHttp;
          const delivered = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const client = yield* DurableStreamsClient.make({
            url: "https://streams.test/s",
            live: "long-poll",
          });
          const source: typeof client.json = client[view];
          const run = yield* source.pipe(
            Stream.tap(() =>
              Deferred.succeed(delivered, undefined).pipe(Effect.andThen(Deferred.await(release))),
            ),
            Stream.runCollect,
            Effect.provide(http.layer),
            Effect.forkChild,
          );
          yield* Queue.take(http.requests);
          const contentType = view === "json" ? "application/json" : "text/plain";
          yield* Queue.offer(
            http.replies,
            readReply({
              offset: "tail",
              text: view === "json" ? "[]" : "",
              contentType,
              upToDate: true,
            }),
          );
          const request = yield* Queue.take(http.requests);
          expect(new URL(request.request.url).searchParams.get("live")).toBe("long-poll");
          yield* Queue.offer(
            http.replies,
            readReply({
              offset: "final+opaque",
              text: view === "json" ? "[1,[2,3],null]" : "final payload",
              contentType,
              closed: true,
            }),
          );
          yield* Deferred.await(delivered).pipe(Effect.raceFirst(Fiber.join(run)));
          expect(yield* client.offset).toEqual(Option.some("tail"));
          expect(request.signal.aborted).toBe(false);
          yield* Deferred.succeed(release, undefined);
          expect(yield* Fiber.join(run)).toEqual(
            view === "json" ? [1, [2, 3], null] : ["final payload"],
          );
          expect(yield* client.offset).toEqual(Option.some("final+opaque"));
          expect(request.signal.aborted).toBe(true);
          expect(yield* Queue.size(http.requests)).toBe(0);
        }),
    );
  }
  it.effect(
    "resets explicit retry budgets per request and cancels a pending backoff without continuation",
    () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/s",
          live: "long-poll",
          backoffOptions: { maxRetries: 1 },
        });
        const run = yield* client.bytes.pipe(
          Stream.runDrain,
          Effect.provide(http.layer),
          Effect.forkChild,
        );
        yield* Queue.take(http.requests);
        yield* Queue.offer(http.replies, readReply({ offset: "tail", text: "", upToDate: true }));
        for (const closed of [false, true]) {
          const request = yield* Queue.take(http.requests);
          yield* Queue.offer(http.replies, {
            status: 429,
            headers: { "retry-after": "2" },
            body: Stream.empty,
          });
          yield* TestClock.adjust("2 seconds");
          const retry = yield* Queue.take(http.requests);
          expect(retry.request.url).toBe(request.request.url);
          yield* Queue.offer(http.replies, _poll({ offset: "tail", cursor: "c", closed }));
        }
        yield* Fiber.join(run);
        const cancelled = yield* DurableStreamsClient.make({
          url: "https://streams.test/s",
          live: "long-poll",
        });
        const waiting = yield* cancelled.bytes.pipe(
          Stream.runDrain,
          Effect.provide(http.layer),
          Effect.forkChild,
        );
        yield* Queue.take(http.requests);
        yield* Queue.offer(http.replies, readReply({ offset: "safe", text: "", upToDate: true }));
        const request = yield* Queue.take(http.requests);
        yield* Queue.offer(http.replies, {
          status: 503,
          headers: { "retry-after": "3600" },
          body: Stream.empty,
        });
        yield* TestClock.adjust("1 second");
        expect(request.signal.aborted).toBe(true);
        yield* Fiber.interrupt(waiting);
        expect(Exit.hasInterrupts(yield* Fiber.await(waiting))).toBe(true);
        expect(yield* cancelled.offset).toEqual(Option.some("safe"));
        yield* TestClock.adjust("2 hours");
        expect(yield* Queue.size(http.requests)).toBe(0);
      }),
  );
  it.effect(
    "keeps decoding services ambient and offsets uncommitted while schema decoding waits",
    () =>
      Effect.gen(function* () {
        class Decode extends Context.Service<Decode, { readonly prefix: string }>()(
          "long-poll/Decode",
        ) {}
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const schema = Schema.String.pipe(
          Schema.decodeTo(Schema.Struct({ id: Schema.String }), {
            decode: SchemaGetter.transformOrFail((value) =>
              Effect.gen(function* () {
                const service = yield* Decode;
                yield* Deferred.succeed(entered, undefined);
                yield* Deferred.await(release);
                return { id: service.prefix + value };
              }),
            ),
            encode: SchemaGetter.transform((value) => value.id),
          }),
        );
        const http = yield* makeReadHttp;
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/s",
          live: "long-poll",
          schema,
        });
        expectTypeOf<Stream.Services<typeof client.json>>().toEqualTypeOf<
          HttpClient.HttpClient | Decode
        >();
        const run = yield* client.json.pipe(
          Stream.runCollect,
          Effect.provide(Layer.merge(http.layer, Layer.succeed(Decode, { prefix: "id-" }))),
          Effect.forkChild,
        );
        yield* Queue.take(http.requests);
        yield* Queue.offer(
          http.replies,
          readReply({
            offset: "safe",
            text: "[]",
            contentType: "application/json",
            upToDate: true,
          }),
        );
        const request = yield* Queue.take(http.requests);
        const page = readReply({
          offset: "final",
          text: '["1","2"]',
          contentType: "application/json",
          closed: true,
        });
        yield* Queue.offer(http.replies, {
          ...page,
          headers: { ...page.headers, "stream-cursor": "c" },
        });
        yield* Deferred.await(entered);
        expect(yield* client.offset).toEqual(Option.some("safe"));
        expect(request.signal.aborted).toBe(false);
        yield* Deferred.succeed(release, undefined);
        expect(yield* Fiber.join(run)).toEqual([{ id: "id-1" }, { id: "id-2" }]);
        expect(yield* client.offset).toEqual(Option.some("final"));
        expect(request.signal.aborted).toBe(true);
      }),
  );

  it.effect("classifies live rejection statuses through status-filtering HTTP clients", () =>
    Effect.gen(function* () {
      const http = yield* makeReadHttp;
      for (const [status, tag] of [
        [400, "InvalidRequestError"],
        [401, "UnauthorizedError"],
        [403, "ForbiddenError"],
        [404, "StreamNotFoundError"],
        [410, "StreamGoneError"],
        [429, "RateLimitedError"],
        [503, "StreamUnavailableError"],
        [304, "ProtocolViolationError"],
      ] as const) {
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/s",
          live: "long-poll",
          backoffOptions: { maxRetries: 0 },
        });
        const run = yield* client.bytes.pipe(
          Stream.runDrain,
          Effect.flip,
          Effect.provideServiceEffect(
            HttpClient.HttpClient,
            Effect.map(HttpClient.HttpClient, HttpClient.filterStatusOk),
          ),
          Effect.provide(http.layer),
          Effect.forkChild,
        );
        yield* Queue.take(http.requests);
        yield* Queue.offer(http.replies, readReply({ offset: "safe", text: "", upToDate: true }));
        const request = yield* Queue.take(http.requests);
        yield* Queue.offer(http.replies, {
          status,
          headers: { "retry-after": "3" },
          body: Stream.empty,
        });
        const error = yield* Fiber.join(run);
        expect(error).toHaveProperty("_tag", tag);
        expect(error).toMatchObject({ response: { status } });
        expect(yield* client.offset).toEqual(Option.some("safe"));
        expect(request.signal.aborted).toBe(true);
        expect(yield* Queue.size(http.requests)).toBe(0);
      }
    }),
  );

  it.effect(
    "rejects invalid UTF-8, JSON and schema payloads without replay or checkpoint advancement",
    () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        for (const text of ["", "[", "[1]", '{"id":1}']) {
          const client = yield* DurableStreamsClient.make({
            url: "https://streams.test/s",
            live: "long-poll",
            schema: Schema.Struct({ id: Schema.Int }),
          });
          const run = yield* client.json.pipe(
            Stream.runDrain,
            Effect.flip,
            Effect.provide(http.layer),
            Effect.forkChild,
          );
          yield* Queue.take(http.requests);
          yield* Queue.offer(
            http.replies,
            readReply({
              offset: "safe",
              text: "[]",
              contentType: "application/json",
              upToDate: true,
            }),
          );
          const request = yield* Queue.take(http.requests);
          const page = readReply({
            offset: "bad",
            text,
            contentType: "application/json",
            closed: true,
          });
          yield* Queue.offer(http.replies, {
            ...page,
            headers: { ...page.headers, "stream-cursor": "c" },
          });
          expect(yield* Fiber.join(run)).toHaveProperty("_tag", "PayloadDecodeError");
          expect(yield* client.offset).toEqual(Option.some("safe"));
          expect(request.signal.aborted).toBe(true);
        }
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/s",
          live: "long-poll",
        });
        const run = yield* client.text.pipe(
          Stream.runDrain,
          Effect.flip,
          Effect.provide(http.layer),
          Effect.forkChild,
        );
        yield* Queue.take(http.requests);
        yield* Queue.offer(http.replies, readReply({ offset: "safe", text: "", upToDate: true }));
        yield* Queue.take(http.requests);
        const page = readReply({ offset: "partial", text: "", upToDate: true });
        yield* Queue.offer(http.replies, {
          ...page,
          headers: { ...page.headers, "stream-cursor": "c" },
          body: Stream.make(new Uint8Array([0xe2])),
        });
        const request = yield* Queue.take(http.requests);
        yield* Queue.offer(http.replies, _poll({ offset: "partial", closed: true }));
        expect(yield* Fiber.join(run)).toHaveProperty("_tag", "PayloadDecodeError");
        expect(yield* client.offset).toEqual(Option.some("safe"));
        expect(request.signal.aborted).toBe(true);
        expect(yield* Queue.size(http.requests)).toBe(0);
      }),
  );
  it.effect(
    "keeps catch-up cacheable, echoes opaque positions, handles open 204 and final data",
    () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/s?z=last",
          params: { a: "first" },
          live: "long-poll",
        });
        const run = yield* client.text.pipe(
          Stream.runCollect,
          Effect.provide(http.layer),
          Effect.forkChild,
        );
        const first = yield* Queue.take(http.requests);
        expect(first.request.url).toBe("https://streams.test/s?a=first&offset=-1&z=last");
        yield* Queue.offer(http.replies, readReply({ offset: "page", text: "history" }));
        const second = yield* Queue.take(http.requests);
        expect(first.signal.aborted).toBe(true);
        expect(new URL(second.request.url).searchParams.has("live")).toBe(false);
        yield* Queue.offer(http.replies, {
          ...readReply({ offset: "Opaque+:%#", text: "", upToDate: true }),
          headers: {
            "content-type": "text/plain",
            "stream-next-offset": "Opaque+:%#",
            "stream-cursor": "cursor +/?",
            "stream-up-to-date": "true",
          },
        });
        const poll = yield* Queue.take(http.requests);
        expect(second.signal.aborted).toBe(true);
        expect(poll.request.url).toBe(
          "https://streams.test/s?a=first&cursor=cursor+%2B%2F%3F&live=long-poll&offset=Opaque%2B%3A%25%23&z=last",
        );
        yield* Queue.offer(http.replies, _poll({ offset: "same-tail", cursor: "next+cursor" }));
        const next = yield* Queue.take(http.requests);
        expect(poll.signal.aborted).toBe(true);
        expect(yield* client.offset).toEqual(Option.some("same-tail"));
        expect(new URL(next.request.url).searchParams.get("cursor")).toBe("next+cursor");
        const final = readReply({ offset: "final", text: "last", closed: true });
        yield* Queue.offer(http.replies, {
          ...final,
          headers: { ...final.headers, "stream-cursor": "final-cursor" },
        });
        expect(yield* Fiber.join(run)).toEqual(["history", "last"]);
        expect(yield* client.offset).toEqual(Option.some("final"));
        expect(next.signal.aborted).toBe(true);
        expect(yield* Queue.size(http.requests)).toBe(0);
      }),
  );

  it.effect(
    "now and saved JSON reads skip bodyless polls and accept closed 204 without cursor or content type",
    () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        for (const offset of ["now", "saved+offset"]) {
          const client = yield* DurableStreamsClient.make({
            url: "https://streams.test/s",
            offset,
            live: "long-poll",
            schema: Schema.Struct({ id: Schema.Int }),
          });
          const run = yield* client.json.pipe(
            Stream.runCollect,
            Effect.provide(http.layer),
            Effect.forkChild,
          );
          expect(
            new URL((yield* Queue.take(http.requests)).request.url).searchParams.get("offset"),
          ).toBe(offset);
          yield* Queue.offer(
            http.replies,
            readReply({
              offset: "tail",
              text: "[]",
              contentType: "application/json",
              upToDate: true,
            }),
          );
          yield* Queue.take(http.requests);
          yield* Queue.offer(http.replies, _poll({ offset: "tail", cursor: "one" }));
          yield* Queue.take(http.requests);
          const data = readReply({
            offset: "next",
            text: '[{"id":1},{"id":2}]',
            contentType: "application/json",
            upToDate: true,
          });
          yield* Queue.offer(http.replies, {
            ...data,
            headers: { ...data.headers, "stream-cursor": "two" },
          });
          yield* Queue.take(http.requests);
          yield* Queue.offer(http.replies, _poll({ offset: "next", closed: true }));
          expect(yield* Fiber.join(run)).toEqual([{ id: 1 }, { id: 2 }]);
          expect(yield* client.offset).toEqual(Option.some("next"));
        }
      }),
  );

  it.effect(
    "returns to cacheable catch-up when a live response is not up to date, as the reference does",
    () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/s",
          live: "long-poll",
        });
        const run = yield* client.bytes.pipe(
          Stream.runDrain,
          Effect.provide(http.layer),
          Effect.forkChild,
        );
        yield* Queue.take(http.requests);
        yield* Queue.offer(http.replies, readReply({ offset: "tail", text: "", upToDate: true }));
        const poll = yield* Queue.take(http.requests);
        expect(new URL(poll.request.url).searchParams.get("live")).toBe("long-poll");
        const page = readReply({ offset: "more", text: "new" });
        yield* Queue.offer(http.replies, {
          ...page,
          headers: { ...page.headers, "stream-cursor": "cursor" },
        });
        const catchup = yield* Queue.take(http.requests);
        expect(new URL(catchup.request.url).searchParams.has("live")).toBe(false);
        expect(new URL(catchup.request.url).searchParams.get("cursor")).toBe("cursor");
        yield* Queue.offer(http.replies, readReply({ offset: "end", text: "", closed: true }));
        yield* Fiber.join(run);
      }),
  );

  const invalidResponses: ReadonlyArray<Pick<ReadReply, "status" | "headers">> = [
    { status: 204, headers: { "stream-next-offset": "tail", "stream-up-to-date": "true" } },
    { status: 204, headers: { "stream-next-offset": "tail", "stream-cursor": "c" } },
    {
      status: 204,
      headers: { "stream-next-offset": "now", "stream-up-to-date": "true", "stream-cursor": "c" },
    },
    {
      status: 204,
      headers: {
        "stream-next-offset": "tail",
        "stream-up-to-date": "true",
        "stream-closed": "bad",
      },
    },
    { status: 200, headers: { "stream-next-offset": "tail", "content-type": "text/plain" } },
    {
      status: 200,
      headers: {
        "stream-next-offset": "tail",
        "content-type": "text/plain",
        "stream-closed": "false",
      },
    },
  ];
  for (const invalid of invalidResponses) {
    it.effect(`rejects malformed live metadata ${Object.keys(invalid.headers).join(",")}`, () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        const client = yield* DurableStreamsClient.make({
          url: "https://user:secret@streams.test/s?token=private",
          live: "long-poll",
        });
        const run = yield* client.bytes.pipe(
          Stream.runDrain,
          Effect.flip,
          Effect.provide(http.layer),
          Effect.forkChild,
        );
        yield* Queue.take(http.requests);
        yield* Queue.offer(http.replies, readReply({ offset: "safe", text: "", upToDate: true }));
        const request = yield* Queue.take(http.requests);
        yield* Queue.offer(http.replies, {
          ...invalid,
          body: Stream.empty,
        });
        const error = yield* Fiber.join(run);
        expect(error).toHaveProperty("_tag", "ProtocolViolationError");
        expect(error).toMatchObject({
          response: { status: invalid.status, url: "https://streams.test/s" },
        });
        expect(yield* client.offset).toEqual(Option.some("safe"));
        expect(request.signal.aborted).toBe(true);
        expect(yield* Queue.size(http.requests)).toBe(0);
      }),
    );
  }

  it.effect(
    "retains strict UTF-8 across caught-up and bodyless boundaries, flushing only at EOF",
    () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/s",
          live: "long-poll",
        });
        const run = yield* client.text.pipe(
          Stream.runCollect,
          Effect.provide(http.layer),
          Effect.forkChild,
        );
        yield* Queue.take(http.requests);
        yield* Queue.offer(http.replies, {
          ...readReply({ offset: "partial", text: "", upToDate: true }),
          body: Stream.make(new Uint8Array([0xe2])),
        });
        yield* Queue.take(http.requests);
        expect(yield* client.offset).toEqual(Option.none());
        yield* Queue.offer(http.replies, _poll({ offset: "partial", cursor: "one" }));
        yield* Queue.take(http.requests);
        expect(yield* client.offset).toEqual(Option.none());
        const page = readReply({ offset: "whole", text: "", upToDate: true });
        yield* Queue.offer(http.replies, {
          ...page,
          headers: { ...page.headers, "stream-cursor": "two" },
          body: Stream.make(new Uint8Array([0x82, 0xac])),
        });
        yield* Queue.take(http.requests);
        expect(yield* client.offset).toEqual(Option.some("whole"));
        yield* Queue.offer(http.replies, _poll({ offset: "whole", closed: true }));
        expect(yield* Fiber.join(run)).toEqual(["€"]);
      }),
  );

  it.effect(
    "retries indefinitely with refreshed auth per attempt, Retry-After floors and interruption",
    () =>
      Effect.gen(function* () {
        const http = yield* makeReadHttp;
        const auth = { count: 0 };
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/s",
          live: "long-poll",
        });
        const run = yield* client.bytes.pipe(
          Stream.runDrain,
          Effect.provideServiceEffect(
            HttpClient.HttpClient,
            Effect.map(HttpClient.HttpClient, (http) =>
              http.pipe(
                HttpClient.mapRequestEffect((request) =>
                  Effect.sync(() => {
                    auth.count++;
                    return request.pipe(
                      HttpClientRequest.setHeader("authorization", `token-${auth.count}`),
                    );
                  }),
                ),
              ),
            ),
          ),
          Effect.provide(http.layer),
          Effect.forkChild,
        );
        yield* Queue.take(http.requests);
        yield* Queue.offer(http.replies, readReply({ offset: "safe", text: "", upToDate: true }));
        for (const attempt of Array.from({ length: 9 }, (_, n) => n + 2)) {
          const request = yield* Queue.take(http.requests);
          expect(request.request.headers.authorization).toBe(`token-${attempt}`);
          expect(new URL(request.request.url).searchParams.get("live")).toBe("long-poll");
          expect(new URL(request.request.url).searchParams.get("offset")).toBe("safe");
          yield* Queue.offer(http.replies, {
            status: attempt % 2 === 0 ? 429 : 503,
            headers: { "retry-after": "2" },
            body: Stream.empty,
          });
          yield* TestClock.adjust("1 second");
          expect(request.signal.aborted).toBe(true);
          expect(yield* Queue.size(http.requests)).toBe(0);
          yield* TestClock.adjust("1 second");
        }
        const blocked = yield* Queue.take(http.requests);
        yield* Fiber.interrupt(run);
        expect(Exit.hasInterrupts(yield* Fiber.await(run))).toBe(true);
        expect(blocked.signal.aborted).toBe(true);
        expect(yield* client.offset).toEqual(Option.some("safe"));
        yield* TestClock.adjust("1 hour");
        expect(yield* Queue.size(http.requests)).toBe(0);
      }),
  );

  it.effect("does not restart an exhausted finite request or replay a failed response body", () =>
    Effect.gen(function* () {
      const http = yield* makeReadHttp;
      for (const bodyFailure of [false, true]) {
        const client = yield* DurableStreamsClient.make({
          url: "https://streams.test/s",
          live: "long-poll",
          backoffOptions: { maxRetries: 0 },
        });
        const run = yield* client.bytes.pipe(
          Stream.runDrain,
          Effect.flip,
          Effect.provide(http.layer),
          Effect.forkChild,
        );
        yield* Queue.take(http.requests);
        yield* Queue.offer(http.replies, readReply({ offset: "safe", text: "", upToDate: true }));
        const request = yield* Queue.take(http.requests);
        const page = readReply({ offset: "unsafe", text: "", upToDate: true });
        yield* Queue.offer(
          http.replies,
          bodyFailure
            ? {
                ...page,
                headers: { ...page.headers, "stream-cursor": "c" },
                body: Stream.fail(
                  new HttpClientError.HttpClientError({
                    reason: new HttpClientError.TransportError({ request: request.request }),
                  }),
                ),
              }
            : { status: 503, headers: {}, body: Stream.empty },
        );
        expect(yield* Fiber.join(run)).toHaveProperty("_tag", "StreamUnavailableError");
        expect(yield* client.offset).toEqual(Option.some("safe"));
        expect(request.signal.aborted).toBe(true);
        expect(yield* Queue.size(http.requests)).toBe(0);
      }
    }),
  );

  it.effect("interruption waits for partial-body cleanup and never commits undelivered data", () =>
    Effect.gen(function* () {
      const http = yield* makeReadHttp;
      const entered = yield* Deferred.make<void>();
      const releasing = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const client = yield* DurableStreamsClient.make({
        url: "https://streams.test/s",
        live: "long-poll",
      });
      const run = yield* client.text.pipe(
        Stream.runForEach(() => Deferred.succeed(entered, undefined)),
        Effect.provide(http.layer),
        Effect.forkChild,
      );
      yield* Queue.take(http.requests);
      yield* Queue.offer(http.replies, readReply({ offset: "safe", text: "", upToDate: true }));
      const request = yield* Queue.take(http.requests);
      const page = readReply({ offset: "unsafe", text: "", upToDate: true });
      yield* Queue.offer(http.replies, {
        ...page,
        headers: { ...page.headers, "stream-cursor": "c" },
        body: Stream.make(new TextEncoder().encode("partial")).pipe(
          Stream.concat(Stream.never),
          Stream.ensuring(
            Deferred.succeed(releasing, undefined).pipe(Effect.andThen(Deferred.await(release))),
          ),
        ),
      });
      yield* Deferred.await(entered);
      const interrupt = yield* Fiber.interrupt(run).pipe(Effect.forkChild);
      yield* Deferred.await(releasing);
      expect(yield* client.offset).toEqual(Option.some("safe"));
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(interrupt);
      expect(request.signal.aborted).toBe(true);
      expect(yield* Queue.size(http.requests)).toBe(0);
    }),
  );
});
