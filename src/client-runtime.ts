import { Context, Effect, Exit, Layer, Predicate, Schema, Scope } from "effect";
import {
  FetchHttpClient,
  HttpClient,
  HttpClientError,
  HttpClientRequest,
} from "effect/unstable/http";
import { DurableStreamsConnection } from "./model";
import { FieldValue } from "./headers";
import { InvalidDurableStreamsConfigError } from "./errors";
import { checkExtensions, RequestMetadataFailure } from "./request";
import {
  AbortError,
  InvalidClientOptionsError,
  mapClientErrors,
  unwrapClientExit,
  type NativeClientError,
} from "./client-errors";

export type MaybePromise<A> = A | Promise<A>;
export type HeadersRecord = Readonly<Record<string, string | (() => MaybePromise<string>)>>;
export type ParamsRecord = Readonly<
  Record<string, string | (() => MaybePromise<string>) | undefined>
>;
export type TransportOptions = {
  readonly fetch?: typeof globalThis.fetch;
  readonly headers?: HeadersRecord;
  readonly params?: ParamsRecord;
  readonly signal?: AbortSignal;
};

const fetchContext = Effect.runSync(Effect.scoped(Layer.build(FetchHttpClient.layer)));

export const runClientSync = <A>(effect: Effect.Effect<A, NativeClientError>) =>
  unwrapClientExit(Effect.runSyncExit(mapClientErrors(effect)));

export const parseClientConnection = (input: typeof DurableStreamsConnection.Encoded) =>
  runClientSync(
    Schema.decodeEffect(DurableStreamsConnection)(input).pipe(
      Effect.mapError(() => new RequestMetadataFailure()),
      Effect.flatMap((connection) => checkExtensions(connection).pipe(Effect.as(connection))),
      Effect.catchTag("RequestMetadataFailure", () =>
        Effect.fail(
          new InvalidDurableStreamsConfigError({
            field: "connection",
            issues: ["Invalid options"],
          }),
        ),
      ),
      Effect.flatMap((connection) =>
        connection.url.protocol === "http:" || connection.url.protocol === "https:"
          ? Effect.succeed(connection)
          : Effect.fail(
              new InvalidDurableStreamsConfigError({
                field: "url",
                issues: ["Expected HTTP or HTTPS"],
              }),
            ),
      ),
    ),
  );

export const mergeHeaders = (
  base: HeadersRecord | undefined,
  override: HeadersRecord | undefined,
) =>
  Object.fromEntries(
    [...Object.entries(base ?? {}), ...Object.entries(override ?? {})].map(([key, value]) => [
      key.toLowerCase(),
      value,
    ]),
  );

export const validateTransportOptions = (input: TransportOptions & { readonly url: string }) => {
  validateClientSignal(input.signal);
  if (Object.hasOwn(input, "warnOnHttp")) throw new InvalidClientOptionsError();
  if (Predicate.hasProperty(input, "backoffOptions") && input.backoffOptions !== undefined) {
    if (
      Predicate.hasProperty(input.backoffOptions, "debug") ||
      Predicate.hasProperty(input.backoffOptions, "onFailedAttempt")
    )
      throw new InvalidClientOptionsError();
  }
  if (input.fetch !== undefined && !Predicate.isFunction(input.fetch))
    throw new InvalidClientOptionsError();
  parseClientConnection({
    url: input.url,
    headers: Object.fromEntries(Object.keys(input.headers ?? {}).map((key) => [key, ""])),
    params: Object.fromEntries(
      Object.entries(input.params ?? {})
        .filter(([, value]) => value !== undefined)
        .map(([key]) => [key, ""]),
    ),
  });
};

export const validateClientSignal = (signal: AbortSignal | undefined) => {
  if (signal !== undefined && !Schema.is(Schema.instanceOf(AbortSignal))(signal))
    throw new InvalidClientOptionsError();
};

export type ClientOwner = {
  readonly controller: AbortController;
  readonly scopes: Set<Scope.Closeable>;
  readonly pending: Set<Promise<void>>;
};
export const createClientRuntime = (options: TransportOptions) =>
  createBoundRuntime({ options, context: fetchContext, defaultFetch: true });
export const createBoundRuntime = (input: {
  readonly options: TransportOptions;
  readonly context: Context.Context<HttpClient.HttpClient>;
  readonly owner?: ClientOwner;
  readonly defaultFetch?: boolean;
}) => {
  const options = input.options;
  const parentSignal: AbortSignal | undefined = input.owner?.controller.signal;
  const cleanups = new Set<Promise<void>>();
  const closing = new WeakMap<Scope.Closeable, Promise<void>>();
  const releaseBody = (response: Response) => {
    const pending = (
      response.body !== null && !response.body.locked && !response.bodyUsed
        ? response.body.cancel()
        : Promise.resolve()
    ).catch((error) => {
      if (!Predicate.isError(error) || error.name !== "AbortError")
        Effect.runSync(Effect.logWarning("Response body cleanup failed"));
    });
    cleanups.add(pending);
    void pending.then(() => cleanups.delete(pending));
    return pending;
  };
  const base = Context.get(input.context, HttpClient.HttpClient);
  const http = base.pipe(
    HttpClient.mapRequestEffect((request) =>
      Effect.gen(function* () {
        const headers = yield* Effect.forEach(
          Object.entries(options.headers ?? {}),
          ([key, value]) =>
            Effect.tryPromise({
              try: async () => [key, Predicate.isFunction(value) ? await value() : value] as const,
              catch: () => new RequestMetadataFailure(),
            }),
        );
        const params = yield* Effect.forEach(Object.entries(options.params ?? {}), ([key, value]) =>
          Effect.tryPromise({
            try: async () => [key, Predicate.isFunction(value) ? await value() : value] as const,
            catch: () => new RequestMetadataFailure(),
          }),
        );
        if (
          headers.some(([, value]) => !Schema.is(FieldValue)(value)) ||
          params.some(([, value]) => value !== undefined && !Predicate.isString(value))
        )
          return yield* new RequestMetadataFailure();
        const url = new URL(request.url);
        for (const [key, value] of params)
          if (value !== undefined) url.searchParams.set(key, value);
        url.searchParams.sort();
        return request.pipe(
          HttpClientRequest.setUrl(url),
          HttpClientRequest.setHeaders(Object.fromEntries(headers)),
        );
      }).pipe(
        Effect.tapError(() => Effect.logWarning("Request metadata callback failed")),
        Effect.mapError(
          (cause) =>
            new HttpClientError.HttpClientError({
              reason: new HttpClientError.EncodeError({ request, cause }),
            }),
        ),
      ),
    ),
  );
  const fetchClient: typeof globalThis.fetch = async (input, init) => {
    if (init?.signal?.aborted) throw new AbortError();
    const response = await (options.fetch ?? globalThis.fetch)(input, init);
    if (init?.signal?.aborted) {
      await releaseBody(response);
      throw new AbortError();
    }
    init?.signal?.addEventListener(
      "abort",
      () => {
        void releaseBody(response);
      },
      { once: true },
    );
    return response;
  };
  const ambient = Context.add(input.context, HttpClient.HttpClient, http);
  const context =
    input.defaultFetch === true
      ? Context.add(ambient, FetchHttpClient.Fetch, fetchClient)
      : ambient;
  return {
    context,
    signal: parentSignal,
    own: (scope: Scope.Closeable) => {
      input.owner?.scopes.add(scope);
    },
    run: <A>(
      effect: Effect.Effect<A, NativeClientError, HttpClient.HttpClient>,
      signal?: AbortSignal,
    ) => {
      validateClientSignal(signal);
      const combined =
        input.owner === undefined
          ? signal
          : AbortSignal.any(
              [input.owner.controller.signal, signal].filter((value) => value !== undefined),
            );
      if (combined?.aborted) return Promise.reject<A>(new AbortError());
      const result = Effect.runPromiseExitWith(context)(mapClientErrors(effect), {
        signal: combined,
      }).then(async (exit) => {
        await Promise.all(cleanups);
        return unwrapClientExit(exit);
      });
      const observed = result.then(
        () => undefined,
        () => undefined,
      );
      input.owner?.pending.add(observed);
      void observed.then(() => input.owner?.pending.delete(observed));
      return result;
    },
    closeScope: (scope: Scope.Closeable) => {
      const existing = closing.get(scope);
      if (existing !== undefined) return existing;
      const pending = Effect.runPromiseExit(Scope.close(scope, Exit.void)).then(async () => {
        await Promise.all(cleanups);
        input.owner?.scopes.delete(scope);
      });
      closing.set(scope, pending);
      input.owner?.pending.add(pending);
      void pending.then(() => input.owner?.pending.delete(pending));
      return pending;
    },
  };
};
