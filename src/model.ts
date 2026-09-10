import { DateTime, Duration, Option, Schema, type Stream } from "effect";
import { FieldValue } from "./headers.ts";

export const Offset = Schema.NonEmptyString.check(Schema.isPattern(/^[^\s,&=?/]+$/));
export type Offset = typeof Offset.Type;

export const LiveMode = Schema.Literals(["long-poll", "sse"]);
export type LiveMode = typeof LiveMode.Type;

export const ContentType = FieldValue.check(
  Schema.isPattern(/^[!#$%&'*+.^_`|~\w-]+\/[!#$%&'*+.^_`|~\w-]+(?:\s*;[^\r\n]*)?$/),
);
export type ContentType = typeof ContentType.Type;

export const DurableStreamsConnection = Schema.Struct({
  url: Schema.URLFromString,
  offset: Schema.optionalKey(Offset),
  live: Schema.optionalKey(LiveMode),
  contentType: Schema.optionalKey(ContentType),
  batching: Schema.optionalKey(Schema.Boolean),
  sseResilience: Schema.optionalKey(
    Schema.Struct({
      minConnectionDuration: Schema.optionalKey(
        Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)),
      ),
      maxShortConnections: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
      backoffBaseDelay: Schema.optionalKey(Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0))),
      backoffMaxDelay: Schema.optionalKey(Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0))),
      logWarnings: Schema.optionalKey(Schema.Boolean),
    }),
  ),
  headers: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  params: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  backoffOptions: Schema.optionalKey(
    Schema.Struct({
      initialDelay: Schema.optionalKey(Schema.Finite.check(Schema.isGreaterThan(0))),
      maxDelay: Schema.optionalKey(Schema.Finite.check(Schema.isGreaterThan(0))),
      multiplier: Schema.optionalKey(Schema.Finite.check(Schema.isGreaterThanOrEqualTo(1))),
      maxRetries: Schema.optionalKey(
        Schema.Union([
          Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
          // oxlint-disable-next-line effecttsgo/schema-number -- SAFETY: positive Infinity is the supported unlimited retry sentinel.
          Schema.Number.check(Schema.makeFilter((value) => value === Infinity)),
        ]),
      ),
    }).check(
      Schema.makeFilter((options) => (options.maxDelay ?? 60000) >= (options.initialDelay ?? 100)),
    ),
  ),
});
export type DurableStreamsConnection = typeof DurableStreamsConnection.Type;

export type DurableStreamsClientConfig<S extends Schema.Top = typeof Schema.Json> =
  typeof DurableStreamsConnection.Encoded & { readonly schema?: S };

export type DurableStreamsClientLayerConfig = typeof DurableStreamsConnection.Encoded & {
  readonly schema?: never;
};

export const StreamMetadata = Schema.TaggedUnion({
  Missing: {},
  Existing: {
    contentType: ContentType,
    offset: Offset,
    closed: Schema.Boolean,
    etag: Schema.optionalKey(Schema.String),
    cacheControl: Schema.optionalKey(Schema.String),
    ttlSeconds: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
    expiresAt: Schema.optionalKey(Schema.String),
  },
});
export type StreamMetadata = typeof StreamMetadata.Type;

export const StreamLifetime = Schema.TaggedUnion({
  Ttl: {
    ttlSeconds: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
    expiresAt: Schema.optionalKey(Schema.Never),
  },
  ExpiresAt: {
    expiresAt: Schema.String.check(
      Schema.isPattern(
        /^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+)?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/,
      ),
      Schema.makeFilter((value) =>
        DateTime.make(`${value.slice(0, 10)}T00:00:00Z`).pipe(
          Option.exists((date) => DateTime.formatIsoDateUtc(date) === value.slice(0, 10)),
        ),
      ),
    ),
    ttlSeconds: Schema.optionalKey(Schema.Never),
  },
});
export type StreamLifetime = typeof StreamLifetime.Type;

export const CreateOptions = Schema.Struct({
  contentType: Schema.optionalKey(ContentType),
  lifetime: Schema.optionalKey(StreamLifetime),
  closed: Schema.optionalKey(Schema.Boolean),
});
export type CreateInput<A> = typeof CreateOptions.Type &
  (
    | { readonly value: A; readonly values?: never }
    | { readonly values: ReadonlyArray<A>; readonly value?: never }
    | { readonly value?: never; readonly values?: never }
  );
export type AppendInput<A> = { readonly value: A; readonly seq?: string };
export type AppendStreamInput<E = never, R = never> = {
  readonly source: Stream.Stream<Uint8Array | string, E, R>;
  readonly seq?: string;
};
export type CloseInput<A> = { readonly value?: A; readonly seq?: string };

export const AppendResult = Schema.Struct({ offset: Offset, closed: Schema.Boolean });
export type AppendResult = typeof AppendResult.Type;
export const CloseResult = Schema.Struct({ finalOffset: Offset });
export type CloseResult = typeof CloseResult.Type;
export const CreateResult = Schema.Struct({
  status: Schema.Literals([200, 201]),
  contentType: ContentType,
  offset: Offset,
  closed: Schema.Boolean,
});
export type CreateResult = typeof CreateResult.Type;

export const ProducerOptions = Schema.Struct({
  producerId: FieldValue.check(Schema.isMinLength(1)),
  epoch: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
  autoClaim: Schema.optionalKey(Schema.Boolean),
  maxBatchBytes: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  linger: Schema.optionalKey(
    Schema.Duration.check(
      Schema.makeFilter(
        (value) => Number.isFinite(Duration.toMillis(value)) && Duration.toMillis(value) >= 0,
      ),
    ),
  ),
  maxInFlight: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
});
export type ProducerOptions = typeof ProducerOptions.Type;
export type ProducerAppendInput<A> = { readonly value: A };
export type ProducerCloseInput<A> = { readonly value?: A };
export const ProducerAppendResult = Schema.Struct({
  offset: Schema.optionalKey(Offset),
  duplicate: Schema.Boolean,
  producerSeq: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
});
export type ProducerAppendResult = typeof ProducerAppendResult.Type;
