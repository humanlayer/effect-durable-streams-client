import { Schema } from "effect";

export const Offset = Schema.NonEmptyString.check(Schema.isPattern(/^[^\s,&=?/]+$/));
export type Offset = typeof Offset.Type;

export const LiveMode = Schema.Literals(["long-poll", "sse"]);
export type LiveMode = typeof LiveMode.Type;

export const ContentType = Schema.String.check(
  Schema.isPattern(/^[!#$%&'*+.^_`|~\w-]+\/[!#$%&'*+.^_`|~\w-]+(?:\s*;[^\r\n]*)?$/),
);
export type ContentType = typeof ContentType.Type;

export const DurableStreamsConnection = Schema.Struct({
  url: Schema.URLFromString,
  offset: Schema.optionalKey(Offset),
  live: Schema.optionalKey(LiveMode),
  contentType: Schema.optionalKey(ContentType),
  batching: Schema.optionalKey(Schema.Boolean),
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
