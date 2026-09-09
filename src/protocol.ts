import {
  Data,
  DateTime,
  Effect,
  Option,
  Predicate,
  Record,
  Schema,
  type SchemaIssue,
} from "effect";
import { ContentType, Offset, StreamMetadata } from "./model.ts";

export const NEXT_OFFSET = "stream-next-offset";
export const CLOSED = "stream-closed";
export const TTL = "stream-ttl";
export const EXPIRES_AT = "stream-expires-at";

export const WriteHeaders = Schema.Struct({
  "stream-next-offset": Offset.check(Schema.isPattern(/^(?!-1$|now$)/)),
  "stream-closed": Schema.optionalKey(Schema.Literals(["true", "false"])),
});

export class HeadMetadataFailure extends Data.TaggedError("HeadMetadataFailure")<{
  readonly component: string;
  readonly cause: Schema.SchemaError | SchemaIssue.Issue;
}> {}

const HeadHeaders = Schema.Struct({
  "content-type": ContentType,
  "stream-next-offset": Offset.check(Schema.isPattern(/^(?!-1$|now$)/)),
  "stream-closed": Schema.optionalKey(Schema.Literals(["true", "false"])),
  "stream-ttl": Schema.optionalKey(Schema.String.check(Schema.isPattern(/^(0|[1-9]\d*)$/))),
  "stream-expires-at": Schema.optionalKey(
    Schema.String.check(
      Schema.isPattern(
        /^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+)?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/,
      ),
      Schema.makeFilter((value) =>
        DateTime.make(`${value.slice(0, 10)}T00:00:00Z`).pipe(
          Option.exists((date) => DateTime.formatIsoDateUtc(date) === value.slice(0, 10)),
        ),
      ),
    ),
  ),
  etag: Schema.optionalKey(Schema.String),
  "cache-control": Schema.optionalKey(Schema.String),
});

export type ParseHeadInput = {
  readonly headers: Readonly<Record<string, string>>;
  readonly requiresJson?: boolean;
};

export const parseHeadMetadata = (input: ParseHeadInput) =>
  Effect.gen(function* () {
    const headers = yield* Schema.decodeUnknownEffect(HeadHeaders)(input.headers);
    if (input.requiresJson) {
      yield* Schema.decodeEffect(
        ContentType.check(
          Schema.makeFilter(
            (value) => value.split(";")[0]?.trim().toLowerCase() === "application/json",
          ),
        ),
      )(headers["content-type"]);
    }
    if (headers[EXPIRES_AT] !== undefined) {
      yield* Schema.decodeEffect(Schema.DateTimeUtcFromString)(headers[EXPIRES_AT]);
    }
    return yield* StreamMetadata.cases.Existing.makeEffect({
      contentType: headers["content-type"],
      offset: headers[NEXT_OFFSET],
      closed: headers[CLOSED] === "true",
      ...Record.filter(
        {
          etag: headers.etag,
          cacheControl: headers["cache-control"],
          ttlSeconds: headers[TTL] !== undefined ? Number(headers[TTL]) : undefined,
          expiresAt: headers[EXPIRES_AT],
        },
        Predicate.isNotUndefined,
      ),
    });
  }).pipe(
    Effect.mapError((cause) => new HeadMetadataFailure({ component: "HEAD headers", cause })),
  );
