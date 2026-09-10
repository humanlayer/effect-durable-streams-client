import { Schema } from "effect";

export const FieldValue = Schema.String.check(Schema.isPattern(/^[\t\x20-\x7e\x80-\xff]*$/));
