# Error-only lint policy

`vp run check` runs Vite+ formatting, type-aware lint/type checking, and typed-lint. `vite.config.ts` inherits the Effect recommended preset and explicitly sets every rule in that preset to `error`, including inherited warning rules. `strict-effect-provide` is explicitly an error as well. Configured `no-empty-function`, `no-unused-vars`, and the suspicious category are errors; correctness was already an error. The automation config factory and rule catalog follow the same error-only policy. Diff-check uses error severities for its two added-line rules, default correctness category, and key-file change alerts.

## Narrow exceptions

- `effecttsgo/strict-effect-provide` is off only for `tests/**/*.test.ts`. Tests are application composition boundaries: they install isolated real or scripted dependencies, including deliberately distinct caller contexts used to verify batching ownership. Every other Effect rule remains enabled for tests. The conformance adapter has one local directive at its application composition root; its other code is not exempt.
- `no-underscore-dangle` is off in the root and automation factory because it directly conflicts with the required private-function underscore prefix and Effect's `_tag`. The custom `automation/private-function-prefix` rule stays an error. Oxlint does not offer a private-prefix-pattern allowance, so a redundant opposing rule is disabled rather than maintaining an identifier-by-identifier list.
- The positive-`Infinity` branch of `backoffOptions.maxRetries` has one local `schema-number` directive. The branch's existing equality filter still accepts only positive infinity; the other branch still accepts nonnegative integers. This preserves the approved unlimited retry policy without accepting `NaN` or negative infinity. The adapter's TTL and sequence numbers use `Schema.Finite` instead.

The two local directives carry `SAFETY:` explanations, which the source-comment rules permit. No Effect rules are globally disabled, no test directories are excluded, and no diff-check rule is disabled. Existing tool-source exclusions remain unchanged.

## Implementation constraints

The stream pull initializer uses `Effect.map` to intentionally return a pull Effect as data, rather than suppressing the nested-Effect diagnostic. The append operation retains its named span through `Effect.fn`; source error and requirement types remain generic. The worker context reset uses explicit `<never, never>` type arguments, not an unused parameter or unknown inference. A typed local Effect retains the source/SDK failure union. Sequence selection uses the last defined sequence, and diagnostic-byte accumulation uses a private state record with unchanged capacity, truncation, and lazy allocation behavior. Test server construction/start failures remain typed, and acquisition/release stays scoped.

Warning-free lint does not mean all external tools are silent: the current packager separately reports that TypeScript 7's compiler API is experimental.
