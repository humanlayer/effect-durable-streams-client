# Vendored AI Automation Oxlint plugin

This directory vendors the Oxlint runtime plugin source from
[`typeonce-dev/ai-automation`](https://github.com/typeonce-dev/ai-automation) at commit
[`0bca096fe6fe9878cd15303a623dd2cd85915ddd`](https://github.com/typeonce-dev/ai-automation/commit/0bca096fe6fe9878cd15303a623dd2cd85915ddd).

The local `config.ts` and catalog apply this project's error-only severity policy; the plugin entry point, profiles, and rule implementations retain the vendored behavior. All catalog rules, built-in rules, and enabled categories are errors rather than warnings.

`createConfig` applies `documentedDisabledRules`: `no-underscore-dangle` is off because it contradicts the required `private-function-prefix` convention and Effect's `_tag` discriminator. Oxlint's allow-list is identifier-specific, not a private-prefix pattern. The project keeps the custom private-function rule enabled instead of maintaining a growing list of individual private names. The root `vite.config.ts` applies the same exception.

`prefer-tagged-error-handling` is vendored from the Fold automation plugin at commit `f5e85a99f95d9e10dd61e32c3ff997a48c65aa90`.

The Promise facade modules (`src/async-await.ts`, `src/client-runtime.ts`, `src/client-response.ts`, and `src/client-errors.ts`) explicitly use ordinary, non-underscore helper and binding names. Their top-level helpers remain module-private by not being exported; class internals use TypeScript `private` (and `private readonly` where applicable), not JavaScript `#` fields. TypeScript privacy is compile-time only, not runtime enforcement. A file-scoped `automation/private-function-prefix` override in `vite.config.ts` permits this convention without changing the native Effect modules' naming policy or Effect's `_tag` protocol discriminator.
