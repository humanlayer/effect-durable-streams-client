# Vendored added-line policy

Vendored from [`typeonce-dev/ai-automation`](https://github.com/typeonce-dev/ai-automation) at commit `0bca096fe6fe9878cd15303a623dd2cd85915ddd`. It exposes both added-line rules and the configured `diff/key-file-change` review alert.

Local policy promotes both added-line rules, default correctness diagnostics, and key-file change alerts to errors. The runner accepts Oxlint exit 1 as diagnostic output, then filters diagnostics to added lines before choosing its own exit status. It still rejects process/configuration failures. This matters because an unchanged explicit return type elsewhere in a changed file must not crash an added-line check. Regression tests cover successful output, error output and line filtering, and execution failures.
