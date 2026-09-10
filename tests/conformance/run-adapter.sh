#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
exec node --import tsx tests/conformance/adapter.ts
