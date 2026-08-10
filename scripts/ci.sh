#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> cargo fmt --check"
cargo fmt -- --check

echo "==> cargo clippy --workspace -- -D warnings"
cargo clippy --workspace -- -D warnings

echo "==> check-layering.sh"
./scripts/check-layering.sh

echo "==> cargo test --workspace"
cargo test --workspace

echo "==> CI passed"
