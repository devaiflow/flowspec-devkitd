default:
    @just --list

# Run all tests across the workspace.
test:
    cargo test --workspace

# Check formatting, clippy, and the domain-layering invariant.
lint:
    cargo fmt -- --check
    cargo clippy --workspace -- -D warnings
    ./scripts/check-layering.sh

# Run the server binary.
run:
    cargo run --bin flowspec-server

# Format all Rust source files.
fmt:
    cargo fmt

# Run the Phase 4 tracer against a live devkitd (docs/devkitd-dev.md).
tracer:
    cargo test -p flowspec-server --test tracer -- --ignored --nocapture

# Run the live create-feature -> containers-up -> opencode -> claude chain
# against a live devkitd (docs/devkitd-dev.md). Real side effects.
chain:
    cargo test -p flowspec-server --test devkit_chain -- --ignored --nocapture
