#!/usr/bin/env bash
set -euo pipefail

# The domain crate must not depend on tokio (or any async/IO crate).
# `cargo tree -i tokio -p flowspec-domain` is expected to fail.
if cargo tree -i tokio -p flowspec-domain >/dev/null 2>&1; then
  echo "ERROR: flowspec-domain depends on tokio; dependency rule violated" >&2
  exit 1
fi

echo "OK: flowspec-domain has no tokio dependency"

# The application crate must not depend on rusqlite (the SQLite adapter lives
# in flowspec-server, not the orchestration layer). `cargo tree -i rusqlite -p
# flowspec-app` is expected to fail.
if cargo tree -i rusqlite -p flowspec-app >/dev/null 2>&1; then
  echo "ERROR: flowspec-app depends on rusqlite; dependency rule violated" >&2
  exit 1
fi

echo "OK: flowspec-app has no rusqlite dependency"

# The application crate must not depend on rmcp (the MCP client adapter lives
# in flowspec-server, not the orchestration layer). `cargo tree -i rmcp -p
# flowspec-app` is expected to fail.
if cargo tree -i rmcp -p flowspec-app >/dev/null 2>&1; then
  echo "ERROR: flowspec-app depends on rmcp; dependency rule violated" >&2
  exit 1
fi

echo "OK: flowspec-app has no rmcp dependency"

# The application crate must not depend on reqwest (the platform connector
# adapter lives in flowspec-server, not the orchestration layer).
# `cargo tree -i reqwest -p flowspec-app` is expected to fail.
if cargo tree -i reqwest -p flowspec-app >/dev/null 2>&1; then
  echo "ERROR: flowspec-app depends on reqwest; dependency rule violated" >&2
  exit 1
fi

echo "OK: flowspec-app has no reqwest dependency"
