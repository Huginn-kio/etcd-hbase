#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
if [[ "${1:-}" == -C ]]; then exec "$SCRIPT_DIR/go-j03.sh" "$@"; fi
exec "$SCRIPT_DIR/go-j03.sh" -C "$SCRIPT_DIR/../../testkit/go" "$@"
