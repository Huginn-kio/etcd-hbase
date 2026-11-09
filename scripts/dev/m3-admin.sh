#!/usr/bin/env bash
set -euo pipefail
exec "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)/admin-command.sh" \
  io.etcdhbase.admin.M3InitializeTablesCommand "$@"
