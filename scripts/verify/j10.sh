#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$ROOT"
scripts/dev/mvn21.sh -B -ntp -pl hbase -am clean verify
python3 - <<'PY'
from pathlib import Path
import sys

sys.path.insert(0, str(Path("scripts/verify").resolve()))
from evidence import java_reports

required = {"io.etcdhbase.core.bytes.ByteKeyTest", "io.etcdhbase.core.bytes.KeyRangeTest",
            "io.etcdhbase.hbase.codec.RowKeyCodecTest", "io.etcdhbase.hbase.codec.StorageMessagesTest"}
java_reports(Path.cwd(), required, ("spi", "core", "hbase"))
print("J10 byte, row-key and storage message contracts passed without skips")
PY
git diff --check
