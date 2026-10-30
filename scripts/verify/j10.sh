#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$ROOT"
scripts/dev/mvn21.sh -B -ntp -pl hbase -am clean verify
python3 - <<'PY'
from pathlib import Path
import xml.etree.ElementTree as ET

required = {"ByteKeyTest", "KeyRangeTest", "RowKeyCodecTest", "StorageMessagesTest"}
observed = set()
for module in ("spi", "core", "hbase"):
    for directory in ("surefire-reports", "failsafe-reports"):
        for path in Path(module, "target", directory).glob("TEST-*.xml"):
            suite = ET.parse(path).getroot()
            if int(suite.get("tests", "0")) == 0 or any(int(suite.get(key, "0")) for key in ("failures", "errors", "skipped")):
                raise SystemExit("suite did not execute successfully: " + path.name)
            observed.add(suite.get("name", "").rsplit(".", 1)[-1])
if not required <= observed:
    raise SystemExit("missing J10 contract suites")
print("J10 byte, row-key and storage message contracts passed without skips")
PY
git diff --check
