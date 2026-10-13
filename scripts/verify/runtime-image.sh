#!/usr/bin/env bash
set -euo pipefail
umask 077

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
ARTIFACT_ROOT="$ROOT/.artifacts/m0/runtime-image/$(date -u +%Y%m%dT%H%M%SZ)-$$"

if [[ $# -gt 2 ]]; then
  echo "usage: scripts/verify/runtime-image.sh [--artifact-root PATH]" >&2
  exit 2
fi
if [[ $# -gt 0 ]]; then
  if [[ "$#" -ne 2 || "$1" != "--artifact-root" ]]; then
    echo "usage: scripts/verify/runtime-image.sh [--artifact-root PATH]" >&2
    exit 2
  fi
  ARTIFACT_ROOT="$2"
fi
[[ "$ARTIFACT_ROOT" = /* ]] || ARTIFACT_ROOT="$ROOT/$ARTIFACT_ROOT"
command -v docker >/dev/null 2>&1 || { echo "Docker is required for runtime image verification" >&2; exit 2; }
docker info >/dev/null
mkdir -p "$ARTIFACT_ROOT"

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
IMAGE="etcd-hbase-m0-runtime:$RUN_ID"
CONTAINER="etcd-hbase-m0-runtime-$RUN_ID"
cleanup() {
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  docker image rm "$IMAGE" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if ! docker build \
  --file "$ROOT/server/src/main/docker/Dockerfile.m0" \
  --tag "$IMAGE" \
  "$ROOT" >"$ARTIFACT_ROOT/docker-build.log" 2>&1; then
  cat "$ARTIFACT_ROOT/docker-build.log" >&2
  exit 1
fi

if ! docker run --rm --entrypoint java "$IMAGE" \
  -XshowSettings:properties -version >"$ARTIFACT_ROOT/java-version.log" 2>&1; then
  cat "$ARTIFACT_ROOT/java-version.log" >&2
  echo "could not read the packaged runtime Java version" >&2
  exit 1
fi
python3 - "$ROOT" "$ARTIFACT_ROOT/java-version.log" <<'PY'
import json
import re
import sys
from pathlib import Path

root = Path(sys.argv[1])
output = Path(sys.argv[2]).read_text(encoding="utf-8")
expected = json.loads((root / "build/dependency-lock.json").read_text(encoding="utf-8"))["jdk"]["release"]
specification = re.search(r"^\s*java\.specification\.version = (\S+)$", output, re.MULTILINE)
runtime = re.search(r"^\s*java\.runtime\.version = (\S+)$", output, re.MULTILINE)
if specification is None or specification.group(1) != "21":
    raise SystemExit("packaged runtime image must report java.specification.version=21")
if runtime is None or runtime.group(1) not in {expected, f"{expected}-LTS"}:
    raise SystemExit(f"packaged runtime image must match locked Java release {expected}")
PY

"$ROOT/scripts/dev/go-m0.sh" -C testkit/go build \
  -trimpath -o "$ARTIFACT_ROOT/m0driver" ./cmd/m0driver

docker run --detach --name "$CONTAINER" \
  --publish 127.0.0.1::2379 "$IMAGE" >"$ARTIFACT_ROOT/container-id.txt"
PORT_MAPPING="$(docker port "$CONTAINER" 2379/tcp)"
HOST_PORT="${PORT_MAPPING##*:}"
if [[ ! "$HOST_PORT" =~ ^[0-9]+$ ]]; then
  echo "could not resolve the loopback-only runtime image port mapping: $PORT_MAPPING" >&2
  exit 1
fi

python3 - "$HOST_PORT" <<'PY'
import socket
import sys
import time

port = int(sys.argv[1])
deadline = time.monotonic() + 30
while time.monotonic() < deadline:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.5):
            break
    except OSError:
        time.sleep(0.2)
else:
    raise SystemExit("packaged gRPC server did not open its published loopback port")
PY

set +e
"$ARTIFACT_ROOT/m0driver" \
  --endpoint "127.0.0.1:$HOST_PORT" \
  --case m0-runtime-image-skeleton \
  --trace "$ARTIFACT_ROOT/runtime.jsonl" \
  --timeout 2s >"$ARTIFACT_ROOT/m0driver.log" 2>&1
DRIVER_STATUS=$?
set -e
if [[ "$DRIVER_STATUS" -ne 1 ]]; then
  cat "$ARTIFACT_ROOT/m0driver.log" >&2
  echo "packaged skeleton probe returned $DRIVER_STATUS, expected explicit UNIMPLEMENTED" >&2
  exit 1
fi

if ! python3 - "$ARTIFACT_ROOT/runtime.jsonl" <<'PY'
import json
import sys
from pathlib import Path

records = [
    json.loads(line)
    for line in Path(sys.argv[1]).read_text(encoding="utf-8").splitlines()
    if line.strip()
]
if len(records) != 1:
    raise SystemExit(f"runtime probe expected one trace record, got {len(records)}")
record = records[0]
if record.get("case_id") != "m0-runtime-image-skeleton":
    raise SystemExit("runtime image trace has the wrong case id")
if record.get("evidence_scope") != "transport":
    raise SystemExit("runtime image trace has the wrong evidence scope")
if record.get("error", {}).get("grpc_code") != "Unimplemented":
    raise SystemExit("packaged skeleton did not return gRPC UNIMPLEMENTED")
PY
then
  echo "m0driver output:" >&2
  cat "$ARTIFACT_ROOT/m0driver.log" >&2
  echo "runtime trace:" >&2
  cat "$ARTIFACT_ROOT/runtime.jsonl" >&2
  echo "container log:" >&2
  docker logs "$CONTAINER" >&2 || true
  exit 1
fi

docker stop --time 10 "$CONTAINER" >"$ARTIFACT_ROOT/container-stop.log"
CONTAINER_EXIT="$(docker inspect --format '{{.State.ExitCode}}' "$CONTAINER")"
if [[ "$CONTAINER_EXIT" -ne 0 ]]; then
  echo "packaged server did not stop cleanly (exit=$CONTAINER_EXIT)" >&2
  exit 1
fi

echo "runtime image passed: Java 21 package started, returned UNIMPLEMENTED, and shut down cleanly; artifacts=$ARTIFACT_ROOT"
