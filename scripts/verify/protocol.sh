#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$ROOT"

python3 "$ROOT/scripts/verify/vendor-proto.py" \
  --lock protocol/upstream-lock.json \
  --root protocol/src/main/proto
"$ROOT/scripts/dev/mvn21.sh" -B -ntp -pl protocol -am clean verify

DESCRIPTOR_SET="$ROOT/protocol/target/generated-resources/protobuf/descriptor-sets/etcd-v3.5.23.protobin"
if [[ ! -s "$DESCRIPTOR_SET" ]]; then
  echo "Protocol descriptor set was not generated: $DESCRIPTOR_SET" >&2
  exit 1
fi

echo "Verified the locked etcd wire contract and generated descriptor set."
