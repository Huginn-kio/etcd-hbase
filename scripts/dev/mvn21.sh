#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
JDK_HOME="${ETCD_HBASE_JDK21_HOME:-}"

if [[ -z "$JDK_HOME" ]]; then
  echo "Set ETCD_HBASE_JDK21_HOME to the pinned Temurin JDK 21 home." >&2
  exit 2
fi

JDK_HOME="$(cd "$JDK_HOME" && pwd -P)"
export JAVA_HOME="$JDK_HOME"
export PATH="$JAVA_HOME/bin:$PATH"

python3 "$ROOT/scripts/dev/configure-toolchain.py" --java-home "$JAVA_HOME"
exec "$ROOT/mvnw" \
  --toolchains "$ROOT/.mvn/toolchains.xml" \
  --settings "$ROOT/build/maven-settings-public.xml" \
  "$@"
