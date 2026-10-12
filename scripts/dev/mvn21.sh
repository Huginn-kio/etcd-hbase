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
python3 "$ROOT/scripts/verify/test_maven_lock.py"
MAVEN_REPOSITORY="${ETCD_HBASE_MAVEN_REPOSITORY:-}"
HAS_REPOSITORY_ARG=false
for ARG in "$@"; do
  case "$ARG" in
    -Dmaven.repo.local=*)
      MAVEN_REPOSITORY="${ARG#*=}"
      HAS_REPOSITORY_ARG=true
      ;;
  esac
done
if [[ -n "$MAVEN_REPOSITORY" ]]; then
  python3 "$ROOT/scripts/verify/maven-lock.py" --mode preflight --repository "$MAVEN_REPOSITORY"
else
  python3 "$ROOT/scripts/verify/maven-lock.py" --mode preflight
fi

if [[ -n "$MAVEN_REPOSITORY" && "$HAS_REPOSITORY_ARG" == false ]]; then
  if "$ROOT/mvnw" \
    --toolchains "$ROOT/.mvn/toolchains.xml" \
    --settings "$ROOT/build/maven-settings-public.xml" \
    "$@" "-Dmaven.repo.local=$MAVEN_REPOSITORY"; then
    python3 "$ROOT/scripts/verify/maven-lock.py" --mode artifacts --repository "$MAVEN_REPOSITORY"
  else
    STATUS=$?
    exit "$STATUS"
  fi
elif [[ -n "$MAVEN_REPOSITORY" ]]; then
  if "$ROOT/mvnw" \
    --toolchains "$ROOT/.mvn/toolchains.xml" \
    --settings "$ROOT/build/maven-settings-public.xml" \
    "$@"; then
    python3 "$ROOT/scripts/verify/maven-lock.py" --mode artifacts --repository "$MAVEN_REPOSITORY"
  else
    STATUS=$?
    exit "$STATUS"
  fi
else
  if "$ROOT/mvnw" \
    --toolchains "$ROOT/.mvn/toolchains.xml" \
    --settings "$ROOT/build/maven-settings-public.xml" \
    "$@"; then
    python3 "$ROOT/scripts/verify/maven-lock.py" --mode artifacts
  else
    STATUS=$?
    exit "$STATUS"
  fi
fi
