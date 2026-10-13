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
"$ROOT/scripts/verify/test_mvn21_args.sh"
# shellcheck source=./maven-repository.sh
source "$ROOT/scripts/dev/maven-repository.sh"
m0_parse_maven_repo_local "$@"
MAVEN_REPOSITORY="$M0_MAVEN_REPOSITORY"
HAS_REPOSITORY_ARG="$M0_HAS_MAVEN_REPOSITORY_ARG"
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
