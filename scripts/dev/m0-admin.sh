#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
JDK_HOME="${ETCD_HBASE_JDK21_HOME:-}"
if [[ -z "$JDK_HOME" ]]; then
  printf '%s\n' 'Set ETCD_HBASE_JDK21_HOME to the pinned Temurin JDK 21 home.' >&2
  exit 2
fi
export JAVA_HOME="$(cd "$JDK_HOME" && pwd -P)"

# Prepare packaged reactor artifacts; build output must not become part of the plan on stdout.
"$REPO_ROOT/scripts/dev/mvn21.sh" -B -ntp -pl admin -am package >&2
ADMIN_JAR="$REPO_ROOT/admin/target/admin-0.1.0-SNAPSHOT.jar"
CLASSPATH_FILE="$REPO_ROOT/admin/target/j05-runtime-classpath.txt"
if [[ ! -s "$ADMIN_JAR" || ! -s "$CLASSPATH_FILE" ]]; then
  printf '%s\n' 'Packaged admin runtime or its dependency classpath is missing.' >&2
  exit 1
fi
DEPENDENCIES="$(<"$CLASSPATH_FILE")"
if [[ "$DEPENDENCIES" == *'/target/classes'* || "$DEPENDENCIES" == *'/target/test-classes'* \
   || "$DEPENDENCIES" == *'-tests.jar'* || "$DEPENDENCIES" == *'protobuf-java-2.5.0.jar'* ]]; then
  printf '%s\n' 'Admin runtime classpath contains development, fixture or legacy server artifacts.' >&2
  exit 1
fi
for MODULE in core spi hbase; do
  if [[ "$DEPENDENCIES" != *"/$MODULE-0.1.0-SNAPSHOT.jar"* ]]; then
    printf 'Admin runtime is missing a packaged module: %s\n' "$MODULE" >&2
    exit 1
  fi
done
exec "$JAVA_HOME/bin/java" -cp "$ADMIN_JAR:$DEPENDENCIES" io.etcdhbase.admin.InitializeTablesCommand "$@"
