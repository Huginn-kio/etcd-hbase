#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
# shellcheck source=../dev/maven-repository.sh
source "$ROOT/scripts/dev/maven-repository.sh"

unset ETCD_HBASE_MAVEN_REPOSITORY

assert_parse() {
  local expected_repository="$1"
  local expected_argument="$2"
  shift 2

  m0_parse_maven_repo_local "$@"
  if [[ "$M0_MAVEN_REPOSITORY" != "$expected_repository" ]]; then
    echo "expected repository '$expected_repository', got '$M0_MAVEN_REPOSITORY'" >&2
    return 1
  fi
  if [[ "$M0_HAS_MAVEN_REPOSITORY_ARG" != "$expected_argument" ]]; then
    echo "expected argument flag '$expected_argument', got '$M0_HAS_MAVEN_REPOSITORY_ARG'" >&2
    return 1
  fi
}

assert_parse "/tmp/maven-one" true "-Dmaven.repo.local=/tmp/maven-one"
assert_parse "/tmp/maven-two" true -D "maven.repo.local=/tmp/maven-two"
assert_parse "/tmp/maven-three" true "--define=maven.repo.local=/tmp/maven-three"
assert_parse "/tmp/maven-four" true --define "maven.repo.local=/tmp/maven-four"
assert_parse "" false -B validate

ETCD_HBASE_MAVEN_REPOSITORY="/tmp/maven-env"
assert_parse "/tmp/maven-env" false -B validate
assert_parse "/tmp/maven-cli" true -D "maven.repo.local=/tmp/maven-cli"
unset ETCD_HBASE_MAVEN_REPOSITORY

if m0_parse_maven_repo_local "-Dmaven.repo.local=" >/dev/null 2>&1; then
  echo "empty -Dmaven.repo.local must be rejected" >&2
  exit 1
fi

echo "Maven local repository argument tests passed: 8 cases"
