#!/usr/bin/env bash

# Resolve the Maven local repository using the same -D spellings accepted by Maven.
m0_parse_maven_repo_local() {
  M0_MAVEN_REPOSITORY="${ETCD_HBASE_MAVEN_REPOSITORY:-}"
  M0_HAS_MAVEN_REPOSITORY_ARG=false

  local -a m0_args=("$@")
  local m0_index=0
  local m0_argument
  local m0_value
  local m0_next

  while ((m0_index < ${#m0_args[@]})); do
    m0_argument="${m0_args[m0_index]}"
    case "$m0_argument" in
      -Dmaven.repo.local=*)
        m0_value="${m0_argument#-Dmaven.repo.local=}"
        if [[ -z "$m0_value" ]]; then
          echo "maven.repo.local must not be empty" >&2
          return 2
        fi
        M0_MAVEN_REPOSITORY="$m0_value"
        M0_HAS_MAVEN_REPOSITORY_ARG=true
        ;;
      --define=maven.repo.local=*)
        m0_value="${m0_argument#--define=maven.repo.local=}"
        if [[ -z "$m0_value" ]]; then
          echo "maven.repo.local must not be empty" >&2
          return 2
        fi
        M0_MAVEN_REPOSITORY="$m0_value"
        M0_HAS_MAVEN_REPOSITORY_ARG=true
        ;;
      -D|--define)
        if ((m0_index + 1 < ${#m0_args[@]})); then
          m0_next="${m0_args[m0_index + 1]}"
          if [[ "$m0_next" == maven.repo.local=* ]]; then
            m0_value="${m0_next#*=}"
            if [[ -z "$m0_value" ]]; then
              echo "maven.repo.local must not be empty" >&2
              return 2
            fi
            M0_MAVEN_REPOSITORY="$m0_value"
            M0_HAS_MAVEN_REPOSITORY_ARG=true
            m0_index=$((m0_index + 1))
          fi
        fi
        ;;
    esac
    m0_index=$((m0_index + 1))
  done
}
