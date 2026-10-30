#!/bin/sh
set -eu
RUNTIME_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
IFS= read -r RUNTIME_CLASSPATH < "$RUNTIME_DIR/classpath.txt"
JAVA_BIN="${JAVA_HOME:+$JAVA_HOME/bin/}java"
cd "$RUNTIME_DIR"
if [ "$#" -eq 0 ]; then set -- --config "$RUNTIME_DIR/server.json"; fi
exec "$JAVA_BIN" -XX:MaxRAMPercentage=50.0 -XX:+ExitOnOutOfMemoryError \
  -cp "$RUNTIME_CLASSPATH" io.etcdhbase.server.RuntimeServer "$@"
