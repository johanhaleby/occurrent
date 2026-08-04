#!/usr/bin/env bash
# Fails when a test builds a MongoDB container the old way. Two patterns, both of which used to be
# everywhere and both of which are now bugs:
#
#   1. A fixed host port binding, "27017:27017". 69 test classes pinned it, which meant two Maven
#      runs on one machine could not coexist, one of them had to move to 27018 to get any parallelism
#      at all, and a locally installed mongod blocked the suite outright.
#
#   2. "mongo:" + System.getProperty("test.mongo.version"). Surefire supplies that property, so an IDE
#      run built the image name "mongo:null" and failed with a pull error rather than saying what was
#      wrong. ReplicaSetReadyMongoDBContainer.withDefaultVersion() falls back to the version the build
#      filtered into occurrent-test-support.properties.
#
# Both are what issue #505 fixed, and both are the kind of thing that grows back one test class at a
# time: the pattern reached 171 files by being copied from the neighbouring test. See ADR 94.
#
# Scoped to **/src/test/** on purpose. That exempts test-support/src/main, where the one legitimate
# read of test.mongo.version lives, and it keeps the check off production sources that have no
# business mentioning either pattern anyway.
#
# Deliberately no allow-list, same reasoning as check-ci-runnable-tests.sh: after the sweep both
# counts are zero, so the guard is green on an empty set, and an allow-list would only give the next
# one somewhere to hide.
#
# Matches the binding string rather than the bare port number, because two unit tests legitimately
# construct a synthetic MongoBulkWriteException with new ServerAddress("localhost", 27017) and never
# start a container.

set -euo pipefail

status=0

report() {
  local description=$1 remedy=$2
  shift 2
  echo "::error::$description"
  printf '::error::  %s\n' "$@"
  echo "::error::$remedy"
  status=1
}

# ---- 1. A pinned host port ---------------------------------------------------
pinned=$(grep -rEl '"270(17|18):27017"' --include='*.java' --include='*.kt' . \
  | grep '/src/test/' | sed 's|^\./||' | sort || true)

if [ -n "$pinned" ]; then
  # shellcheck disable=SC2086
  report "These tests pin a fixed MongoDB host port:" \
    "Drop the binding. The container's mapped port is what getReplicaSetUrl() reports, so nothing needs pinning." \
    $pinned
fi

# ---- 2. The image name built from a system property -------------------------
handbuilt=$(grep -rl 'System.getProperty("test.mongo.version")' --include='*.java' --include='*.kt' . \
  | grep '/src/test/' | sed 's|^\./||' | sort || true)

if [ -n "$handbuilt" ]; then
  # shellcheck disable=SC2086
  report "These tests build the Mongo image name from a system property:" \
    "Use ReplicaSetReadyMongoDBContainer.withDefaultVersion(), which also works in an IDE." \
    $handbuilt
fi

if [ "$status" -eq 0 ]; then
  echo "Every MongoDB test container goes through ReplicaSetReadyMongoDBContainer on a dynamic port."
fi

exit "$status"
