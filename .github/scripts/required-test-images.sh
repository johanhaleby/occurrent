#!/usr/bin/env bash
# Prints the Docker Hub images the test suite needs, one per line, so the mirror job and the test
# shards agree on the list without either of them hardcoding a tag.
#
# Every lookup is best effort. An image that cannot be derived is reported as a warning and left out
# of the list, which means it gets pulled from Docker Hub as it always did rather than breaking the
# run. That is deliberate: a list that silently rots is worse than a mirror that covers less.
#
# Usage: required-test-images.sh [repo-root]
set -uo pipefail

root=${1:-.}

# Mongo. Every shard starts one. The version is the same property Surefire passes to the tests.
mongo_version=$(grep -oE '<integration-tests\.mongo\.version>[^<]+' "$root/pom.xml" 2>/dev/null | sed 's/.*>//')
if [ -n "$mongo_version" ]; then
    echo "mongo:$mongo_version"
else
    echo "::warning::Could not read integration-tests.mongo.version from pom.xml, so the Mongo image is not mirrored and will come from Docker Hub." >&2
fi

# Redis. One shard needs it, and the tag is a literal in the test rather than a pom property, so read
# it where it actually lives instead of repeating it here.
redis_test="$root/subscription/redis/spring/blocking-checkpoint-storage/src/test/java/org/occurrent/subscription/redis/spring/blocking/SpringRedisCheckpointStorageTest.java"
redis_image=$(grep -oE '"redis:[^"]+"' "$redis_test" 2>/dev/null | head -1 | tr -d '"')
if [ -n "$redis_image" ]; then
    echo "$redis_image"
else
    echo "::warning::Could not read the Redis image from SpringRedisCheckpointStorageTest, so it is not mirrored and will come from Docker Hub." >&2
fi

# Ryuk, the Testcontainers reaper, pulled by almost every shard. Its tag exists only as a constant in
# RyukContainer's bytecode: no properties resource carries it, and getRyukImage() reports the image
# without a tag. So read it out of the resolved jar, and treat a miss as a fall back to Docker Hub,
# which is what happens if a Testcontainers upgrade ever moves it.
ryuk_image=""
ryuk_jar=$(find "$root/.m2repo" "$HOME/.m2/repository" -path '*org/testcontainers/testcontainers/*' -name 'testcontainers-*.jar' 2>/dev/null | sort | tail -1)
if [ -n "$ryuk_jar" ]; then
    ryuk_image=$(javap -c -constants -cp "$ryuk_jar" org.testcontainers.utility.RyukContainer 2>/dev/null \
        | grep -oE 'testcontainers/ryuk:[0-9][0-9A-Za-z._-]*' | head -1)
fi
if [ -n "$ryuk_image" ]; then
    echo "$ryuk_image"
else
    echo "::warning::Could not read the ryuk image tag from the Testcontainers jar, so it is not mirrored and will come from Docker Hub." >&2
fi
