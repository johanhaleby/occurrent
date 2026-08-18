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

# RabbitMQ. No shard starts one yet (the broker transport modules aren't built), but the property already
# exists for them to read, so derive the image the same way as Mongo rather than waiting for the first user.
rabbitmq_version=$(grep -oE '<integration-tests\.rabbitmq\.version>[^<]+' "$root/pom.xml" 2>/dev/null | sed 's/.*>//')
if [ -n "$rabbitmq_version" ]; then
    echo "rabbitmq:$rabbitmq_version"
else
    echo "::warning::Could not read integration-tests.rabbitmq.version from pom.xml, so the RabbitMQ image is not mirrored and will come from Docker Hub." >&2
fi

# Kafka. Same reasoning as RabbitMQ above.
kafka_version=$(grep -oE '<integration-tests\.kafka\.version>[^<]+' "$root/pom.xml" 2>/dev/null | sed 's/.*>//')
if [ -n "$kafka_version" ]; then
    echo "apache/kafka:$kafka_version"
else
    echo "::warning::Could not read integration-tests.kafka.version from pom.xml, so the Kafka image is not mirrored and will come from Docker Hub." >&2
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
# without a tag. So read it out of the jar, and treat a miss as a fall back to Docker Hub, which is
# what happens if a Testcontainers upgrade ever moves that constant.
#
# The jar path is built from the pinned version rather than by scanning for one. A local repository
# commonly holds several Testcontainers versions, and picking by name would sort 1.20.6 above 1.20.12
# and mirror the wrong ryuk tag. This way it is the version the build actually resolves.
tc_version=$(grep -oE '<test-containers\.version>[^<]+' "$root/pom.xml" 2>/dev/null | sed 's/.*>//')
ryuk_image=""
if [ -n "$tc_version" ]; then
    for maven_repo in "$root/.m2repo" "$HOME/.m2/repository"; do
        ryuk_jar="$maven_repo/org/testcontainers/testcontainers/$tc_version/testcontainers-$tc_version.jar"
        [ -f "$ryuk_jar" ] || continue
        ryuk_image=$(javap -c -constants -cp "$ryuk_jar" org.testcontainers.utility.RyukContainer 2>/dev/null \
            | grep -oE 'testcontainers/ryuk:[0-9][0-9A-Za-z._-]*' | head -1)
        [ -n "$ryuk_image" ] && break
    done
fi
if [ -n "$ryuk_image" ]; then
    echo "$ryuk_image"
elif [ -z "$tc_version" ]; then
    echo "::warning::Could not read test-containers.version from pom.xml, so the ryuk image is not mirrored and will come from Docker Hub." >&2
else
    echo "::warning::Could not read the ryuk image tag from the Testcontainers $tc_version jar, so it is not mirrored and will come from Docker Hub." >&2
fi
