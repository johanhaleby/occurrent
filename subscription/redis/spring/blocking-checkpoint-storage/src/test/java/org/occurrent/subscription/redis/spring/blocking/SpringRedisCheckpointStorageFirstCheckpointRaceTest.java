/*
 * Copyright 2026 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.subscription.redis.spring.blocking;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * The three-argument {@code resolveFirstCheckpointRace} against a Redis container, since what it settles is done by one
 * Lua script whose version comparison and Cluster-safe answer a fake cannot exercise.
 */
@Timeout(30)
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
class SpringRedisCheckpointStorageFirstCheckpointRaceTest {

    @Container
    private static final GenericContainer<?> redisContainer = new GenericContainer<>("redis:5.0.3-alpine").withExposedPorts(6379);

    @RegisterExtension
    FlushRedisExtension flushRedisExtension = new FlushRedisExtension(redisContainer.getHost(), redisContainer.getFirstMappedPort());

    private LettuceConnectionFactory lettuceConnectionFactory;
    private RedisOperations<String, String> redis;
    private SpringRedisCheckpointStorage storage;
    private String subscriptionId;

    @BeforeEach
    void connect() {
        lettuceConnectionFactory = new LettuceConnectionFactory(redisContainer.getHost(), redisContainer.getFirstMappedPort());
        lettuceConnectionFactory.afterPropertiesSet();
        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(lettuceConnectionFactory);
        template.afterPropertiesSet();
        redis = template;
        storage = new SpringRedisCheckpointStorage(redis);
        subscriptionId = UUID.randomUUID().toString();
    }

    @AfterEach
    void disconnect() {
        lettuceConnectionFactory.destroy();
    }

    @Test
    void stores_the_candidate_and_the_version_when_nothing_is_stored() {
        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, checkpoint("candidate"), 2);

        assertAll(
                () -> assertThat(resolution).as("answer").hasValueSatisfying(c -> assertThat(c.asString()).isEqualTo("candidate")),
                () -> assertThat(storage.read(subscriptionId).asString()).as("stored position").isEqualTo("candidate"),
                () -> assertThat(storage.writeVersion(subscriptionId)).as("stored version").hasValue(2L)
        );
    }

    @Test
    void keeps_and_answers_a_stored_checkpoint_with_an_older_version_and_stores_the_new_version() {
        storage.save(subscriptionId, checkpoint("stored"), CheckpointWriteCondition.notOlderThan(1));

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, checkpoint("candidate"), 2);

        assertAll(
                () -> assertThat(resolution).as("answer").hasValueSatisfying(c -> assertThat(c.asString()).isEqualTo("stored")),
                () -> assertThat(storage.read(subscriptionId).asString()).as("stored position").isEqualTo("stored"),
                () -> assertThat(storage.writeVersion(subscriptionId)).as("stored version").hasValue(2L)
        );
    }

    @Test
    void keeps_and_answers_a_stored_checkpoint_with_no_version_and_stores_the_version() {
        storage.save(subscriptionId, checkpoint("stored"));

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, checkpoint("candidate"), 2);

        assertAll(
                () -> assertThat(resolution).as("answer").hasValueSatisfying(c -> assertThat(c.asString()).isEqualTo("stored")),
                () -> assertThat(storage.read(subscriptionId).asString()).as("stored position").isEqualTo("stored"),
                () -> assertThat(storage.writeVersion(subscriptionId)).as("stored version").hasValue(2L)
        );
    }

    @Test
    void a_checkpoint_written_by_a_plain_save_is_answered_back_exactly() {
        String written = "operation time 1 \"quoted\" åäö";
        storage.save(subscriptionId, checkpoint(written));

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, checkpoint("candidate"), 2);

        assertThat(resolution).hasValueSatisfying(c -> assertThat(c.asString()).isEqualTo(written));
    }

    @Test
    void accepts_a_version_equal_to_the_stored_one() {
        storage.save(subscriptionId, checkpoint("stored"), CheckpointWriteCondition.notOlderThan(2));

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, checkpoint("candidate"), 2);

        assertAll(
                () -> assertThat(resolution).as("answer").hasValueSatisfying(c -> assertThat(c.asString()).isEqualTo("stored")),
                () -> assertThat(storage.writeVersion(subscriptionId)).as("stored version").hasValue(2L)
        );
    }

    @Test
    void refuses_a_version_older_than_the_stored_one_and_writes_nothing() {
        storage.save(subscriptionId, checkpoint("newer-nodes-position"), CheckpointWriteCondition.notOlderThan(3));

        assertAll(
                () -> assertThatThrownBy(() -> storage.resolveFirstCheckpointRace(subscriptionId, checkpoint("candidate"), 2))
                        .isInstanceOfSatisfying(CheckpointWriteConditionNotFulfilledException.class, e -> assertThat(e.storedVersion).as("stored version in the exception").hasValue(3L)),
                () -> assertThat(storage.read(subscriptionId).asString()).as("stored position").isEqualTo("newer-nodes-position"),
                () -> assertThat(storage.writeVersion(subscriptionId)).as("stored version").hasValue(3L)
        );
    }

    @Test
    void compares_versions_above_two_to_the_power_of_fifty_three_exactly() {
        storage.save(subscriptionId, checkpoint("stored"), CheckpointWriteCondition.notOlderThan(9007199254740993L));

        assertAll(
                () -> assertThatThrownBy(() -> storage.resolveFirstCheckpointRace(subscriptionId, checkpoint("candidate"), 9007199254740992L))
                        .as("an offered version one below the stored one")
                        .isInstanceOfSatisfying(CheckpointWriteConditionNotFulfilledException.class, e -> assertThat(e.storedVersion).hasValue(9007199254740993L)),
                () -> assertThat(storage.writeVersion(subscriptionId)).as("stored version after the refusal").hasValue(9007199254740993L),
                () -> assertThat(storage.resolveFirstCheckpointRace(subscriptionId, checkpoint("candidate"), 9007199254740993L))
                        .as("an offered version equal to the stored one")
                        .hasValueSatisfying(c -> assertThat(c.asString()).isEqualTo("stored"))
        );
    }

    @Test
    void concurrent_callers_with_the_same_version_are_all_answered_the_one_checkpoint_that_is_stored() throws Exception {
        int callers = 8;
        List<String> candidates = new ArrayList<>();
        for (int i = 0; i < callers; i++) {
            candidates.add("candidate-" + i);
        }
        CyclicBarrier startTogether = new CyclicBarrier(callers);
        ExecutorService executor = Executors.newFixedThreadPool(callers);
        try {
            List<Future<Optional<Checkpoint>>> answers = new ArrayList<>();
            for (String candidate : candidates) {
                answers.add(executor.submit(() -> {
                    startTogether.await(10, TimeUnit.SECONDS);
                    return storage.resolveFirstCheckpointRace(subscriptionId, checkpoint(candidate), 2);
                }));
            }
            List<Optional<String>> answered = new ArrayList<>();
            for (Future<Optional<Checkpoint>> answer : answers) {
                answered.add(answer.get(15, TimeUnit.SECONDS).map(Checkpoint::asString));
            }

            assertAll(
                    () -> assertThat(candidates).as("the stored position is one of the candidates").contains(storage.read(subscriptionId).asString()),
                    () -> assertThat(answered).as("answers").hasSize(callers).containsOnly(Optional.of(storage.read(subscriptionId).asString())),
                    () -> assertThat(storage.writeVersion(subscriptionId)).as("stored version").hasValue(2L)
            );
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void a_negative_version_is_refused() {
        assertThatThrownBy(() -> storage.resolveFirstCheckpointRace(subscriptionId, checkpoint("candidate"), -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(storage.exists(subscriptionId)).as("a checkpoint is stored").isFalse();
    }

    @Test
    void a_cluster_safe_instance_answers_empty_and_writes_nothing_for_an_id_cluster_cannot_put_in_one_slot() {
        String id = "{}orders-" + subscriptionId;

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(id, checkpoint("candidate"), 2);

        assertAll(
                () -> assertThat(resolution).as("answer").isEmpty(),
                () -> assertThat(redis.hasKey(id)).as("checkpoint key").isFalse(),
                () -> assertThat(redis.hasKey(SpringRedisCheckpointStorage.versionKey(id))).as("version key").isFalse()
        );
    }

    @Test
    void a_standalone_instance_settles_the_first_position_for_that_same_id() {
        String id = "{}orders-" + subscriptionId;
        SpringRedisCheckpointStorage standalone = SpringRedisCheckpointStorage.forStandalone(redis);

        Optional<Checkpoint> resolution = standalone.resolveFirstCheckpointRace(id, checkpoint("candidate"), 2);

        assertAll(
                () -> assertThat(resolution).as("answer").hasValueSatisfying(c -> assertThat(c.asString()).isEqualTo("candidate")),
                () -> assertThat(standalone.read(id).asString()).as("stored position").isEqualTo("candidate"),
                () -> assertThat(standalone.writeVersion(id)).as("stored version").hasValue(2L)
        );
    }

    @Test
    void an_id_in_the_version_key_namespace_is_refused_in_both_modes() {
        String id = "occurrent:checkpoint-version:" + subscriptionId;

        assertAll(
                () -> assertThatThrownBy(() -> storage.resolveFirstCheckpointRace(id, checkpoint("candidate"), 2))
                        .as("cluster-safe").isInstanceOf(IllegalArgumentException.class),
                () -> assertThatThrownBy(() -> SpringRedisCheckpointStorage.forStandalone(redis).resolveFirstCheckpointRace(id, checkpoint("candidate"), 2))
                        .as("standalone").isInstanceOf(IllegalArgumentException.class)
        );
    }

    private static Checkpoint checkpoint(String value) {
        return new StringBasedCheckpoint(value);
    }
}
