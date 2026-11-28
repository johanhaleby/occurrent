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
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.RedisSerializer;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
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
    void answers_empty_and_writes_nothing_when_a_checkpoint_with_an_older_version_is_stored() {
        storage.save(subscriptionId, checkpoint("stored"), CheckpointWriteCondition.notOlderThan(1));

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, checkpoint("candidate"), 2);

        assertAll(
                () -> assertThat(resolution).as("answer").isEmpty(),
                () -> assertThat(storage.read(subscriptionId).asString()).as("stored position").isEqualTo("stored"),
                () -> assertThat(storage.writeVersion(subscriptionId)).as("stored version").hasValue(1L)
        );
    }

    @Test
    void answers_empty_and_writes_nothing_when_a_checkpoint_with_no_version_is_stored() {
        storage.save(subscriptionId, checkpoint("stored"));

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, checkpoint("candidate"), 2);

        assertAll(
                () -> assertThat(resolution).as("answer").isEmpty(),
                () -> assertThat(storage.read(subscriptionId).asString()).as("stored position").isEqualTo("stored"),
                () -> assertThat(storage.writeVersion(subscriptionId)).as("stored version").isEmpty()
        );
    }

    @Test
    void a_checkpoint_written_by_a_plain_save_is_left_exactly_as_it_was() {
        String written = "operation time 1 \"quoted\" åäö";
        storage.save(subscriptionId, checkpoint(written));

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, checkpoint("candidate"), 2);

        assertAll(
                () -> assertThat(resolution).as("answer").isEmpty(),
                () -> assertThat(storage.read(subscriptionId).asString()).as("stored position").isEqualTo(written)
        );
    }

    @Test
    void a_candidate_with_quotes_and_non_ascii_characters_is_stored_and_answered_exactly() {
        String candidate = "operation time 1 \"quoted\" åäö";

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, checkpoint(candidate), 2);

        assertAll(
                () -> assertThat(resolution).as("answer").hasValueSatisfying(c -> assertThat(c.asString()).isEqualTo(candidate)),
                () -> assertThat(storage.read(subscriptionId).asString()).as("stored position").isEqualTo(candidate)
        );
    }

    @Test
    void answers_empty_and_writes_nothing_when_a_checkpoint_with_a_version_equal_to_the_offered_one_is_stored() {
        storage.save(subscriptionId, checkpoint("stored"), CheckpointWriteCondition.notOlderThan(2));

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, checkpoint("candidate"), 2);

        assertAll(
                () -> assertThat(resolution).as("answer").isEmpty(),
                () -> assertThat(storage.read(subscriptionId).asString()).as("stored position").isEqualTo("stored"),
                () -> assertThat(storage.writeVersion(subscriptionId)).as("stored version").hasValue(2L)
        );
    }

    @Test
    void stores_the_version_and_answers_the_candidate_when_the_same_position_with_an_older_version_is_stored() {
        storage.save(subscriptionId, checkpoint("candidate"), CheckpointWriteCondition.notOlderThan(1));

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, checkpoint("candidate"), 2);

        assertAll(
                () -> assertThat(resolution).as("answer").hasValueSatisfying(c -> assertThat(c.asString()).isEqualTo("candidate")),
                () -> assertThat(storage.read(subscriptionId).asString()).as("stored position").isEqualTo("candidate"),
                () -> assertThat(storage.writeVersion(subscriptionId)).as("stored version").hasValue(2L)
        );
    }

    @Test
    void stores_the_version_and_answers_the_candidate_when_the_same_position_with_no_version_is_stored() {
        storage.save(subscriptionId, checkpoint("candidate"));

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, checkpoint("candidate"), 2);

        assertAll(
                () -> assertThat(resolution).as("answer").hasValueSatisfying(c -> assertThat(c.asString()).isEqualTo("candidate")),
                () -> assertThat(storage.read(subscriptionId).asString()).as("stored position").isEqualTo("candidate"),
                () -> assertThat(storage.writeVersion(subscriptionId)).as("stored version").hasValue(2L)
        );
    }

    @Test
    void refuses_the_same_position_stored_with_a_newer_version_and_leaves_that_version() {
        storage.save(subscriptionId, checkpoint("candidate"), CheckpointWriteCondition.notOlderThan(3));

        assertAll(
                () -> assertThatThrownBy(() -> storage.resolveFirstCheckpointRace(subscriptionId, checkpoint("candidate"), 2))
                        .isInstanceOf(CheckpointWriteConditionNotFulfilledException.class),
                () -> assertThat(storage.writeVersion(subscriptionId)).as("stored version").hasValue(3L)
        );
    }

    @Test
    void a_retry_after_an_attempt_that_wrote_and_lost_its_reply_answers_the_candidate_the_first_attempt_wrote() {
        AtomicInteger attempts = new AtomicInteger();
        StringRedisTemplate losingTheFirstReply = new StringRedisTemplate(lettuceConnectionFactory) {
            @Override
            public <T> T execute(RedisScript<T> script, RedisSerializer<?> argsSerializer, RedisSerializer<T> resultSerializer, List<String> keys, Object... args) {
                T reply = super.execute(script, argsSerializer, resultSerializer, keys, args);
                if (attempts.getAndIncrement() == 0) {
                    throw new RedisConnectionFailureException("Connection lost after the script ran");
                }
                return reply;
            }
        };
        losingTheFirstReply.afterPropertiesSet();
        SpringRedisCheckpointStorage retrying = new SpringRedisCheckpointStorage(losingTheFirstReply, RetryStrategy.fixed(1L).maxAttempts(3));

        Optional<Checkpoint>[] resolution = new Optional[1];
        Throwable failure = catchThrowable(() -> resolution[0] = retrying.resolveFirstCheckpointRace(subscriptionId, checkpoint("candidate"), 2));

        assertAll(
                () -> assertThat(failure).as("failure").isNull(),
                () -> assertThat(attempts).as("the attempt that wrote and the retry").hasValue(2),
                () -> assertThat(resolution[0]).as("answer").hasValueSatisfying(c -> assertThat(c.asString()).isEqualTo("candidate")),
                () -> assertThat(retrying.read(subscriptionId).asString()).as("stored position").isEqualTo("candidate"),
                () -> assertThat(retrying.writeVersion(subscriptionId)).as("stored version").hasValue(2L)
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
                        .isEmpty(),
                () -> assertThat(storage.read(subscriptionId).asString()).as("stored position after the equal version").isEqualTo("stored"),
                () -> assertThat(storage.writeVersion(subscriptionId)).as("stored version after the equal version").hasValue(9007199254740993L)
        );
    }

    @Test
    void of_concurrent_callers_with_the_same_version_exactly_one_is_answered_and_the_rest_are_answered_empty() throws Exception {
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
                    () -> assertThat(answered).as("answers").hasSize(callers).containsOnlyOnce(Optional.of(storage.read(subscriptionId).asString())),
                    () -> assertThat(answered).as("answers other than the stored position").filteredOn(Optional::isPresent).hasSize(1),
                    () -> assertThat(answered).as("empty answers").filteredOn(Optional::isEmpty).hasSize(callers - 1),
                    () -> assertThat(storage.writeVersion(subscriptionId)).as("stored version").hasValue(2L)
            );
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void retries_a_failed_attempt_and_settles_the_first_position_with_its_version() {
        AtomicInteger attempts = new AtomicInteger();
        StringRedisTemplate failingTheFirstAttempt = new StringRedisTemplate(lettuceConnectionFactory) {
            @Override
            public <T> T execute(RedisScript<T> script, RedisSerializer<?> argsSerializer, RedisSerializer<T> resultSerializer, List<String> keys, Object... args) {
                if (attempts.getAndIncrement() == 0) {
                    throw new RedisConnectionFailureException("Connection lost before the script ran");
                }
                return super.execute(script, argsSerializer, resultSerializer, keys, args);
            }
        };
        failingTheFirstAttempt.afterPropertiesSet();
        SpringRedisCheckpointStorage retrying = new SpringRedisCheckpointStorage(failingTheFirstAttempt, RetryStrategy.fixed(1L).maxAttempts(3));

        Optional<Checkpoint>[] resolution = new Optional[1];
        Throwable failure = catchThrowable(() -> resolution[0] = retrying.resolveFirstCheckpointRace(subscriptionId, checkpoint("candidate"), 2));

        assertAll(
                () -> assertThat(failure).as("failure").isNull(),
                () -> assertThat(attempts).as("one failed attempt and one that went through").hasValue(2),
                () -> assertThat(resolution[0]).as("answer").hasValueSatisfying(c -> assertThat(c.asString()).isEqualTo("candidate")),
                () -> assertThat(retrying.read(subscriptionId).asString()).as("stored position").isEqualTo("candidate"),
                () -> assertThat(retrying.writeVersion(subscriptionId)).as("stored version").hasValue(2L)
        );
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
    void a_cluster_safe_instance_refuses_an_id_cluster_cannot_put_in_one_slot_when_a_newer_version_is_stored_and_writes_nothing() {
        String id = "{}orders-" + subscriptionId;
        SpringRedisCheckpointStorage.forStandalone(redis).save(id, checkpoint("newer-nodes-position"), CheckpointWriteCondition.notOlderThan(3));

        assertAll(
                () -> assertThatThrownBy(() -> storage.resolveFirstCheckpointRace(id, checkpoint("candidate"), 2))
                        .isInstanceOfSatisfying(CheckpointWriteConditionNotFulfilledException.class, e -> assertThat(e.storedVersion).as("stored version in the exception").hasValue(3L)),
                () -> assertThat(redis.opsForValue().get(id)).as("checkpoint value").isEqualTo("newer-nodes-position"),
                () -> assertThat(SpringRedisCheckpointStorage.forStandalone(redis).writeVersion(id)).as("stored version").hasValue(3L)
        );
    }

    @Test
    void a_cluster_safe_instance_answers_empty_and_writes_nothing_for_such_an_id_when_an_older_version_is_stored() {
        String id = "{}orders-" + subscriptionId;
        SpringRedisCheckpointStorage standalone = SpringRedisCheckpointStorage.forStandalone(redis);
        standalone.save(id, checkpoint("stored"), CheckpointWriteCondition.notOlderThan(1));

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(id, checkpoint("candidate"), 2);

        assertAll(
                () -> assertThat(resolution).as("answer").isEmpty(),
                () -> assertThat(redis.opsForValue().get(id)).as("checkpoint value").isEqualTo("stored"),
                () -> assertThat(standalone.writeVersion(id)).as("stored version").hasValue(1L)
        );
    }

    @Test
    void a_cluster_safe_instance_answers_empty_and_writes_nothing_for_such_an_id_when_the_stored_version_is_the_offered_one() {
        String id = "{}orders-" + subscriptionId;
        SpringRedisCheckpointStorage standalone = SpringRedisCheckpointStorage.forStandalone(redis);
        standalone.save(id, checkpoint("stored"), CheckpointWriteCondition.notOlderThan(2));

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(id, checkpoint("candidate"), 2);

        assertAll(
                () -> assertThat(resolution).as("answer").isEmpty(),
                () -> assertThat(redis.opsForValue().get(id)).as("checkpoint value").isEqualTo("stored"),
                () -> assertThat(standalone.writeVersion(id)).as("stored version").hasValue(2L)
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
