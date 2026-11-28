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

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * {@code storeWriteVersion} against a Redis container, since the version comparison and the version write are done by one Lua
 * script a fake cannot exercise. The id Cluster cannot put in one slot has its own answer in Cluster-safe mode.
 */
@Timeout(30)
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
class SpringRedisCheckpointStorageStoreWriteVersionTest {

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
    void answers_empty_and_writes_nothing_when_nothing_is_stored() {
        Optional<Checkpoint> answer = storage.storeWriteVersion(subscriptionId, 2);

        assertAll(
                () -> assertThat(answer).as("answer").isEmpty(),
                () -> assertThat(redis.hasKey(subscriptionId)).as("checkpoint key").isFalse(),
                () -> assertThat(redis.hasKey(SpringRedisCheckpointStorage.versionKey(subscriptionId))).as("version key").isFalse()
        );
    }

    @Test
    void stores_the_version_and_answers_the_checkpoint_when_a_checkpoint_with_no_version_is_stored() {
        storage.save(subscriptionId, checkpoint("stored"));

        Optional<Checkpoint> answer = storage.storeWriteVersion(subscriptionId, 2);

        assertAll(
                () -> assertThat(answer).as("answer").hasValueSatisfying(c -> assertThat(c.asString()).isEqualTo("stored")),
                () -> assertThat(storage.read(subscriptionId).asString()).as("stored position").isEqualTo("stored"),
                () -> assertThat(storage.writeVersion(subscriptionId)).as("stored version").hasValue(2L)
        );
    }

    @Test
    void stores_the_version_and_answers_the_checkpoint_when_a_checkpoint_with_an_older_version_is_stored() {
        storage.save(subscriptionId, checkpoint("stored"), CheckpointWriteCondition.notOlderThan(1));

        Optional<Checkpoint> answer = storage.storeWriteVersion(subscriptionId, 2);

        assertAll(
                () -> assertThat(answer).as("answer").hasValueSatisfying(c -> assertThat(c.asString()).isEqualTo("stored")),
                () -> assertThat(storage.read(subscriptionId).asString()).as("stored position").isEqualTo("stored"),
                () -> assertThat(storage.writeVersion(subscriptionId)).as("stored version").hasValue(2L)
        );
    }

    @Test
    void answers_the_checkpoint_and_keeps_the_version_when_a_checkpoint_with_the_same_version_is_stored() {
        storage.save(subscriptionId, checkpoint("stored"), CheckpointWriteCondition.notOlderThan(2));

        Optional<Checkpoint> answer = storage.storeWriteVersion(subscriptionId, 2);

        assertAll(
                () -> assertThat(answer).as("answer").hasValueSatisfying(c -> assertThat(c.asString()).isEqualTo("stored")),
                () -> assertThat(storage.read(subscriptionId).asString()).as("stored position").isEqualTo("stored"),
                () -> assertThat(storage.writeVersion(subscriptionId)).as("stored version").hasValue(2L)
        );
    }

    @Test
    void answers_a_checkpoint_with_quotes_and_non_ascii_characters_exactly_as_stored() {
        String written = "operation time 1 \"quoted\" åäö";
        storage.save(subscriptionId, checkpoint(written));

        Optional<Checkpoint> answer = storage.storeWriteVersion(subscriptionId, 2);

        assertThat(answer).as("answer").hasValueSatisfying(c -> assertThat(c.asString()).isEqualTo(written));
    }

    @Test
    void refuses_a_version_older_than_the_stored_one_with_that_version_and_writes_nothing() {
        storage.save(subscriptionId, checkpoint("newer-nodes-position"), CheckpointWriteCondition.notOlderThan(3));

        assertAll(
                () -> assertThatThrownBy(() -> storage.storeWriteVersion(subscriptionId, 2))
                        .isInstanceOfSatisfying(CheckpointWriteConditionNotFulfilledException.class, e -> assertAll(
                                () -> assertThat(e.storedVersion).as("stored version in the exception").hasValue(3L),
                                () -> assertThat(e.condition).as("condition in the exception").isEqualTo(CheckpointWriteCondition.notOlderThan(2)))),
                () -> assertThat(storage.read(subscriptionId).asString()).as("stored position").isEqualTo("newer-nodes-position"),
                () -> assertThat(storage.writeVersion(subscriptionId)).as("stored version").hasValue(3L)
        );
    }

    @Test
    void a_negative_version_is_refused_and_writes_nothing() {
        storage.save(subscriptionId, checkpoint("stored"));

        assertAll(
                () -> assertThatThrownBy(() -> storage.storeWriteVersion(subscriptionId, -1)).isInstanceOf(IllegalArgumentException.class),
                () -> assertThat(storage.read(subscriptionId).asString()).as("stored position").isEqualTo("stored"),
                () -> assertThat(storage.writeVersion(subscriptionId)).as("stored version").isEmpty()
        );
    }

    @Test
    void an_id_in_the_version_key_namespace_is_refused_in_both_modes() {
        String id = "occurrent:checkpoint-version:" + subscriptionId;

        assertAll(
                () -> assertThatThrownBy(() -> storage.storeWriteVersion(id, 2)).as("cluster-safe").isInstanceOf(IllegalArgumentException.class),
                () -> assertThatThrownBy(() -> SpringRedisCheckpointStorage.forStandalone(redis).storeWriteVersion(id, 2)).as("standalone").isInstanceOf(IllegalArgumentException.class)
        );
    }

    @Test
    void a_cluster_safe_instance_writes_nothing_and_answers_empty_for_an_id_cluster_cannot_align_when_a_checkpoint_with_an_older_version_is_stored() {
        String id = "{}orders-" + subscriptionId;
        SpringRedisCheckpointStorage standalone = SpringRedisCheckpointStorage.forStandalone(redis);
        standalone.save(id, checkpoint("stored"), CheckpointWriteCondition.notOlderThan(1));

        Optional<Checkpoint> answer = storage.storeWriteVersion(id, 2);

        assertAll(
                () -> assertThat(answer).as("answer").isEmpty(),
                () -> assertThat(redis.opsForValue().get(id)).as("checkpoint value").isEqualTo("stored"),
                () -> assertThat(standalone.writeVersion(id)).as("stored version").hasValue(1L)
        );
    }

    @Test
    void a_cluster_safe_instance_writes_nothing_and_answers_empty_for_an_id_cluster_cannot_align_when_nothing_is_stored() {
        String id = "{}orders-" + subscriptionId;

        Optional<Checkpoint> answer = storage.storeWriteVersion(id, 2);

        assertAll(
                () -> assertThat(answer).as("answer").isEmpty(),
                () -> assertThat(redis.hasKey(id)).as("checkpoint key").isFalse(),
                () -> assertThat(redis.hasKey(SpringRedisCheckpointStorage.versionKey(id))).as("version key").isFalse()
        );
    }

    @Test
    void a_cluster_safe_instance_refuses_an_id_cluster_cannot_align_when_a_newer_version_is_stored_and_writes_nothing() {
        String id = "{}orders-" + subscriptionId;
        SpringRedisCheckpointStorage standalone = SpringRedisCheckpointStorage.forStandalone(redis);
        standalone.save(id, checkpoint("newer-nodes-position"), CheckpointWriteCondition.notOlderThan(3));

        assertAll(
                () -> assertThatThrownBy(() -> storage.storeWriteVersion(id, 2))
                        .isInstanceOfSatisfying(CheckpointWriteConditionNotFulfilledException.class, e -> assertAll(
                                () -> assertThat(e.storedVersion).as("stored version in the exception").hasValue(3L),
                                () -> assertThat(e.condition).as("condition in the exception").isEqualTo(CheckpointWriteCondition.notOlderThan(2)))),
                () -> assertThat(redis.opsForValue().get(id)).as("checkpoint value").isEqualTo("newer-nodes-position"),
                () -> assertThat(standalone.writeVersion(id)).as("stored version").hasValue(3L)
        );
    }

    @Test
    void a_standalone_instance_stores_the_version_for_that_same_id() {
        String id = "{}orders-" + subscriptionId;
        SpringRedisCheckpointStorage standalone = SpringRedisCheckpointStorage.forStandalone(redis);
        standalone.save(id, checkpoint("stored"));

        Optional<Checkpoint> answer = standalone.storeWriteVersion(id, 2);

        assertAll(
                () -> assertThat(answer).as("answer").hasValueSatisfying(c -> assertThat(c.asString()).isEqualTo("stored")),
                () -> assertThat(standalone.writeVersion(id)).as("stored version").hasValue(2L)
        );
    }

    private static Checkpoint checkpoint(String value) {
        return new StringBasedCheckpoint(value);
    }
}
