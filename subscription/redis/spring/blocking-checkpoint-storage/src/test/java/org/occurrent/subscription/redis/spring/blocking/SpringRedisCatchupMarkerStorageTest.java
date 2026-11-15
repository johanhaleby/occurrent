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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.occurrent.subscription.CatchupMarker;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringRedisCatchupMarkerStorageTest {

    private static final Checkpoint MARKER = new StringBasedCheckpoint("marker");

    @Container
    private static final GenericContainer<?> redisContainer = new GenericContainer<>("redis:5.0.3-alpine").withExposedPorts(6379);

    @RegisterExtension
    FlushRedisExtension flushRedisExtension = new FlushRedisExtension(redisContainer.getHost(), redisContainer.getFirstMappedPort());

    private static LettuceConnectionFactory connectionFactory;
    private static StringRedisTemplate redisTemplate;

    private final String subscriptionId = UUID.randomUUID().toString();

    @BeforeAll
    static void connect() {
        connectionFactory = new LettuceConnectionFactory(redisContainer.getHost(), redisContainer.getFirstMappedPort());
        connectionFactory.afterPropertiesSet();
        // String serializers throughout, so the test can write the generation hash the way the storage's scripts read it
        redisTemplate = new StringRedisTemplate(connectionFactory);
    }

    @AfterAll
    static void disconnect() {
        connectionFactory.destroy();
    }

    @Test
    void forget_sets_a_generation_other_than_0_and_a_different_one_each_time() {
        // Given
        SpringRedisCheckpointStorage storage = new SpringRedisCheckpointStorage(redisTemplate);

        // When
        storage.forgetCatchupMarker(subscriptionId);
        long first = storage.readCatchupMarker(subscriptionId).generation();
        storage.forgetCatchupMarker(subscriptionId);
        long second = storage.readCatchupMarker(subscriptionId).generation();
        storage.forgetCatchupMarker(subscriptionId);
        long third = storage.readCatchupMarker(subscriptionId).generation();

        // Then
        assertThat(new long[]{first, second, third}).doesNotContain(0).doesNotHaveDuplicates();
    }

    // A failover lost the forget that raised the generation from 4 to 5, after an attempt had read 5
    @Test
    void a_late_save_stamped_with_a_generation_a_failover_lost_reads_absent_after_the_next_forget() {
        // Given
        SpringRedisCheckpointStorage storage = new SpringRedisCheckpointStorage(redisTemplate);
        setGeneration(subscriptionId, 4);
        storage.saveCatchupMarker(subscriptionId, MARKER, 5, CheckpointWriteCondition.any());
        CatchupMarker afterFailover = storage.readCatchupMarker(subscriptionId);

        // When
        storage.forgetCatchupMarker(subscriptionId);
        storage.saveCatchupMarker(subscriptionId, MARKER, 5, CheckpointWriteCondition.any());

        // Then
        CatchupMarker marker = storage.readCatchupMarker(subscriptionId);
        assertAll(
                () -> assertThat(afterFailover.generation()).as("generation after the failover").isEqualTo(4),
                () -> assertThat(afterFailover.isPresent()).as("present after the failover").isFalse(),
                () -> assertThat(marker.isPresent()).as("present after the late save").isFalse(),
                () -> assertThat(marker.generation()).as("generation after the forget").isNotIn(4L, 5L)
        );
    }

    @Test
    void the_marker_calls_refuse_an_id_with_the_generation_key_prefix_in_both_modes() {
        String id = "occurrent:catchup-marker:orders";

        for (SpringRedisCheckpointStorage storage : new SpringRedisCheckpointStorage[]{new SpringRedisCheckpointStorage(redisTemplate), SpringRedisCheckpointStorage.forStandalone(redisTemplate)}) {
            assertAll(
                    () -> assertThatThrownBy(() -> storage.readCatchupMarker(id)).isInstanceOf(IllegalArgumentException.class),
                    () -> assertThatThrownBy(() -> storage.saveCatchupMarker(id, MARKER, 0, CheckpointWriteCondition.any())).isInstanceOf(IllegalArgumentException.class),
                    () -> assertThatThrownBy(() -> storage.forgetCatchupMarker(id)).isInstanceOf(IllegalArgumentException.class)
            );
        }
        assertThat(requireNonNull(redisTemplate.keys("*"))).as("keys").isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "{}orders", "a}b{c"})
    void the_marker_calls_refuse_in_cluster_safe_mode_the_ids_a_conditional_save_refuses(String id) {
        SpringRedisCheckpointStorage storage = new SpringRedisCheckpointStorage(redisTemplate);

        assertAll(
                () -> assertThatThrownBy(() -> storage.save(id, MARKER, CheckpointWriteCondition.ifAbsent())).as("conditional save").isInstanceOf(IllegalArgumentException.class),
                () -> assertThatThrownBy(() -> storage.readCatchupMarker(id)).as("read").isInstanceOf(IllegalArgumentException.class),
                () -> assertThatThrownBy(() -> storage.saveCatchupMarker(id, MARKER, 0, CheckpointWriteCondition.any())).as("save").isInstanceOf(IllegalArgumentException.class),
                () -> assertThatThrownBy(() -> storage.forgetCatchupMarker(id)).as("forget").isInstanceOf(IllegalArgumentException.class),
                () -> assertThat(requireNonNull(redisTemplate.keys("*"))).as("keys").isEmpty()
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "{}orders", "a}b{c"})
    void the_marker_calls_accept_in_standalone_mode_the_ids_cluster_safe_mode_refuses(String id) {
        SpringRedisCheckpointStorage storage = SpringRedisCheckpointStorage.forStandalone(redisTemplate);

        assertThatCode(() -> {
            storage.forgetCatchupMarker(id);
            long generation = storage.readCatchupMarker(id).generation();
            storage.saveCatchupMarker(id, MARKER, generation, CheckpointWriteCondition.any());
        }).as("forget, read and save").doesNotThrowAnyException();
        assertThat(storage.readCatchupMarker(id).isPresent()).as("present").isTrue();
    }

    private static void setGeneration(String subscriptionId, long generation) {
        redisTemplate.opsForHash().put(SpringRedisCheckpointStorage.generationKey(subscriptionId), "generation", Long.toString(generation));
    }
}
