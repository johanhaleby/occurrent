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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.CatchupMarker;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

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

    @Test
    void a_not_older_than_marker_write_below_the_stored_version_is_refused_when_both_versions_are_above_2_pow_53_and_adjacent() {
        // Given
        SpringRedisCheckpointStorage storage = new SpringRedisCheckpointStorage(redisTemplate);
        storage.save(subscriptionId, new StringBasedCheckpoint("first"), CheckpointWriteCondition.notOlderThan(9007199254740993L));

        // When
        boolean written = storage.saveCatchupMarker(subscriptionId, MARKER, 0, CheckpointWriteCondition.notOlderThan(9007199254740992L));

        // Then
        assertAll(
                () -> assertThat(written).as("written").isFalse(),
                () -> assertThat(storage.read(subscriptionId)).as("checkpoint").isEqualTo(new StringBasedCheckpoint("first")),
                () -> assertThat(storage.writeVersion(subscriptionId)).as("write version").hasValue(9007199254740993L)
        );
    }

    @Test
    void every_attempt_of_one_forget_sends_its_own_generation() {
        // Given, a first attempt that reaches Redis and then fails, so the call retries
        List<String> generationsSent = new CopyOnWriteArrayList<>();
        AtomicBoolean failNext = new AtomicBoolean(true);
        StringRedisTemplate failingAfterTheFirstAttempt = new StringRedisTemplate(connectionFactory) {
            @Override
            public <T> T execute(RedisScript<T> script, RedisSerializer<?> argsSerializer, RedisSerializer<T> resultSerializer, List<String> keys, Object... args) {
                // The forget script is the only one called with three keys and exactly one argument, the generation
                boolean forget = keys.size() == 3 && args.length == 1;
                if (forget) {
                    generationsSent.add(new String((byte[]) args[0], UTF_8));
                }
                T result = super.execute(script, argsSerializer, resultSerializer, keys, args);
                if (forget && failNext.getAndSet(false)) {
                    throw new RedisConnectionFailureException("Connection lost after the script ran");
                }
                return result;
            }
        };
        failingAfterTheFirstAttempt.afterPropertiesSet();
        SpringRedisCheckpointStorage storage = new SpringRedisCheckpointStorage(failingAfterTheFirstAttempt, RetryStrategy.fixed(1L).maxAttempts(3));

        // When
        storage.forgetCatchupMarker(subscriptionId);

        // Then
        assertAll(
                () -> assertThat(generationsSent).as("generations sent, one per attempt").hasSize(2).doesNotHaveDuplicates(),
                () -> assertThat(String.valueOf(storage.readCatchupMarker(subscriptionId).generation())).as("generation stored").isEqualTo(generationsSent.get(1))
        );
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

    static Stream<Arguments> plainCallsThatRefuseAGenerationKeyAsSubscriptionId() {
        Map<String, Consumer<SpringRedisCheckpointStorage>> calls = new java.util.LinkedHashMap<>();
        String id = SpringRedisCheckpointStorage.generationKey("orders");
        calls.put("read", storage -> storage.read(id));
        calls.put("save any", storage -> storage.save(id, MARKER, CheckpointWriteCondition.any()));
        calls.put("save notOlderThan", storage -> storage.save(id, MARKER, CheckpointWriteCondition.notOlderThan(1)));
        calls.put("save ifAbsent", storage -> storage.save(id, MARKER, CheckpointWriteCondition.ifAbsent()));
        calls.put("delete", storage -> storage.delete(id));
        calls.put("exists", storage -> storage.exists(id));
        calls.put("writeVersion", storage -> storage.writeVersion(id));
        return calls.entrySet().stream().flatMap(call -> Stream.of(
                Arguments.of(call.getKey() + " in cluster-safe mode", false, call.getValue()),
                Arguments.of(call.getKey() + " in standalone mode", true, call.getValue())));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("plainCallsThatRefuseAGenerationKeyAsSubscriptionId")
    void a_plain_call_refuses_the_generation_hash_of_another_subscription_and_leaves_it_untouched(String call, boolean standalone, Consumer<SpringRedisCheckpointStorage> plainCall) {
        // Given
        SpringRedisCheckpointStorage storage = standalone ? SpringRedisCheckpointStorage.forStandalone(redisTemplate) : new SpringRedisCheckpointStorage(redisTemplate);
        storage.forgetCatchupMarker("orders");
        String generationKey = SpringRedisCheckpointStorage.generationKey("orders");
        Set<String> keysBefore = requireNonNull(redisTemplate.keys("*"));
        Map<Object, Object> hashBefore = redisTemplate.opsForHash().entries(generationKey);

        // When
        Throwable thrown = org.assertj.core.api.Assertions.catchThrowable(() -> plainCall.accept(storage));

        // Then
        assertAll(
                () -> assertThat(thrown).as(call).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("reserves for its own generation keys"),
                () -> assertThat(hashBefore).as("generation hash before").isNotEmpty(),
                () -> assertThat(requireNonNull(redisTemplate.keys("*"))).as("keys").isEqualTo(keysBefore),
                () -> assertThat(redisTemplate.opsForHash().entries(generationKey)).as("generation hash").isEqualTo(hashBefore),
                () -> assertThat(storage.readCatchupMarker("orders").isPresent()).as("marker present").isFalse(),
                () -> assertThat(storage.readCatchupMarker("orders").generation()).as("generation").isEqualTo(Long.parseLong((String) hashBefore.get("generation")))
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void evaluates_write_conditions_for_answers_false_for_an_id_with_the_generation_key_prefix_in_either_mode(boolean standalone) {
        SpringRedisCheckpointStorage storage = standalone ? SpringRedisCheckpointStorage.forStandalone(redisTemplate) : new SpringRedisCheckpointStorage(redisTemplate);

        assertAll(
                () -> assertThat(storage.evaluatesWriteConditionsFor("occurrent:catchup-marker:orders")).as("generation key of orders").isFalse(),
                () -> assertThat(storage.evaluatesWriteConditionsFor("occurrent:catchup-marker:")).as("bare prefix").isFalse(),
                () -> assertThat(storage.evaluatesWriteConditionsFor("orders")).as("ordinary id").isTrue()
        );
    }

    @Test
    void the_marker_calls_throw_instead_of_retrying_forever_when_the_generation_key_holds_a_string() {
        // Given, a 0.34.0 node or an outside client left a string where the generation hash goes
        SpringRedisCheckpointStorage storage = new SpringRedisCheckpointStorage(redisTemplate);
        redisTemplate.opsForValue().set(SpringRedisCheckpointStorage.generationKey(subscriptionId), "not a hash");

        // When, then
        assertAll(
                () -> assertTimeoutPreemptively(Duration.ofSeconds(5), () -> assertThatThrownBy(() -> storage.readCatchupMarker(subscriptionId)).as("read").hasStackTraceContaining("WRONGTYPE")),
                () -> assertTimeoutPreemptively(Duration.ofSeconds(5), () -> assertThatThrownBy(() -> storage.saveCatchupMarker(subscriptionId, MARKER, 0, CheckpointWriteCondition.any())).as("save").hasStackTraceContaining("WRONGTYPE")),
                () -> assertTimeoutPreemptively(Duration.ofSeconds(5), () -> assertThatThrownBy(() -> storage.forgetCatchupMarker(subscriptionId)).as("forget").hasStackTraceContaining("WRONGTYPE")),
                () -> assertThat(redisTemplate.opsForValue().get(SpringRedisCheckpointStorage.generationKey(subscriptionId))).as("string left in place").isEqualTo("not a hash")
        );
    }

    @Test
    void a_plain_read_throws_instead_of_retrying_forever_when_the_checkpoint_key_holds_a_hash() {
        // Given
        SpringRedisCheckpointStorage storage = new SpringRedisCheckpointStorage(redisTemplate);
        redisTemplate.opsForHash().put(subscriptionId, "field", "value");

        // When, then
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> assertThatThrownBy(() -> storage.read(subscriptionId)).hasStackTraceContaining("WRONGTYPE"));
    }

    private static void setGeneration(String subscriptionId, long generation) {
        redisTemplate.opsForHash().put(SpringRedisCheckpointStorage.generationKey(subscriptionId), "generation", Long.toString(generation));
    }
}
