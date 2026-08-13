/*
 * Copyright 2021 Johan Haleby
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.github.artsok.RepeatedIfExceptionsTest;
import io.lettuce.core.RedisCommandExecutionException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.functional.CheckedFunction;
import org.occurrent.functional.Not;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.occurrent.time.TimeConversion;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.ONE_SECOND;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Timeout(20)
@DisplayNameGeneration(DisplayNameGenerator.Simple.class)
@Testcontainers
class SpringRedisCheckpointStorageTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion()
            .withReuse(true);
    @Container
    private static final GenericContainer<?> redisContainer = new GenericContainer<>("redis:5.0.3-alpine").withExposedPorts(6379);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    @RegisterExtension
    FlushRedisExtension flushRedisExtension = new FlushRedisExtension(redisContainer.getHost(), redisContainer.getFirstMappedPort());

    private MongoClient mongoClient;
    private SpringMongoEventStore mongoEventStore;
    private ObjectMapper objectMapper;
    private LettuceConnectionFactory lettuceConnectionFactory;
    private DurableSubscriptionModel redisSubscription;
    private RedisOperations<String, String> redisTemplate;
    private ConnectionString connectionString;

    @BeforeEach
    void initialize() {
        connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(connectionString.getCollection()).transactionConfig(mongoTransactionManager).timeRepresentation(TimeRepresentation.RFC_3339_STRING).build();
        mongoEventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
        SpringMongoSubscriptionModel subscriptionModel = new SpringMongoSubscriptionModel(mongoTemplate, connectionString.getCollection(), TimeRepresentation.RFC_3339_STRING);
        lettuceConnectionFactory = new LettuceConnectionFactory(redisContainer.getHost(), redisContainer.getFirstMappedPort());
        redisTemplate = createRedisTemplate(lettuceConnectionFactory);
        CheckpointStorage storage = new SpringRedisCheckpointStorage(redisTemplate);
        redisSubscription = new DurableSubscriptionModel(subscriptionModel, storage);
        objectMapper = new ObjectMapper();
    }

    @AfterEach
    void dispose() {
        redisSubscription.shutdown();
        mongoClient.close();
        lettuceConnectionFactory.destroy();
    }

    @Test
    void redis_blocking_spring_subscription_calls_listener_for_each_new_event() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        redisSubscription.subscribe(UUID.randomUUID().toString(), state::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name", "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name", "name3");

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined1));
        mongoEventStore.write("2", 0, serialize(nameDefined2));
        mongoEventStore.write("1", 1, serialize(nameWasChanged1));

        // Then
        await().atMost(2, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(3));
    }

    @RepeatedIfExceptionsTest(repeats = 5, suspend = 500)
    void redis_blocking_spring_subscription_allows_resuming_events_from_where_it_left_off() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        String subscriberId = UUID.randomUUID().toString();
        redisSubscription.subscribe(subscriberId, state::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name", "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name", "name3");

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined1));
        cancelSubscription(redisSubscription, subscriberId);
        // The subscription is async so we need to wait for it
        await().atMost(ONE_SECOND).until(Not.not(state::isEmpty));
        mongoEventStore.write("2", 0, serialize(nameDefined2));
        mongoEventStore.write("1", 1, serialize(nameWasChanged1));
        redisSubscription.subscribe(subscriberId, state::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));

        // Then
        await().atMost(2, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(3));
    }

    @Test
    void redis_blocking_spring_subscription_allows_resuming_events_from_where_it_left_when_first_event_for_subscription_fails_the_first_time() {
        // Given
        LocalDateTime now = LocalDateTime.now();

        AtomicInteger counter = new AtomicInteger();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        String subscriberId = UUID.randomUUID().toString();

        Runnable stream = () -> {
            MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
            SpringMongoSubscriptionModel subscriptionModel = new SpringMongoSubscriptionModel(mongoTemplate, connectionString.getCollection(), TimeRepresentation.RFC_3339_STRING, RetryStrategy.none());
            CheckpointStorage storage = new SpringRedisCheckpointStorage(redisTemplate, RetryStrategy.none());
            redisSubscription = new DurableSubscriptionModel(subscriptionModel, storage);
            redisSubscription.subscribe(subscriberId, cloudEvent -> {
                if (counter.incrementAndGet() == 1) {
                    // We simulate error on first event
                    throw new IllegalArgumentException("Expected");
                } else {
                    state.add(cloudEvent);
                }
            }).waitUntilStarted();
        };
        stream.run();
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name", "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name", "name3");

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined1));
        // The subscription is async so we need to wait for it. At least one call rather than exactly one,
        // because the model restarts from the position it had read and the failed event was never processed,
        // so it is handed to the handler again rather than skipped (#522).
        await().atMost(ONE_SECOND).and().dontCatchUncaughtExceptions().untilAtomic(counter, greaterThanOrEqualTo(1));
        // Since an exception occurred we need to run the stream again
        redisSubscription.shutdown();
        stream.run();
        mongoEventStore.write("2", 0, serialize(nameDefined2));
        mongoEventStore.write("1", 1, serialize(nameWasChanged1));

        // Then
        // Distinct ids rather than a size, because the first event can arrive more than once (#522). Its handler
        // threw, so the model hands it over again when it restarts, and the replacement model hands it over too if it
        // starts before that redelivery has been checkpointed. Every event still has to arrive, and in order.
        await().atMost(2, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state.stream().map(CloudEvent::getId).distinct())
                .containsExactly(nameDefined1.eventId(), nameDefined2.eventId(), nameWasChanged1.eventId()));
        // An upper bound too, so a model replaying the whole stream on every restart could not pass quietly. Six is
        // three events times the two models that run over this subscription id, since a restart the first model had
        // already begun can outlive the shutdown that follows it. Each resumes from a position that only moves
        // forward, so neither can hand over the same event twice.
        assertThat(state).hasSizeLessThanOrEqualTo(6);
    }

    @RepeatedIfExceptionsTest(repeats = 2)
    void redis_blocking_spring_subscription_allows_cancelling_subscription() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        String subscriberId = UUID.randomUUID().toString();
        redisSubscription.subscribe(subscriberId, state::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined1));
        // The subscription is async so we need to wait for it
        await().atMost(ONE_SECOND).until(Not.not(state::isEmpty));
        redisSubscription.cancelSubscription(subscriberId);

        // Then
        assertThat(requireNonNull(redisTemplate.keys("*")).size()).isZero();
    }

    @Test
    void construction_refuses_a_redis_operations_whose_key_serializer_wraps_a_string_instead_of_passing_it_through() {
        // RedisTemplate's own default. Java serialization wraps a key in a class descriptor and a length-prefixed
        // envelope before the string's own bytes, so a brace this class places at one position in the text is no
        // longer at a matching position in what Cluster actually hashes.
        @SuppressWarnings("unchecked")
        RedisOperations<String, String> redis = mock(RedisOperations.class);
        when(redis.getKeySerializer()).thenReturn((RedisSerializer) RedisSerializer.java());

        assertThatThrownBy(() -> new SpringRedisCheckpointStorage(redis))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("UTF-8 bytes");
    }

    @Test
    void construction_refuses_a_redis_operations_with_no_key_serializer_configured_at_all() {
        @SuppressWarnings("unchecked")
        RedisOperations<String, String> redis = mock(RedisOperations.class);

        assertThatThrownBy(() -> new SpringRedisCheckpointStorage(redis))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("UTF-8 bytes");
    }

    @Test
    void a_refused_conditional_write_escapes_retry_immediately_instead_of_hanging_the_calling_thread() {
        // Given a retry strategy whose backoff is longer than this test's own timeout and whose max attempts is the
        // exponentialBackoff default, infinite. If the refusal below were retried even once, this test would still
        // be waiting out that backoff when assertTimeoutPreemptively gives up, since the write can never succeed.
        CheckpointStorage storage = new SpringRedisCheckpointStorage(redisTemplate, RetryStrategy.exponentialBackoff(Duration.ofSeconds(30), Duration.ofSeconds(30), 1.0f));
        String subscriptionId = UUID.randomUUID().toString();
        storage.save(subscriptionId, new StringBasedCheckpoint("first"), CheckpointWriteCondition.notOlderThan(5));

        assertTimeoutPreemptively(Duration.ofSeconds(2), () ->
                assertThatThrownBy(() -> storage.save(subscriptionId, new StringBasedCheckpoint("stale"), CheckpointWriteCondition.notOlderThan(1)))
                        .as("a version below the stored one must be refused immediately, not queued behind a 30 second backoff")
                        .isInstanceOf(CheckpointWriteConditionNotFulfilledException.class));
    }

    @Test
    void a_cluster_crossslot_failure_escapes_retry_immediately_instead_of_hanging_the_calling_thread() {
        // Given a retry strategy whose backoff is longer than this test's own timeout and whose max attempts is the
        // exponentialBackoff default, infinite. If a CROSSSLOT failure were retried even once, this test would still
        // be waiting out that backoff when assertTimeoutPreemptively gives up, since the write can never succeed.
        // No test container here runs Cluster mode, so the failure is injected on a mocked RedisOperations instead
        // of provoked from a real one.
        @SuppressWarnings("unchecked")
        RedisOperations<String, String> redis = mock(RedisOperations.class);
        when(redis.getValueSerializer()).thenReturn((RedisSerializer) RedisSerializer.string());
        when(redis.getKeySerializer()).thenReturn((RedisSerializer) RedisSerializer.string());
        // The outer message is the generic Spring wrapper text, not the CROSSSLOT one, so this only passes if the
        // cause chain is actually walked down to the driver exception. The two messages being identical here once
        // let a broken walk (checking only the outer exception) pass anyway.
        RuntimeException crossSlot = new RedisSystemException("Redis exception",
                new RedisCommandExecutionException("CROSSSLOT Keys in request don't hash to the same slot"));
        // The last matcher is typed Object[], not Object, because saveConditionally passes an already-built array
        // as the vararg parameter. A plain any() matches one vararg element, which this call never has exactly
        // one of, so the stub would silently miss and the mock would return null instead of throwing.
        when(redis.execute(any(RedisScript.class), any(), any(), anyList(), any(Object[].class))).thenThrow(crossSlot);

        CheckpointStorage storage = new SpringRedisCheckpointStorage(redis, RetryStrategy.exponentialBackoff(Duration.ofSeconds(30), Duration.ofSeconds(30), 1.0f));

        assertTimeoutPreemptively(Duration.ofSeconds(2), () ->
                assertThatThrownBy(() -> storage.save(UUID.randomUUID().toString(), new StringBasedCheckpoint("first"), CheckpointWriteCondition.notOlderThan(1)))
                        .as("a Cluster CROSSSLOT failure must be refused immediately, not queued behind a 30 second backoff")
                        .isSameAs(crossSlot));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "a}b{c", "{}orders"})
    void refuses_a_conditional_save_for_a_subscription_id_cluster_cannot_align(String subscriptionId) {
        // Each of these falls back to hashing itself whole, empty or containing a closing brace of its own, so
        // Cluster would hash the checkpoint key and this storage's version key for it to different slots. Refused
        // here rather than left to surface as Cluster's crossed-slots error two calls downstream.
        CheckpointStorage storage = new SpringRedisCheckpointStorage(redisTemplate);

        assertThatThrownBy(() -> storage.save(subscriptionId, new StringBasedCheckpoint("first"), CheckpointWriteCondition.notOlderThan(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be used with a conditional write");
    }

    /**
     * Every conditional-save test elsewhere in this class uses a brace-free {@link UUID}, so a predicate broadened
     * to refuse every brace-carrying id, not just the ones Cluster genuinely cannot align, would still pass the
     * whole suite. These four are all accepted today, a well-formed tag, a fallback with an opening brace and no
     * closing one, a single unmatched opening brace, and a nested pair. None of them are empty and none of their
     * {@code clusterHashTag} results contain a closing brace, so {@code requireClusterSlotAlignable} lets all four
     * through.
     */
    @ParameterizedTest
    @ValueSource(strings = {"{tenant}-orders", "a{b", "{", "a{b{c}d}e"})
    void accepts_a_conditional_save_for_a_subscription_id_cluster_can_align(String subscriptionId) {
        CheckpointStorage storage = new SpringRedisCheckpointStorage(redisTemplate);

        assertThatCode(() -> storage.save(subscriptionId, new StringBasedCheckpoint("first"), CheckpointWriteCondition.notOlderThan(1)))
                .doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "a}b{c", "{}orders"})
    void an_unconditional_save_accepts_a_subscription_id_a_conditional_write_would_refuse(String subscriptionId) {
        // any() never touches the version key, so none of the reasoning that makes a conditional write refuse
        // these ids applies to it. A checkpoint written this way still has to be deletable, see the delete() tests.
        CheckpointStorage storage = new SpringRedisCheckpointStorage(redisTemplate);

        assertThatCode(() -> storage.save(subscriptionId, new StringBasedCheckpoint("first")))
                .doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "a}b{c", "{}orders"})
    void deletes_a_subscription_id_a_conditional_write_would_refuse(String subscriptionId) {
        // Standalone Redis has no slot concept, so the multi-key DEL these ids would refuse for crossing slots on
        // Cluster succeeds directly here. The CROSSSLOT fallback path itself is covered by the mocked test below,
        // since provoking a real CROSSSLOT needs a Cluster this module has no container for.
        CheckpointStorage storage = new SpringRedisCheckpointStorage(redisTemplate);
        storage.save(subscriptionId, new StringBasedCheckpoint("first"));

        assertThatCode(() -> storage.delete(subscriptionId)).doesNotThrowAnyException();
        assertThat(storage.read(subscriptionId)).isNull();
    }

    @Test
    void deleting_falls_back_to_two_single_key_deletes_when_the_multi_key_delete_hits_a_cluster_crossslot_failure() {
        // A checkpoint written through an unconditional save for one of the ids a conditional write refuses still
        // has to be deletable on a real Cluster, where the multi-key DEL these two keys would go through refuses
        // for crossing slots. Two single-key deletes always succeed regardless of slot.
        @SuppressWarnings("unchecked")
        RedisOperations<String, String> redis = mock(RedisOperations.class);
        when(redis.getValueSerializer()).thenReturn((RedisSerializer) RedisSerializer.string());
        when(redis.getKeySerializer()).thenReturn((RedisSerializer) RedisSerializer.string());
        RuntimeException crossSlot = new RedisSystemException("Redis exception",
                new RedisCommandExecutionException("CROSSSLOT Keys in request don't hash to the same slot"));
        when(redis.delete(anyList())).thenThrow(crossSlot);
        when(redis.delete(anyString())).thenReturn(true);

        CheckpointStorage storage = new SpringRedisCheckpointStorage(redis);
        String subscriptionId = "";

        assertThatCode(() -> storage.delete(subscriptionId)).doesNotThrowAnyException();

        verify(redis).delete(subscriptionId);
        verify(redis).delete(SpringRedisCheckpointStorage.versionKey(subscriptionId));
    }

    @Test
    void read_save_delete_and_exists_all_refuse_a_subscription_id_that_is_another_subscriptions_version_key() {
        // versionKey("orders") is the exact Redis key this storage stores "orders"'s fencing version under. Using
        // that same text as a different subscription's own id would let a save or delete on that id corrupt
        // "orders"'s stored version, so every entry point that touches the checkpoint key as a Redis key refuses
        // it before Redis is touched.
        CheckpointStorage storage = new SpringRedisCheckpointStorage(redisTemplate);
        String collidingId = SpringRedisCheckpointStorage.versionKey("orders");

        assertThatThrownBy(() -> storage.read(collidingId))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserves for its own version keys");
        assertThatThrownBy(() -> storage.save(collidingId, new StringBasedCheckpoint("first"), CheckpointWriteCondition.any()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserves for its own version keys");
        assertThatThrownBy(() -> storage.delete(collidingId))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserves for its own version keys");
        assertThatThrownBy(() -> storage.exists(collidingId))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserves for its own version keys");
    }

    @Test
    void a_subscription_id_merely_starting_with_the_version_key_prefix_is_refused_too() {
        // The guard is a prefix check, not an exact match against one specific version key, since any text after
        // the prefix could in principle be some other subscription's tag and digest.
        CheckpointStorage storage = new SpringRedisCheckpointStorage(redisTemplate);
        String subscriptionId = "occurrent:checkpoint-version:{whatever}notarealdigest";

        assertThatThrownBy(() -> storage.save(subscriptionId, new StringBasedCheckpoint("first"), CheckpointWriteCondition.any()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserves for its own version keys");
    }

    private List<CloudEvent> serialize(DomainEvent e) {
        return List.of(CloudEventBuilder.v1()
                .withId(e.eventId())
                .withSource(URI.create("http://name"))
                .withType(e.getClass().getName())
                .withTime(TimeConversion.toLocalDateTime(e.timestamp()).atOffset(UTC))
                .withSubject(e.name())
                .withDataContentType("application/json")
                .withData(CheckedFunction.unchecked(objectMapper::writeValueAsBytes).apply(e))
                .build());
    }

    private static RedisOperations<String, String> createRedisTemplate(LettuceConnectionFactory connectionFactory) {
        connectionFactory.afterPropertiesSet();
        RedisTemplate<String, String> redisTemplate = new RedisTemplate<>();
        redisTemplate.setConnectionFactory(connectionFactory);
        redisTemplate.setKeySerializer(RedisSerializer.string());
        redisTemplate.afterPropertiesSet();
        return redisTemplate;
    }

    private static void cancelSubscription(SubscriptionModelWrapper subscriptionModel, String subscriberId) {
        subscriptionModel.getWrappedSubscriptionModelRecursively().cancelSubscription(subscriberId);
    }
}