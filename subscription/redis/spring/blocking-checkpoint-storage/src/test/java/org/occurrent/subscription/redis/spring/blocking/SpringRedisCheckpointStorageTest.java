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
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
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
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.ONE_SECOND;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

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
        redisTemplate.afterPropertiesSet();
        return redisTemplate;
    }

    private static void cancelSubscription(SubscriptionModelWrapper subscriptionModel, String subscriberId) {
        subscriptionModel.getWrappedSubscriptionModelRecursively().cancelSubscription(subscriberId);
    }
}