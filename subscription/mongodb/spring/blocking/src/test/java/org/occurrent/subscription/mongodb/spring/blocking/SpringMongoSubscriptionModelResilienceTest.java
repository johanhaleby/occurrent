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

package org.occurrent.subscription.mongodb.spring.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.retry.RetryStrategy.exponentialBackoff;
import static org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig.withConfig;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * Tests ADR 116, "A refused write throws, and it must never be retried", against {@link SpringMongoSubscriptionModel}:
 * a delivery action throwing {@link CheckpointWriteConditionNotFulfilledException} must not be retried on the
 * per-event retry, and the error handler's unbounded restart loop must not run on it either. The subscription it
 * belongs to must stay known and pausable rather than forgotten, and no other subscription on the same model is
 * affected.
 */
@Testcontainers
@Timeout(20)
public class SpringMongoSubscriptionModelResilienceTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion()
            .withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private SpringMongoEventStore mongoEventStore;
    private SpringMongoSubscriptionModel subscriptionModel;
    private ObjectMapper objectMapper;
    private MongoTemplate mongoTemplate;
    private MongoClient mongoClient;
    private String eventCollectionName;
    private TimeRepresentation timeRepresentation;

    @BeforeEach
    void createEventStore() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".resilience");
        mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        this.eventCollectionName = connectionString.getCollection();
        this.timeRepresentation = TimeRepresentation.RFC_3339_STRING;
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(eventCollectionName).transactionConfig(mongoTransactionManager).timeRepresentation(timeRepresentation).eventStoreCapabilities(STREAM, DCB).build();
        mongoEventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
        objectMapper = new ObjectMapper();
    }

    @AfterEach
    void shutdown() {
        if (subscriptionModel != null) {
            subscriptionModel.shutdown();
        }
        mongoClient.close();
    }

    private SpringMongoSubscriptionModel newSubscriptionModel() {
        return new SpringMongoSubscriptionModel(mongoTemplate,
                withConfig(eventCollectionName, timeRepresentation).retryStrategy(exponentialBackoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS), 2)));
    }

    private static CheckpointWriteConditionNotFulfilledException refusal(String subscriptionId) {
        return new CheckpointWriteConditionNotFulfilledException(subscriptionId, OptionalLong.of(5), CheckpointWriteCondition.notOlderThan(3));
    }

    @Nested
    @DisplayName("Checkpoint write refusal (ADR 116)")
    class CheckpointWriteRefusalTest {

        @Test
        void delivery_action_throwing_the_refusal_is_invoked_exactly_once() {
            // Given
            subscriptionModel = newSubscriptionModel();
            String subscriptionId = UUID.randomUUID().toString();
            LocalDateTime now = LocalDateTime.now();
            // A seed event the action must deliver successfully, so the model's tracked change-stream position is a
            // concrete resume token before the target event arrives. Without it, a broken exclusion's restart would
            // resolve "start from now" at restart time and never rediscover the already-past target event, letting
            // this test pass by accident instead of by proving the exclusion holds.
            NameDefined seedEvent = new NameDefined(UUID.randomUUID().toString(), now, "seed", "seed");
            NameDefined targetEvent = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(1), "target", "target");
            AtomicInteger targetInvocations = new AtomicInteger();
            CopyOnWriteArrayList<CloudEvent> delivered = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe(subscriptionId, event -> {
                if (event.getId().equals(targetEvent.eventId())) {
                    targetInvocations.incrementAndGet();
                    throw refusal(subscriptionId);
                }
                delivered.add(event);
            }).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));

            // When
            mongoEventStore.write("1", 0, serialize(seedEvent));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(delivered).hasSize(1));
            mongoEventStore.write("2", 0, serialize(targetEvent));

            // Then: exactly one invocation, and it stays that way well past what a retry backoff, or an unbounded
            // restart loop reopening the change stream from the same concrete position, would allow.
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(targetInvocations.get()).isEqualTo(1));
            // during(), not pollDelay()+atMost(): the latter only needs one truthy poll and returns as soon as it
            // sees one, so a redelivery landing between polls would go unnoticed. during() re-checks continuously
            // and fails the moment the count moves off 1, which is what "stays that way" actually means.
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(3)).untilAsserted(() -> assertThat(targetInvocations.get()).isEqualTo(1));
        }

        @Test
        void subscription_stays_known_and_pausable_and_a_resume_redelivers_the_refused_event() {
            // Given
            subscriptionModel = newSubscriptionModel();
            String subscriptionId = UUID.randomUUID().toString();
            LocalDateTime now = LocalDateTime.now();
            // A seed event the action must deliver successfully, so the model's tracked change-stream position is a
            // concrete resume token before the target event arrives. Without it, the position would still be the
            // unresolved "start from now" it was subscribed with, since a refusal aborts the action before that
            // position is advanced, and a resume would then start from "now" at resume time, after the target event
            // rather than before it.
            NameDefined seedEvent = new NameDefined(UUID.randomUUID().toString(), now, "seed", "seed");
            NameDefined targetEvent = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(1), "target", "target");
            AtomicInteger targetInvocations = new AtomicInteger();
            CopyOnWriteArrayList<CloudEvent> delivered = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe(subscriptionId, event -> {
                if (event.getId().equals(targetEvent.eventId()) && targetInvocations.getAndIncrement() == 0) {
                    throw refusal(subscriptionId);
                }
                delivered.add(event);
            }).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));

            // When
            mongoEventStore.write("1", 0, serialize(seedEvent));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(delivered).hasSize(1));
            mongoEventStore.write("2", 0, serialize(targetEvent));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(targetInvocations.get()).isEqualTo(1));

            // Then: the subscription is still known (pause succeeds rather than throwing), and a resume redelivers
            // the event the refusal aborted.
            subscriptionModel.pauseSubscription(subscriptionId);
            assertThat(subscriptionModel.isPaused(subscriptionId)).isTrue();
            subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));

            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(delivered).extracting(CloudEvent::getId).contains(targetEvent.eventId()));
            assertThat(targetInvocations.get()).isEqualTo(2);
        }

        @Test
        void a_refused_write_on_one_subscription_does_not_stop_delivery_on_another() {
            // Given
            subscriptionModel = newSubscriptionModel();
            String refusedSubscriptionId = UUID.randomUUID().toString();
            String healthySubscriptionId = UUID.randomUUID().toString();
            AtomicInteger refusedInvocations = new AtomicInteger();
            CopyOnWriteArrayList<CloudEvent> healthyState = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe(refusedSubscriptionId, event -> {
                refusedInvocations.incrementAndGet();
                throw refusal(refusedSubscriptionId);
            }).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));
            subscriptionModel.subscribe(healthySubscriptionId, healthyState::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));

            LocalDateTime now = LocalDateTime.now();

            // When
            mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), now, "name", "name1")));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> {
                assertThat(refusedInvocations.get()).isEqualTo(1);
                assertThat(healthyState).hasSize(1);
            });
            mongoEventStore.write("2", 0, serialize(new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(1), "name2", "name2")));

            // Then: the healthy subscription keeps delivering. The refused subscription's underlying change stream
            // is never torn down by this exclusion (only the unbounded restart loop is skipped, see
            // registerNewSpringSubscription), so its still-open cursor keeps handing it new documents as they
            // arrive, unlike the native model where a refusal aborts the whole cursor iteration. Each of those
            // deliveries is refused again on its own merits, one invocation per write rather than a retry storm:
            // exactly two here, matching the two writes, not the many attempts an excluded-from-retry backoff
            // would produce within five seconds.
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(healthyState).hasSize(2));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(refusedInvocations.get()).isEqualTo(2));
        }
    }

    private List<CloudEvent> serialize(DomainEvent e) {
        return List.of(CloudEventBuilder.v1()
                .withId(e.eventId())
                .withSource(URI.create("http://name"))
                .withType(e.getClass().getName())
                .withTime(toLocalDateTime(e.timestamp()).atOffset(UTC))
                .withSubject(e.name())
                .withDataContentType("application/json")
                .withData(unchecked(objectMapper::writeValueAsBytes).apply(e))
                .build());
    }
}
