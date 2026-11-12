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

package org.occurrent.subscription.blocking.competingconsumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.occurrent.subscription.blocking.durable.catchup.StartAtTime;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoLeaseCompetingConsumerStrategy;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * A competing consumer model over a catch-up model over the native MongoDB model. The native model refuses a dynamic
 * start that answers a catch-up's position, after subscribe(..) returned. The catch-up model answers running for an id
 * while it replays, so a refusal that came during a replay could be mistaken for a start that went on. The start
 * function is asked per model, and the catch-up model hands the native model a position of its own to go live from after
 * a replay, never what the function answers for the native model. So only a start that skips the replay can be refused.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerRefusedStartOverCatchupTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private static final Duration LEASE_TIME = Duration.ofSeconds(3);
    private static final Duration EVENTUALLY = Duration.ofSeconds(15);
    private static final String SUBSCRIPTION_ID = "s";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private MongoClient mongoClient;
    private SpringMongoEventStore eventStore;
    private NativeMongoLeaseCompetingConsumerStrategy strategy;
    private CatchupSubscriptionModel catchup;
    private CompetingConsumerSubscriptionModel competing;
    private final List<CloudEvent> delivered = new CopyOnWriteArrayList<>();

    @BeforeEach
    void a_competing_consumer_model_over_a_catchup_model_over_the_native_model() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        String databaseName = requireNonNull(connectionString.getDatabase());
        String eventCollection = requireNonNull(connectionString.getCollection());
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, databaseName);
        MongoTransactionManager transactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, databaseName));
        eventStore = new SpringMongoEventStore(mongoTemplate, new EventStoreConfig.Builder().eventStoreCollectionName(eventCollection)
                .transactionConfig(transactionManager).timeRepresentation(TimeRepresentation.RFC_3339_STRING).build());
        NativeMongoSubscriptionModel nativeModel = new NativeMongoSubscriptionModel(database, eventCollection, TimeRepresentation.RFC_3339_STRING, Executors.newCachedThreadPool());
        catchup = new CatchupSubscriptionModel(nativeModel, eventStore, new CatchupSubscriptionModelConfig(1000));
        strategy = new NativeMongoLeaseCompetingConsumerStrategy.Builder(database, "locks-" + UUID.randomUUID()).leaseTime(LEASE_TIME).build();
        competing = new CompetingConsumerSubscriptionModel(catchup, strategy);
    }

    @AfterEach
    void shutdown() {
        try {
            competing.shutdown();
        } finally {
            mongoClient.close();
        }
    }

    @Timeout(value = 60, unit = SECONDS)
    @Test
    void a_start_that_skips_the_replay_and_that_the_native_model_refuses_is_reported_by_the_handle_and_frees_the_lease_and_the_subscription_id() {
        // Given a start that the catch-up model passes straight to the native model, which answers a catch-up position for it
        StartAt skipsTheReplayAndIsRefusedByTheNativeModel = StartAt.dynamic(context -> {
            if (context.subscriptionModelType() == CatchupSubscriptionModel.class) {
                return null;
            }
            return context.subscriptionModelType() == NativeMongoSubscriptionModel.class ? StartAt.checkpoint(GlobalCheckpoint.of(0)) : StartAt.now();
        });

        // When
        Subscription subscription = competing.subscribe("A", SUBSCRIPTION_ID, null, skipsTheReplayAndIsRefusedByTheNativeModel, delivered::add);

        // Then
        assertThatThrownBy(() -> subscription.waitUntilStarted(EVENTUALLY))
                .as("[waitUntilStarted answers although the native model refused the start]")
                .isInstanceOf(UnsupportedStartAtException.class);
        assertTheNodeGaveUpTheLeaseAndTheSubscriptionId();
    }

    @Timeout(value = 60, unit = SECONDS)
    @Test
    void a_start_that_replays_history_is_never_handed_to_the_native_model_as_the_position_it_would_refuse_so_nothing_is_refused_during_or_after_the_replay() {
        // Given history to replay and a start that answers the native model a position it would refuse
        eventStore.write("stream", serialize(new NameDefined("history", LocalDateTime.of(2026, 1, 1, 0, 0), "name", "seed")));
        StartAt replaysAndWouldBeRefusedByTheNativeModel = StartAt.dynamic(context -> {
            if (context.subscriptionModelType() == CatchupSubscriptionModel.class) {
                return StartAtTime.beginningOfTime();
            }
            return context.subscriptionModelType() == NativeMongoSubscriptionModel.class ? StartAt.checkpoint(GlobalCheckpoint.of(0)) : StartAt.now();
        });

        // When
        Subscription subscription = competing.subscribe("A", SUBSCRIPTION_ID, null, replaysAndWouldBeRefusedByTheNativeModel, delivered::add);

        // Then it replays, goes live and keeps the lease, and delivers an event written after the replay
        assertThat(subscription.waitUntilStarted(EVENTUALLY)).as("the handle answers that the subscription started").isTrue();
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(ids()).as("[the history is replayed]").containsExactly("history"));
        eventStore.write("stream", serialize(new NameDefined("live", LocalDateTime.of(2026, 1, 1, 0, 0, 1), "name", "after")));
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(ids()).as("[the event written after the replay is delivered live]").containsExactly("history", "live"));
        await().during(LEASE_TIME.multipliedBy(2)).atMost(LEASE_TIME.multipliedBy(2).plus(EVENTUALLY)).untilAsserted(() -> {
            assertThat(strategy.hasLock(SUBSCRIPTION_ID, "A")).as("[A keeps the lease]").isTrue();
            assertThat(competing.isRunning(SUBSCRIPTION_ID)).as("[s runs on A]").isTrue();
            assertThatCode(() -> subscription.waitUntilStarted(Duration.ofMillis(100))).as("[the handle throws no refusal]").doesNotThrowAnyException();
        });
    }

    private void assertTheNodeGaveUpTheLeaseAndTheSubscriptionId() {
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(strategy.hasLock(SUBSCRIPTION_ID, "A"))
                .as("[A still holds the lease after the native model refused the start]").isFalse());
        await().atMost(EVENTUALLY).untilAsserted(() -> {
            assertThat(competing.isRunning(SUBSCRIPTION_ID)).as("[s still runs on A after the refusal]").isFalse();
            assertThat(competing.isPaused(SUBSCRIPTION_ID)).as("[s is still paused on A after the refusal]").isFalse();
            assertThat(catchup.isRunning(SUBSCRIPTION_ID)).as("[the catch-up model still answers running for s after the refusal]").isFalse();
        });
        Subscription again = competing.subscribe("A", SUBSCRIPTION_ID, null, StartAt.now(), delivered::add);
        assertThat(again.waitUntilStarted(EVENTUALLY)).as("the subscription id is free to subscribe again on A").isTrue();
    }

    private List<String> ids() {
        return delivered.stream().map(CloudEvent::getId).toList();
    }

    private static List<CloudEvent> serialize(DomainEvent e) {
        return List.of(CloudEventBuilder.v1()
                .withId(e.eventId())
                .withSource(URI.create("http://name"))
                .withType(e.getClass().getName())
                .withTime(toLocalDateTime(e.timestamp()).atOffset(UTC))
                .withSubject(e.name())
                .withDataContentType("application/json")
                .withData(unchecked(OBJECT_MAPPER::writeValueAsBytes).apply(e))
                .build());
    }
}
