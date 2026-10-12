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

package org.occurrent.subscription.blocking.durable.catchup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
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
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A subscription held paused with nothing stored, whose id another catch-up instance has since stored a position for,
 * replays from that position when it is resumed. A pause that comes while that replay is still delivering holds back
 * everything written after it until the next resume. Two catch-up instances over one event store and one checkpoint
 * storage stand in for two nodes, both through the dispatching {@link CatchupSubscriptionModel} in stream mode.
 */
@Testcontainers
@Timeout(120)
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupResumeFromStoredPositionStreamModeMongoTest {

    private static final URI SOURCE = URI.create("urn:test");

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private SpringMongoEventStore eventStore;
    private MongoTemplate mongoTemplate;
    private MongoClient mongoClient;
    private String eventCollectionName;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private CatchupSubscriptionModel catchupA;
    private CatchupSubscriptionModel catchupB;

    @BeforeEach
    void create_instances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        eventCollectionName = requireNonNull(connectionString.getCollection());
        MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName(eventCollectionName)
                .transactionConfig(mongoTransactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM)
                .withStreamPosition()
                .build();
        eventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), SOURCE).idMapper(DomainEvent::eventId).build();
    }

    @AfterEach
    void shutdown() {
        if (catchupA != null) {
            catchupA.shutdown();
        }
        if (catchupB != null) {
            catchupB.shutdown();
        }
        mongoClient.close();
    }

    @Test
    void a_pause_through_the_dispatcher_during_the_replay_of_a_resume_from_another_catch_ups_stored_position_holds_back_later_events_until_the_next_resume() throws Exception {
        // Given a history, and two catch-up instances over one checkpoint storage
        SpringMongoCheckpointStorage storage = new SpringMongoCheckpointStorage(mongoTemplate, "storage-" + UUID.randomUUID());
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        catchupA = new CatchupSubscriptionModel(new DurableSubscriptionModel(springModel(), storage), eventStore, config);
        catchupB = new CatchupSubscriptionModel(new DurableSubscriptionModel(springModel(), storage), eventStore, config);

        String subscriptionId = UUID.randomUUID().toString();
        CopyOnWriteArrayList<CloudEvent> receivedByB = new CopyOnWriteArrayList<>();
        AtomicInteger deliveredToA = new AtomicInteger();
        CountDownLatch aIsStalled = new CountDownLatch(1);
        CountDownLatch releaseA = new CountDownLatch(1);
        // A's replay delivers the first event and then stalls inside the handler for the second, so the position it
        // has stored is the one after the first event
        Consumer<CloudEvent> stallingA = event -> {
            if (deliveredToA.incrementAndGet() > 1) {
                aIsStalled.countDown();
                awaitUninterrupted(releaseA);
            }
        };
        // B stalls on the first event it is given after the resume, which is inside the replay
        AtomicBoolean bHasStalled = new AtomicBoolean();
        CountDownLatch bIsStalled = new CountDownLatch(1);
        CountDownLatch releaseB = new CountDownLatch(1);
        Consumer<CloudEvent> stallingB = event -> {
            receivedByB.add(event);
            if (bHasStalled.compareAndSet(false, true)) {
                bIsStalled.countDown();
                awaitUninterrupted(releaseB);
            }
        };

        try {
            append(nameDefined("h1", 0));
            append(nameDefined("h2", 1));
            append(nameDefined("h3", 2));

            // When B holds the subscription paused with nothing stored
            catchupB.subscribePaused(subscriptionId, null, StartAt.subscriptionModelDefault(), stallingB);

            // A replays from the start of the history and stalls after its first event, with that position stored
            catchupA.subscribe(subscriptionId, StartAt.checkpoint(GlobalCheckpoint.of(0)), stallingA);
            assertThat(aIsStalled.await(10, SECONDS)).as("A's replay reaches its second event").isTrue();
            assertThat(GlobalCheckpoint.isGlobalCheckpoint(storage.read(subscriptionId))).as("what A's replay stored is a global checkpoint").isTrue();
            append(nameDefined("afterBRegistered", 3));

            // And B is resumed and paused again while it is delivering. The resume runs off the test thread, since it
            // may return only once the replay has been started.
            CompletableFuture<Void> resumed = CompletableFuture.runAsync(() -> catchupB.resumeSubscription(subscriptionId));
            assertThat(bIsStalled.await(10, SECONDS)).as("B is given an event after the resume").isTrue();
            assertThat(receivedByB).extracting(this::nameOf).as("B stalled on the event after the position A stored, so the pause lands inside the replay").containsExactly("h2");
            assertThat(catchupB.isPaused(subscriptionId)).as("B is not paused while its replay delivers").isFalse();
            assertThat(catchupB.isRunning(subscriptionId)).as("B is running while its replay delivers").isTrue();
            catchupB.pauseSubscription(subscriptionId);
            append(nameDefined("afterPause", 4));
            releaseB.countDown();
            resumed.get(10, SECONDS);

            // Then nothing written after the pause is delivered while the subscription is paused
            await("nothing written after the pause reaches B while it is paused").during(2, SECONDS).atMost(5, SECONDS)
                    .untilAsserted(() -> assertThat(receivedByB).extracting(this::nameOf).doesNotContain("afterPause"));

            // And a second resume delivers it, along with the history after the position A's replay stored
            catchupB.resumeSubscription(subscriptionId);
            await("B delivers what was written after the pause").atMost(10, SECONDS)
                    .untilAsserted(() -> assertThat(receivedByB).extracting(this::nameOf).contains("afterPause"));
            assertThat(receivedByB).extracting(this::nameOf).contains("h2", "h3", "afterBRegistered", "afterPause");
        } finally {
            releaseA.countDown();
            releaseB.countDown();
        }
    }

    private static void awaitUninterrupted(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private SpringMongoSubscriptionModel springModel() {
        return new SpringMongoSubscriptionModel(mongoTemplate, eventCollectionName, TimeRepresentation.RFC_3339_STRING);
    }

    private String nameOf(CloudEvent cloudEvent) {
        return ((NameDefined) cloudEventConverter.toDomainEvent(cloudEvent)).name();
    }

    private NameDefined nameDefined(String name, int secondsAfterStart) {
        return new NameDefined(UUID.randomUUID().toString(), LocalDateTime.of(2026, 1, 1, 0, 0, secondsAfterStart), "name", name);
    }

    private void append(DomainEvent event) {
        eventStore.write(UUID.randomUUID().toString(), cloudEventConverter.toCloudEvents(List.of(event)));
    }
}
