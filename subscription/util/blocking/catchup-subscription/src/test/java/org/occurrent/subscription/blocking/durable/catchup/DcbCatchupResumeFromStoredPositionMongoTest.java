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
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A subscription held paused with nothing stored, whose id another catch-up instance has since stored a DCB position
 * for, must deliver every event after that position when it is resumed. Two catch-up instances over one event store and
 * one checkpoint storage stand in for two nodes, with the lease handover reduced to the resume of the second.
 */
@Testcontainers
@Timeout(120)
@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbCatchupResumeFromStoredPositionMongoTest {

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
                .eventStoreCapabilities(STREAM, DCB)
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
    void a_held_paused_dcb_subscription_resumed_after_another_catch_up_stored_its_position_delivers_everything_after_that_position() throws Exception {
        // Given a DCB history, and two catch-up instances over one checkpoint storage
        SpringMongoCheckpointStorage storage = new SpringMongoCheckpointStorage(mongoTemplate, "storage-" + UUID.randomUUID());
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        DcbCriteria criteria = DcbCriteria.tags(Tag.parse("name:1"));
        catchupA = new CatchupSubscriptionModel(new DurableSubscriptionModel(springModel(), storage), eventStore, criteria, config);
        catchupB = new CatchupSubscriptionModel(new DurableSubscriptionModel(springModel(), storage), eventStore, criteria, config);

        String subscriptionId = UUID.randomUUID().toString();
        CopyOnWriteArrayList<CloudEvent> receivedByA = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<CloudEvent> receivedByB = new CopyOnWriteArrayList<>();
        AtomicInteger deliveredToA = new AtomicInteger();
        CountDownLatch aIsStalled = new CountDownLatch(1);
        CountDownLatch releaseA = new CountDownLatch(1);
        // A's replay delivers the first event and then stalls inside the handler for the second, so the position it
        // has stored is the one after the first event
        Consumer<CloudEvent> stallingA = event -> {
            receivedByA.add(event);
            if (deliveredToA.incrementAndGet() > 1) {
                aIsStalled.countDown();
                try {
                    releaseA.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        try {
            appendTagged("name:1", nameDefined("h1"));
            appendTagged("name:1", nameDefined("h2"));
            appendTagged("name:1", nameDefined("h3"));

            // When B holds the subscription paused with nothing stored, at the present of its subscribe call
            catchupB.subscribePaused(subscriptionId, null, StartAt.subscriptionModelDefault(), receivedByB::add);

            // A replays DCB history from position 0 and stalls after its first event, with that position stored
            catchupA.subscribe(subscriptionId, StartAt.checkpoint(GlobalCheckpoint.of(0)), stallingA);
            assertThat(aIsStalled.await(10, SECONDS)).as("A's replay reaches its second event").isTrue();
            assertThat(GlobalCheckpoint.isGlobalCheckpoint(storage.read(subscriptionId))).as("what A's replay stored is a global checkpoint").isTrue();

            appendTagged("name:1", nameDefined("afterBRegistered"));
            appendTagged("name:1", nameDefined("beforeResume"));

            catchupB.resumeSubscription(subscriptionId);
            appendTagged("name:1", nameDefined("sentinel"));
            await("B delivers the sentinel").atMost(10, SECONDS)
                    .untilAsserted(() -> assertThat(receivedByB).extracting(this::nameOf).contains("sentinel"));

            // Then everything after the position A's replay stored reaches B, the history A had not yet delivered
            // included. Duplicates are fine.
            assertThat(receivedByB).extracting(this::nameOf).contains("h2", "h3", "afterBRegistered", "beforeResume", "sentinel");
        } finally {
            releaseA.countDown();
        }
    }

    @Test
    void a_pause_during_the_dcb_resume_replay_leaves_the_live_delegate_paused_without_asking_it_to_pause_again() throws Exception {
        // Given a DCB history, two catch-up instances over one checkpoint storage, and a live delegate of B that would fail a pause
        SpringMongoCheckpointStorage storage = new SpringMongoCheckpointStorage(mongoTemplate, "storage-" + UUID.randomUUID());
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        DcbCriteria criteria = DcbCriteria.tags(Tag.parse("name:1"));
        PauseFailingLiveModel liveOfB = new PauseFailingLiveModel(mongoTemplate, eventCollectionName);
        DurableSubscriptionModel durableB = new DurableSubscriptionModel(liveOfB, storage);
        catchupA = new CatchupSubscriptionModel(new DurableSubscriptionModel(springModel(), storage), eventStore, criteria, config);
        catchupB = new CatchupSubscriptionModel(durableB, eventStore, criteria, config);

        String subscriptionId = UUID.randomUUID().toString();
        CopyOnWriteArrayList<CloudEvent> receivedByB = new CopyOnWriteArrayList<>();
        AtomicInteger deliveredToA = new AtomicInteger();
        CountDownLatch aIsStalled = new CountDownLatch(1);
        CountDownLatch releaseA = new CountDownLatch(1);
        Consumer<CloudEvent> stallingA = event -> {
            if (deliveredToA.incrementAndGet() > 1) {
                aIsStalled.countDown();
                awaitUninterrupted(releaseA);
            }
        };
        CountDownLatch bIsStalled = new CountDownLatch(1);
        CountDownLatch releaseB = new CountDownLatch(1);
        Consumer<CloudEvent> stallingB = event -> {
            receivedByB.add(event);
            if (nameOf(event).equals("h3") && bIsStalled.getCount() > 0) {
                bIsStalled.countDown();
                awaitUninterrupted(releaseB);
            }
        };

        try {
            appendTagged("name:1", nameDefined("h1"));
            appendTagged("name:1", nameDefined("h2"));
            appendTagged("name:1", nameDefined("h3"));
            catchupB.subscribePaused(subscriptionId, null, StartAt.subscriptionModelDefault(), stallingB);
            catchupA.subscribe(subscriptionId, StartAt.checkpoint(GlobalCheckpoint.of(0)), stallingA);
            assertThat(aIsStalled.await(10, SECONDS)).as("A's replay reaches its second event").isTrue();
            appendTagged("name:1", nameDefined("afterBRegistered"));
            catchupB.resumeSubscription(subscriptionId);
            assertThat(bIsStalled.await(10, SECONDS)).as("B's replay reaches h3").isTrue();
            int pausesAskedBeforeThePause = liveOfB.pauseCalls.get();
            liveOfB.failTheNextPause();

            // When the subscription is paused, and the replay then hands over
            assertThatCode(() -> catchupB.pauseSubscription(subscriptionId)).as("the pause").doesNotThrowAnyException();
            releaseB.countDown();
            appendTagged("name:1", nameDefined("afterPause"));

            // Then what is written after the pause does not reach B
            await("nothing written after the pause reaches B while it is paused").during(3, SECONDS).atMost(6, SECONDS)
                    .untilAsserted(() -> assertThat(receivedByB).extracting(this::nameOf).doesNotContain("afterPause"));
            // And the subscription is paused, and the live delegate holds it paused without having been asked to pause it
            assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is paused").isTrue();
            assertThat(durableB.isPaused(subscriptionId)).as("the live delegate holds the subscription paused").isTrue();
            assertThat(liveOfB.pauseCalls.get() - pausesAskedBeforeThePause).as("the pauses the live delegate was asked for").isZero();

            // And a later resume delivers what was written after the pause
            catchupB.resumeSubscription(subscriptionId);
            await("B delivers what was written after the pause").atMost(10, SECONDS)
                    .untilAsserted(() -> assertThat(receivedByB).extracting(this::nameOf).contains("h2", "h3", "afterBRegistered", "afterPause"));
            assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is not paused after the resume").isFalse();
        } finally {
            releaseA.countDown();
            releaseB.countDown();
        }
    }

    private SpringMongoSubscriptionModel springModel() {
        return new SpringMongoSubscriptionModel(mongoTemplate, eventCollectionName, TimeRepresentation.RFC_3339_STRING);
    }

    private static void awaitUninterrupted(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private String nameOf(CloudEvent cloudEvent) {
        return ((NameDefined) cloudEventConverter.toDomainEvent(cloudEvent)).name();
    }

    private NameDefined nameDefined(String name) {
        return new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", name);
    }

    private void appendTagged(String tag, DomainEvent... events) {
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(List.of(events)).stream()
                .map(event -> DcbCloudEvents.withTags(event, List.of(Tag.parse(tag))))
                .toList();
        eventStore.append(cloudEvents);
    }

    /**
     * The live delegate as it is, except that it counts the pauses it is asked for and can fail the next one.
     */
    private static final class PauseFailingLiveModel extends SpringMongoSubscriptionModel {
        private final AtomicInteger pauseFailuresLeft = new AtomicInteger();
        final AtomicInteger pauseCalls = new AtomicInteger();

        PauseFailingLiveModel(MongoTemplate mongoTemplate, String eventCollection) {
            super(mongoTemplate, eventCollection, TimeRepresentation.RFC_3339_STRING);
        }

        void failTheNextPause() {
            pauseFailuresLeft.set(1);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            pauseCalls.incrementAndGet();
            if (pauseFailuresLeft.getAndUpdate(left -> left > 0 ? left - 1 : 0) > 0) {
                throw new IllegalStateException("The live delegate is unavailable");
            }
            super.pauseSubscription(subscriptionId);
        }
    }
}
