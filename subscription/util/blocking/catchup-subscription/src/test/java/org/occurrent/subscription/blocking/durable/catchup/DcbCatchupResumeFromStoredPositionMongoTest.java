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
import org.jspecify.annotations.Nullable;
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
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
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
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;
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
    private DcbCatchupSubscriptionModel standaloneB;

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
        if (standaloneB != null) {
            standaloneB.shutdown();
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

    @Test
    void a_resume_at_a_position_whose_live_delegate_fails_after_waiting_for_the_action_of_the_dcb_resume_replay_leaves_the_replay_running_and_it_hands_over() throws Exception {
        // Given a DCB history, a standalone DCB catch-up for B whose live delegate fails the next resume, and another catch-up instance over one checkpoint storage
        SpringMongoCheckpointStorage storage = new SpringMongoCheckpointStorage(mongoTemplate, "storage-" + UUID.randomUUID());
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        DcbCriteria criteria = DcbCriteria.tags(Tag.parse("name:1"));
        PauseFailingLiveModel liveOfB = new PauseFailingLiveModel(mongoTemplate, eventCollectionName);
        DurableSubscriptionModel durableB = new DurableSubscriptionModel(liveOfB, storage);
        catchupA = new CatchupSubscriptionModel(new DurableSubscriptionModel(springModel(), storage), eventStore, criteria, config);
        standaloneB = new DcbCatchupSubscriptionModel(durableB, eventStore, criteria, config);

        String subscriptionId = UUID.randomUUID().toString();
        CopyOnWriteArrayList<CloudEvent> receivedByB = new CopyOnWriteArrayList<>();
        AtomicInteger inAction = new AtomicInteger();
        AtomicInteger maxInAction = new AtomicInteger();
        AtomicInteger deliveredToA = new AtomicInteger();
        CountDownLatch aIsStalled = new CountDownLatch(1);
        CountDownLatch releaseA = new CountDownLatch(1);
        Consumer<CloudEvent> stallingA = event -> {
            if (deliveredToA.incrementAndGet() > 1) {
                aIsStalled.countDown();
                awaitUninterrupted(releaseA);
            }
        };
        CountDownLatch inH2 = new CountDownLatch(1);
        CountDownLatch releaseH2 = new CountDownLatch(1);
        Consumer<CloudEvent> stallingB = event -> {
            maxInAction.accumulateAndGet(inAction.incrementAndGet(), Math::max);
            try {
                receivedByB.add(event);
                if (nameOf(event).equals("h2") && inH2.getCount() > 0) {
                    inH2.countDown();
                    awaitUninterrupted(releaseH2);
                }
            } finally {
                inAction.decrementAndGet();
            }
        };

        try {
            appendTagged("name:1", nameDefined("h1"));
            appendTagged("name:1", nameDefined("h2"));
            appendTagged("name:1", nameDefined("h3"));
            standaloneB.subscribePaused(subscriptionId, null, StartAt.subscriptionModelDefault(), stallingB);
            catchupA.subscribe(subscriptionId, StartAt.checkpoint(GlobalCheckpoint.of(0)), stallingA);
            assertThat(aIsStalled.await(10, SECONDS)).as("A's replay reaches its second event").isTrue();
            appendTagged("name:1", nameDefined("afterBRegistered"));
            Subscription handleOfTheReplay = standaloneB.resumeSubscription(subscriptionId);
            assertThat(inH2.await(10, SECONDS)).as("B's replay reaches h2").isTrue();
            liveOfB.failTheNextResumes(1);

            // When the subscription is resumed at the present from another thread, and waits for the action
            Checkpoint present = requireNonNull(durableB.globalCheckpoint());
            RepositionableSubscriptions repositionable = RepositionableSubscriptions.findIn(standaloneB).orElseThrow();
            CompletableFuture<Subscription> repositioning = CompletableFuture.supplyAsync(() -> repositionable.resumeSubscription(subscriptionId, StartAt.checkpoint(present)));
            appendTagged("name:1", nameDefined("duringTheCall"));
            assertThat(catchThrowable(() -> repositioning.get(1, SECONDS))).as("the resume at a position while the replay is in the action").isInstanceOf(TimeoutException.class);
            // And the action returns, so the live delegate is asked to resume and fails
            releaseH2.countDown();
            Throwable failure = catchThrowable(() -> repositioning.get(10, SECONDS));
            appendTagged("name:1", nameDefined("afterTheFailedCall"));

            // Then what the live delegate threw reaches the caller
            assertThat(failure).as("the resume at a position").isInstanceOf(ExecutionException.class)
                    .cause().isInstanceOf(IllegalStateException.class).hasMessageContaining("The live delegate is unavailable");
            // And the subscription runs, with its replay as it was
            assertThat(standaloneB.isRunning(subscriptionId)).as("running after the failed resume at a position").isTrue();
            assertThat(standaloneB.isPaused(subscriptionId)).as("paused after the failed resume at a position").isFalse();
            // And the replay hands over, so the handle of the first resume starts
            assertThat(handleOfTheReplay.waitUntilStarted(Duration.ofSeconds(15))).as("the handle of the replay started").isTrue();
            // And no event is lost, whether written before the call, during it or after it
            await("B delivers the history and what was written during and after the failed call").atMost(15, SECONDS)
                    .untilAsserted(() -> assertThat(receivedByB).extracting(this::nameOf).contains("h2", "h3", "afterBRegistered", "duringTheCall", "afterTheFailedCall"));
            assertThat(maxInAction.get()).as("the most calls of the action in flight at once").isEqualTo(1);
        } finally {
            releaseA.countDown();
            releaseH2.countDown();
        }
    }

    @Test
    void a_dcb_replay_put_back_after_a_failed_resume_at_a_position_does_not_deliver_the_event_that_was_in_the_action_again() throws Exception {
        // Given B's DCB resume replay is in the action, blocked on h2, and the live delegate fails the next resume
        Scenario scenario = new Scenario();
        try {
            scenario.liveOfB.failTheNextResumes(1);

            // When the subscription is resumed at the present from another thread, and the action returns, so the live delegate fails
            CompletableFuture<Subscription> repositioning = scenario.resumeAtThePresentFromAnotherThread();
            scenario.releaseH2.countDown();
            Throwable failure = catchThrowable(() -> repositioning.get(10, SECONDS));

            // Then the replay is put back and hands over, and delivers the rest of the history
            assertThat(failure).as("the resume at a position").isInstanceOf(ExecutionException.class)
                    .cause().isInstanceOf(IllegalStateException.class).hasMessageContaining("The live delegate is unavailable");
            assertThat(scenario.handleOfTheReplay.waitUntilStarted(Duration.ofSeconds(15))).as("the handle of the replay started").isTrue();
            await("B delivers the rest of the history").atMost(15, SECONDS)
                    .untilAsserted(() -> assertThat(scenario.receivedByB).extracting(this::nameOf).contains("h3", "afterBRegistered"));
            // And h2 was delivered once, since the held replay stores the position of the event in its action, and the replay put back after the failed live resume starts after it
            assertThat(scenario.receivedByB).extracting(this::nameOf).as("what B received").containsOnlyOnce("h2");
        } finally {
            scenario.release();
        }
    }

    @Test
    void a_resume_at_a_position_whose_live_delegate_throws_an_error_after_waiting_for_the_action_of_the_dcb_resume_replay_rethrows_it_and_leaves_the_replay_running() throws Exception {
        // Given B's DCB resume replay is in the action, blocked on h2, and the live delegate throws an error on the next resume
        Scenario scenario = new Scenario();
        try {
            AssertionError error = new AssertionError("The live delegate broke");
            scenario.liveOfB.throwOnTheNextResume(error);

            // When the subscription is resumed at the present from another thread, and the action returns, so the live delegate throws
            CompletableFuture<Subscription> repositioning = scenario.resumeAtThePresentFromAnotherThread();
            scenario.releaseH2.countDown();
            Throwable failure = catchThrowable(() -> repositioning.get(10, SECONDS));

            // Then the error reaches the caller
            assertThat(failure).as("the resume at a position").isInstanceOf(ExecutionException.class).cause().isSameAs(error);
            // And the replay runs as before
            assertThat(standaloneB.isRunning(scenario.subscriptionId)).as("running after the resume at a position threw an error").isTrue();
            assertThat(standaloneB.hasParkedReplay(scenario.subscriptionId)).as("a replay is held after the resume at a position threw an error").isFalse();
            // And it hands over, so the handle of the first resume starts and the rest of the history arrives
            assertThat(scenario.handleOfTheReplay.waitUntilStarted(Duration.ofSeconds(15))).as("the handle of the replay started").isTrue();
            await("B delivers the rest of the history").atMost(15, SECONDS)
                    .untilAsserted(() -> assertThat(scenario.receivedByB).extracting(this::nameOf).contains("h3", "afterBRegistered"));
        } finally {
            scenario.release();
        }
    }

    /**
     * Two nodes over one event store and one checkpoint storage, with B's DCB resume replay in the action on h2 and
     * the position after h1 stored by A, whose replay stalls in its second event.
     */
    private final class Scenario {
        final SpringMongoCheckpointStorage storage = new SpringMongoCheckpointStorage(mongoTemplate, "storage-" + UUID.randomUUID());
        final CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        final DcbCriteria criteria = DcbCriteria.tags(Tag.parse("name:1"));
        final PauseFailingLiveModel liveOfB = new PauseFailingLiveModel(mongoTemplate, eventCollectionName);
        final DurableSubscriptionModel durableB = new DurableSubscriptionModel(liveOfB, storage);
        final String subscriptionId = UUID.randomUUID().toString();
        final CopyOnWriteArrayList<CloudEvent> receivedByB = new CopyOnWriteArrayList<>();
        final CountDownLatch releaseA = new CountDownLatch(1);
        final CountDownLatch releaseH2 = new CountDownLatch(1);
        final Subscription handleOfTheReplay;

        Scenario() throws Exception {
            catchupA = new CatchupSubscriptionModel(new DurableSubscriptionModel(springModel(), storage), eventStore, criteria, config);
            standaloneB = new DcbCatchupSubscriptionModel(durableB, eventStore, criteria, config);
            AtomicInteger deliveredToA = new AtomicInteger();
            CountDownLatch aIsStalled = new CountDownLatch(1);
            Consumer<CloudEvent> stallingA = event -> {
                if (deliveredToA.incrementAndGet() > 1) {
                    aIsStalled.countDown();
                    awaitUninterrupted(releaseA);
                }
            };
            CountDownLatch inH2 = new CountDownLatch(1);
            Consumer<CloudEvent> stallingB = event -> {
                receivedByB.add(event);
                if (nameOf(event).equals("h2") && inH2.getCount() > 0) {
                    inH2.countDown();
                    awaitUninterrupted(releaseH2);
                }
            };
            appendTagged("name:1", nameDefined("h1"));
            appendTagged("name:1", nameDefined("h2"));
            appendTagged("name:1", nameDefined("h3"));
            standaloneB.subscribePaused(subscriptionId, null, StartAt.subscriptionModelDefault(), stallingB);
            catchupA.subscribe(subscriptionId, StartAt.checkpoint(GlobalCheckpoint.of(0)), stallingA);
            assertThat(aIsStalled.await(10, SECONDS)).as("A's replay reaches its second event").isTrue();
            appendTagged("name:1", nameDefined("afterBRegistered"));
            handleOfTheReplay = standaloneB.resumeSubscription(subscriptionId);
            assertThat(inH2.await(10, SECONDS)).as("B's replay reaches h2").isTrue();
        }

        // The subscription is resumed at the present from another thread, which returns once it waits for the action of the replay it holds
        CompletableFuture<Subscription> resumeAtThePresentFromAnotherThread() {
            RepositionableSubscriptions repositionable = RepositionableSubscriptions.findIn(standaloneB).orElseThrow();
            Checkpoint present = requireNonNull(durableB.globalCheckpoint());
            AtomicReference<Thread> repositioningThread = new AtomicReference<>();
            CompletableFuture<Subscription> repositioning = CompletableFuture.supplyAsync(() -> {
                repositioningThread.set(Thread.currentThread());
                return repositionable.resumeSubscription(subscriptionId, StartAt.checkpoint(present));
            });
            await("the call waits inside awaitTheActions").atMost(10, SECONDS).until(() -> {
                Thread thread = repositioningThread.get();
                if (repositioning.isDone()) {
                    return true;
                }
                if (thread == null || (thread.getState() != Thread.State.WAITING && thread.getState() != Thread.State.TIMED_WAITING)) {
                    return false;
                }
                return Arrays.stream(thread.getStackTrace()).anyMatch(frame -> frame.getMethodName().equals("awaitTheActions"));
            });
            return repositioning;
        }

        void release() {
            releaseA.countDown();
            releaseH2.countDown();
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
     * The live delegate as it is, except that it counts the pauses it is asked for and can fail the next one, and can
     * fail a chosen number of resumes.
     */
    private static final class PauseFailingLiveModel extends SpringMongoSubscriptionModel {
        private final AtomicInteger pauseFailuresLeft = new AtomicInteger();
        private final AtomicInteger resumeFailuresLeft = new AtomicInteger();
        private volatile @Nullable Error resumeError;
        final AtomicInteger pauseCalls = new AtomicInteger();

        PauseFailingLiveModel(MongoTemplate mongoTemplate, String eventCollection) {
            super(mongoTemplate, eventCollection, TimeRepresentation.RFC_3339_STRING);
        }

        void failTheNextPause() {
            pauseFailuresLeft.set(1);
        }

        void failTheNextResumes(int times) {
            resumeFailuresLeft.set(times);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            failIfAsked();
            return super.resumeSubscription(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            failIfAsked();
            return super.resumeSubscription(subscriptionId, startAt);
        }

        void throwOnTheNextResume(Error error) {
            resumeError = error;
        }

        private void failIfAsked() {
            Error error = resumeError;
            if (error != null) {
                resumeError = null;
                throw error;
            }
            if (resumeFailuresLeft.getAndUpdate(left -> left > 0 ? left - 1 : 0) > 0) {
                throw new IllegalStateException("The live delegate is unavailable");
            }
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
