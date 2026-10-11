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
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
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
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A subscription held paused with nothing stored, whose id another catch-up instance has since stored a position for,
 * replays from that position when it is resumed, or when the model it is made through is started again. Two
 * dispatching {@link CatchupSubscriptionModel} instances in stream mode, over one event store and one checkpoint
 * storage, stand in for two nodes.
 */
@Testcontainers
@Timeout(120)
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupResumeReplayStreamModeMongoTest {

    private static final URI SOURCE = URI.create("urn:test");
    private static final String REPLAY_THREAD_PREFIX = "occurrent-catchup-";

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
    private DurableSubscriptionModel durableB;
    private BlockingStorage storage;
    private FlakyReader readerOfB;
    private FailingLiveModel liveOfB;
    private final CountDownLatch releaseA = new CountDownLatch(1);
    private final String subscriptionId = UUID.randomUUID().toString();
    private final CopyOnWriteArrayList<String> receivedByB = new CopyOnWriteArrayList<>();

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
        storage = new BlockingStorage(new SpringMongoCheckpointStorage(mongoTemplate, "storage-" + UUID.randomUUID()));
        readerOfB = new FlakyReader(eventStore);
        liveOfB = new FailingLiveModel(mongoTemplate, eventCollectionName);
    }

    @AfterEach
    void shutdown() {
        storage.unblock();
        releaseA.countDown();
        if (catchupA != null) {
            catchupA.shutdown();
        }
        if (catchupB != null) {
            catchupB.shutdown();
        }
        mongoClient.close();
    }

    @Test
    void a_pause_through_the_dispatcher_that_comes_while_the_resume_replay_hands_over_leaves_the_subscription_paused() throws Exception {
        // Given B has replayed the history another node's position leaves out, and is about to hand over
        CountDownLatch bIsStalled = new CountDownLatch(1);
        CountDownLatch releaseB = new CountDownLatch(1);
        givenAnotherNodeStoredAPosition(recording("h3", bIsStalled, releaseB));
        CompletableFuture<Subscription> resumed = CompletableFuture.supplyAsync(() -> catchupB.resumeSubscription(subscriptionId));
        assertThat(bIsStalled.await(10, SECONDS)).as("B's replay reaches h3").isTrue();
        storage.blockReadsOn(thread -> thread.getName().startsWith(REPLAY_THREAD_PREFIX));
        releaseB.countDown();
        assertThat(storage.blocked.await(10, SECONDS)).as("the handover reads the storage").isTrue();
        assertThat(catchupB.isCatchingUp(subscriptionId)).as("the replay has ended, and the live delegate is not resumed yet").isFalse();

        // When the subscription is paused in that window, off the test thread since the pause may wait for the handover
        CompletableFuture<Void> pausing = CompletableFuture.runAsync(() -> catchupB.pauseSubscription(subscriptionId));
        waitUpTo(pausing, 1);
        storage.unblock();

        // Then the pause returned without failing
        assertThat(failureOf(pausing)).as("the pause returned without throwing").isNull();
        awaitHandover(resumed);
        // And the subscription is paused, and holds back what is written after the pause
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is paused after the pause returned").isTrue();
        append("afterPause", 4);
        await("nothing written after the pause reaches B while it is paused").during(2, SECONDS).atMost(5, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).doesNotContain("afterPause"));
    }

    @Test
    void start_with_resume_through_the_dispatcher_replays_from_the_position_another_catch_up_stored_as_a_resume_does() throws Exception {
        // Given B holds the subscription paused, and another node stored a position for it
        givenAnotherNodeStoredAPosition(event -> receivedByB.add(nameOf(event)));
        catchupB.stop();

        // When B is started again, resuming what it holds paused
        catchupB.start(true);
        append("afterStart", 4);

        // Then everything after the stored position is delivered, not only what is written from now on
        await("B delivers what is written after the start").atMost(10, SECONDS).until(() -> receivedByB.contains("afterStart"));
        assertThat(receivedByB).as("the history after the stored position").contains("h2", "h3", "afterBRegistered");
    }

    @Test
    void two_resumes_through_the_dispatcher_at_the_same_time_never_run_the_action_concurrently() throws Exception {
        // Given an action that stays in its first call until a second call comes, or a second has passed
        AtomicInteger inAction = new AtomicInteger();
        AtomicInteger maxInAction = new AtomicInteger();
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch firstCall = new CountDownLatch(1);
        CountDownLatch secondCall = new CountDownLatch(1);
        givenAnotherNodeStoredAPosition(event -> {
            maxInAction.accumulateAndGet(inAction.incrementAndGet(), Math::max);
            try {
                receivedByB.add(nameOf(event));
                if (calls.incrementAndGet() == 1) {
                    firstCall.countDown();
                    awaitUpTo(secondCall, 1);
                } else {
                    secondCall.countDown();
                }
            } finally {
                inAction.decrementAndGet();
            }
        });
        // And the second resume reads the storage once the first one's replay is in the action, or a second has passed
        // without that happening. A second resume that waits for the first one never gets as far as the read.
        storage.holdReadsOn(thread -> thread.getName().equals("resume-2"), firstCall);

        // When B is resumed from two threads at once
        Thread first = Thread.ofPlatform().name("resume-1").start(() -> catchupB.resumeSubscription(subscriptionId));
        Thread second = Thread.ofPlatform().name("resume-2").start(() -> catchupB.resumeSubscription(subscriptionId));
        first.join(10_000);
        second.join(10_000);

        // Then the history after the stored position is delivered
        await("B delivers the history after the stored position").atMost(15, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h2", "h3", "afterBRegistered"));
        // And no two calls of the action overlapped
        assertThat(maxInAction.get()).as("the most calls of the action in flight at once").isEqualTo(1);
    }

    @Test
    void a_start_through_the_dispatcher_that_fails_listing_the_replays_a_resume_runs_stops_the_model_again_as_a_start_whose_live_delegate_fails_does() throws Exception {
        // Given B is stopped, holds a subscription paused, and its live delegate fails the first time it is asked if one is paused
        givenAnotherNodeStoredAPosition(event -> receivedByB.add(nameOf(event)));
        catchupB.stop();
        liveOfB.failTheNextIsPausedOn(Thread.currentThread());

        // When B is started, resuming what it holds paused, then the start fails
        assertThatThrownBy(() -> catchupB.start(true))
                .as("the start")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(FailingLiveModel.UNAVAILABLE);

        // Then a replay of a subscription made afterwards is held, as in a stopped model
        String laterId = UUID.randomUUID().toString();
        CopyOnWriteArrayList<String> receivedByLater = new CopyOnWriteArrayList<>();
        catchupB.subscribe(laterId, null, StartAt.checkpoint(GlobalCheckpoint.of(0)), event -> receivedByLater.add(nameOf(event)));
        assertThat(catchupB.isPaused(laterId)).as("the subscription made after the failed start is paused").isTrue();
        await("nothing is delivered to the subscription made after the failed start").during(2, SECONDS).atMost(5, SECONDS)
                .untilAsserted(() -> assertThat(receivedByLater).isEmpty());

        // And a start that succeeds runs both replays
        catchupB.start(true);
        await("B delivers the history of both subscriptions").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByLater).contains("h3"));
        assertThat(receivedByB).as("the history after the stored position").contains("h2", "h3", "afterBRegistered");
    }

    @Test
    void a_resume_at_a_position_through_the_dispatcher_while_a_failed_resume_replay_waits_to_run_again_ends_the_retries() throws Exception {
        // Given the event store fails every read of the resume replay, so it waits to run again
        givenAnotherNodeStoredAPosition(event -> receivedByB.add(nameOf(event)));
        readerOfB.failTheNext(Integer.MAX_VALUE);
        catchupB.resumeSubscription(subscriptionId);
        await("the replay is run again after it failed").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(readerOfB.failures.get()).isGreaterThanOrEqualTo(3));

        // When the subscription is resumed at the present
        catchupB.resumeSubscription(subscriptionId, StartAt.checkpoint(requireNonNull(durableB.globalCheckpoint())));
        // An attempt that was reading when the resume came can still fail once
        await().pollDelay(500, MILLISECONDS).until(() -> true);
        int failuresAtResume = readerOfB.failures.get();

        // Then no replay runs again
        await("no replay is run after the resume at a position").during(3, SECONDS).atMost(6, SECONDS)
                .untilAsserted(() -> assertThat(readerOfB.failures.get()).isEqualTo(failuresAtResume));
        // And the subscription runs from that position
        append("afterResume", 4);
        await("B delivers what is written after the resume").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("afterResume"));
        assertThat(catchupB.isRunning(subscriptionId)).as("the subscription is running").isTrue();
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is not paused").isFalse();
        assertThat(catchupB.isCatchingUp(subscriptionId)).as("the subscription is not catching up").isFalse();
    }

    // B holds the subscription paused with nothing stored. A replays from the start and stalls in its second event,
    // with the position after the first stored. An event is written after B registered.
    private void givenAnotherNodeStoredAPosition(Consumer<CloudEvent> actionOfB) throws Exception {
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        catchupA = new CatchupSubscriptionModel(new DurableSubscriptionModel(springModel(), storage), eventStore, config);
        durableB = new DurableSubscriptionModel(liveOfB, storage);
        catchupB = new CatchupSubscriptionModel(durableB, readerOfB, config);
        AtomicInteger deliveredToA = new AtomicInteger();
        CountDownLatch aIsStalled = new CountDownLatch(1);
        Consumer<CloudEvent> stallingA = event -> {
            if (deliveredToA.incrementAndGet() > 1) {
                aIsStalled.countDown();
                awaitUninterrupted(releaseA);
            }
        };
        append("h1", 0);
        append("h2", 1);
        append("h3", 2);
        catchupB.subscribePaused(subscriptionId, null, StartAt.subscriptionModelDefault(), actionOfB);
        catchupA.subscribe(subscriptionId, StartAt.checkpoint(GlobalCheckpoint.of(0)), stallingA);
        assertThat(aIsStalled.await(10, SECONDS)).as("A's replay reaches its second event").isTrue();
        assertThat(GlobalCheckpoint.isGlobalCheckpoint(storage.read(subscriptionId))).as("what A's replay stored is a global checkpoint").isTrue();
        append("afterBRegistered", 3);
    }

    private Consumer<CloudEvent> recording(String stallOn, CountDownLatch stalled, CountDownLatch release) {
        return event -> {
            String name = nameOf(event);
            receivedByB.add(name);
            if (name.equals(stallOn) && stalled.getCount() > 0) {
                stalled.countDown();
                awaitUninterrupted(release);
            }
        };
    }

    private static void awaitHandover(CompletableFuture<Subscription> resumed) throws Exception {
        ((CatchupSubscription) resumed.get(10, SECONDS)).delegatedSubscription().get(10, SECONDS);
    }

    private static void waitUpTo(CompletableFuture<?> future, int seconds) {
        try {
            future.get(seconds, SECONDS);
        } catch (TimeoutException | ExecutionException ignored) {
            // Looked at by failureOf, once the future has had its time
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // What the future failed with, or null when it completed normally
    private static @Nullable Throwable failureOf(CompletableFuture<?> future) throws InterruptedException, TimeoutException {
        try {
            future.get(10, SECONDS);
            return null;
        } catch (ExecutionException e) {
            return e.getCause();
        }
    }

    private static void awaitUninterrupted(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void awaitUpTo(CountDownLatch latch, int seconds) {
        try {
            latch.await(seconds, SECONDS);
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

    private void append(String name, int secondsAfterStart) {
        DomainEvent event = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.of(2026, 1, 1, 0, 0, secondsAfterStart), "name", name);
        eventStore.write(UUID.randomUUID().toString(), cloudEventConverter.toCloudEvents(List.of(event)));
    }

    /**
     * The live delegate as it is, except that the first question whether a subscription is paused fails on a chosen thread.
     */
    private static final class FailingLiveModel extends SpringMongoSubscriptionModel {
        static final String UNAVAILABLE = "The live delegate is unavailable";
        private volatile @Nullable Thread failIsPausedOn;

        FailingLiveModel(MongoTemplate mongoTemplate, String eventCollection) {
            super(mongoTemplate, eventCollection, TimeRepresentation.RFC_3339_STRING);
        }

        void failTheNextIsPausedOn(Thread thread) {
            failIsPausedOn = thread;
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            if (Thread.currentThread() == failIsPausedOn) {
                failIsPausedOn = null;
                throw new IllegalStateException(UNAVAILABLE);
            }
            return super.isPaused(subscriptionId);
        }
    }

    /**
     * Reads the event store as it is, except that the history read of a replay fails a set number of times.
     */
    private static final class FlakyReader implements EventStoreQueries, PositionOrderedReader {
        private final SpringMongoEventStore delegate;
        private final AtomicInteger failuresLeft = new AtomicInteger();
        final AtomicInteger failures = new AtomicInteger();

        FlakyReader(SpringMongoEventStore delegate) {
            this.delegate = delegate;
        }

        void failTheNext(int times) {
            failuresLeft.set(times);
        }

        @Override
        public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            if (Thread.currentThread().getName().startsWith(REPLAY_THREAD_PREFIX) && failuresLeft.getAndUpdate(left -> left > 0 ? left - 1 : 0) > 0) {
                failures.incrementAndGet();
                throw new IllegalStateException("The event store is unavailable");
            }
            return delegate.readInPositionOrder(filter, range);
        }

        @Override
        public long currentPosition() {
            return delegate.currentPosition();
        }

        @Override
        public boolean writesPosition() {
            return delegate.writesPosition();
        }

        @Override
        public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
            return delegate.query(filter, skip, limit, sortBy);
        }

        @Override
        public long count(Filter filter) {
            return delegate.count(filter);
        }

        @Override
        public boolean exists(Filter filter) {
            return delegate.exists(filter);
        }
    }

    /**
     * Reads the storage as it is, except that a read on a chosen thread either waits to be released or is held back
     * until something else has happened.
     */
    private static final class BlockingStorage implements CheckpointStorage {
        private final CheckpointStorage delegate;
        private volatile Predicate<Thread> blockOn = thread -> false;
        private volatile @Nullable CountDownLatch holdUntil;
        final CountDownLatch blocked = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        BlockingStorage(CheckpointStorage delegate) {
            this.delegate = delegate;
        }

        // A read on such a thread waits until unblock()
        void blockReadsOn(Predicate<Thread> predicate) {
            blockOn = predicate;
        }

        // A read on such a thread waits until the latch is counted down, but no longer than a second
        void holdReadsOn(Predicate<Thread> predicate, CountDownLatch latch) {
            holdUntil = latch;
            blockOn = predicate;
        }

        void unblock() {
            blockOn = thread -> false;
            release.countDown();
        }

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            if (blockOn.test(Thread.currentThread())) {
                CountDownLatch until = holdUntil;
                if (until != null) {
                    awaitUpTo(until, 1);
                } else {
                    blocked.countDown();
                    awaitUninterrupted(release);
                }
            }
            return delegate.read(subscriptionId);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            return delegate.save(subscriptionId, checkpoint, condition);
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return delegate.evaluatesWriteConditions();
        }

        @Override
        public boolean evaluatesWriteConditionsFor(String subscriptionId) {
            return delegate.evaluatesWriteConditionsFor(subscriptionId);
        }

        @Override
        public OptionalLong writeVersion(String subscriptionId) {
            return delegate.writeVersion(subscriptionId);
        }

        @Override
        public void delete(String subscriptionId) {
            delegate.delete(subscriptionId);
        }

        @Override
        public boolean exists(String subscriptionId) {
            return delegate.exists(subscriptionId);
        }

        @Override
        public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate) {
            return delegate.resolveFirstCheckpointRace(subscriptionId, candidate);
        }
    }
}
