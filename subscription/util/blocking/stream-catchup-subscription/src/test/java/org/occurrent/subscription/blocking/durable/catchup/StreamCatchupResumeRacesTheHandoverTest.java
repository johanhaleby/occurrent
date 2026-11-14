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
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.HistoryLossReportingSubscriptions;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A resume at the present position that runs while a replay a subscribe started is on its way to the handover, which
 * nothing holds, and the live start that replay hands over.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class StreamCatchupResumeRacesTheHandoverTest {

    private static final String ID = "subscription";
    private static final Duration WAIT = Duration.ofSeconds(10);

    private final CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
    private final CountDownLatch replayHeldBeforeTheHandover = new CountDownLatch(1);
    private final CountDownLatch releaseTheReplay = new CountDownLatch(1);
    private InMemorySubscriptionModel inMemorySubscriptionModel;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private InMemoryEventStore eventStore;
    private PausingTheResumeAfterItSavesThePresentPosition storage;
    private LiveStartLosingSubscriptionModel liveModel;
    private StreamCatchupSubscriptionModel catchup;
    private List<NameDefined> replayed;

    @BeforeEach
    void create_instances() {
        inMemorySubscriptionModel = new InMemorySubscriptionModel();
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        LocalDateTime time = LocalDateTime.now();
        replayed = List.of(nameDefined(time, "event1"), nameDefined(time, "event2"), nameDefined(time, "event3"));
        replayed.forEach(this::write);
        storage = new PausingTheResumeAfterItSavesThePresentPosition();
        liveModel = new LiveStartLosingSubscriptionModel(inMemorySubscriptionModel, true, true, () -> {
        }).neverLosingAnOpen();
    }

    @AfterEach
    void shutdown() {
        releaseTheReplay.countDown();
        storage.releaseTheResume.countDown();
        if (catchup != null) {
            catchup.shutdown();
        }
        inMemorySubscriptionModel.shutdown();
    }

    @Test
    void goes_live_from_the_live_start_of_the_replay_and_not_from_the_present_position_a_resume_stored_when_the_resume_fails_after_the_replay_read_it_before_the_handover() {
        // Given
        catchup = new StreamCatchupSubscriptionModel(new HoldingTheLiveStartCheck(liveModel), eventStore, config());
        catchup.subscribe(ID, StartAt.checkpoint(GlobalCheckpoint.of(0)), action());
        assertThat(reached(replayHeldBeforeTheHandover)).as("the replay reached the check of its live start after the bulk read and the reconcile").isTrue();
        Checkpoint liveStartOfTheReplay = liveModel.readLiveStarts.get(0);
        Checkpoint storedByTheReplay = storage.read(ID);
        assertThat(storedByTheReplay).as("position the replay stored").isNotNull().matches(GlobalCheckpoint::isGlobalCheckpoint, "a catch-up position");
        NameDefined committedAfterTheLiveStart = nameDefined(LocalDateTime.now(), "committed-after-the-live-start-of-the-replay");
        write(committedAfterTheLiveStart);

        // When
        CompletableFuture<Throwable> resumed = CompletableFuture.supplyAsync(() -> {
            storage.theResumeThread = Thread.currentThread();
            return catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.now()));
        });
        boolean atItsPauseOrEnd = reached(CompletableFuture.anyOf(storage.presentPositionSaved, resumed));
        boolean presentPositionSaved = storage.presentPositionSaved.isDone();
        Checkpoint storedWhileTheReplayWasHeld = presentPositionSaved ? null : storage.read(ID);
        releaseTheReplay.countDown();
        boolean handoverRead = presentPositionSaved && reached(storage.handoverRead);
        storage.releaseTheResume.countDown();
        Throwable thrown = resumed.orTimeout(WAIT.toSeconds(), TimeUnit.SECONDS).join();
        await().atMost(WAIT).untilAsserted(() -> assertThat(liveModel.openedAt).as("live starts the live model opened").isNotEmpty());

        // Then
        Checkpoint presentPosition = storage.presentPosition;
        assertAll("the test entered the window, with the replay held between its reconcile and the handover lock and the resume at its end or paused after a save",
                () -> assertThat(atItsPauseOrEnd).as("window: the resume ran to its end or to its pause after saving a position while the replay was held").isTrue(),
                () -> assertThat(presentPosition).as("window: any position the resume saved differs from the live start of the replay").isNotEqualTo(liveStartOfTheReplay),
                () -> assertThat(received).as("window: the event committed after the live start was not delivered by the replay, so only a live start at or before it delivers it")
                        .extracting(DomainEvent::eventId).doesNotContain(committedAfterTheLiveStart.eventId())
        );
        if (presentPositionSaved) {
            assertAll("the resume saved the present position",
                    () -> assertThat(handoverRead).as("the handover of the replay read the stored position while the resume was paused after its save").isTrue(),
                    () -> assertThat(storage.readByTheHandover).as("the handover read the present position the resume stored").isEqualTo(presentPosition)
            );
        } else {
            assertThat(storedWhileTheReplayWasHeld).as("stored position after the resume returned and while the replay was held, which is what the replay stored").isEqualTo(storedByTheReplay);
        }
        assertThat(received).as("events delivered, each of the replayed events once").extracting(DomainEvent::eventId)
                .containsExactlyInAnyOrderElementsOf(replayed.stream().map(DomainEvent::eventId).toList());
        // A resume that fails leaves the subscription as it was, so the replay goes live from its own live start. A
        // resume that succeeds at the present position skips what lies before it by its own request
        if (thrown != null) {
            assertAll("the resume threw " + thrown + ", the stored position at the end is " + storage.read(ID) + ", the present position is " + presentPosition,
                    () -> assertThat(liveModel.openedAt).as("live starts the live model opened, and the event committed after the live start of the replay lies between it and the present position")
                            .containsExactly(liveStartOfTheReplay),
                    () -> assertThat(storage.read(ID)).as("stored position at the end").isNotNull().isNotEqualTo(presentPosition)
            );
        }
    }

    private CatchupSubscriptionModelConfig config() {
        return new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
    }

    private Consumer<CloudEvent> action() {
        return cloudEvent -> received.add(cloudEventConverter.toDomainEvent(cloudEvent));
    }

    private static NameDefined nameDefined(LocalDateTime time, String name) {
        return new NameDefined(UUID.randomUUID().toString(), time, "name", name);
    }

    private void write(DomainEvent event) {
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(List.of(event));
        eventStore.write(event.eventId(), cloudEvents);
    }

    // Stores as usual, and pauses the resume thread once it has saved a position that is not a catch-up's, as the
    // resume of the present position does, until the test lets it go. Remembers what the first read made by another
    // thread after that save returned, which is the handover of the replay reading the stored position
    private static final class PausingTheResumeAfterItSavesThePresentPosition extends InMemoryCheckpointStorage {
        final CompletableFuture<Void> presentPositionSaved = new CompletableFuture<>();
        final CountDownLatch handoverRead = new CountDownLatch(1);
        final CountDownLatch releaseTheResume = new CountDownLatch(1);
        private final AtomicBoolean paused = new AtomicBoolean();
        volatile @Nullable Thread theResumeThread;
        volatile @Nullable Checkpoint presentPosition;
        volatile @Nullable Checkpoint readByTheHandover;

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            Checkpoint read = super.read(subscriptionId);
            if (presentPositionSaved.isDone() && Thread.currentThread() != theResumeThread && handoverRead.getCount() == 1) {
                readByTheHandover = read;
                handoverRead.countDown();
            }
            return read;
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            Checkpoint saved = super.save(subscriptionId, checkpoint, condition);
            if (Thread.currentThread() == theResumeThread && !AbstractCatchupSubscriptionModel.isCatchupCheckpoint(checkpoint) && paused.compareAndSet(false, true)) {
                presentPosition = checkpoint;
                presentPositionSaved.complete(null);
                awaitLatch(releaseTheResume);
            }
            return saved;
        }
    }

    private static boolean reached(CompletableFuture<?> future) {
        try {
            future.get(WAIT.toMillis(), TimeUnit.MILLISECONDS);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (java.util.concurrent.TimeoutException e) {
            return false;
        } catch (java.util.concurrent.ExecutionException e) {
            return true;
        }
    }

    private static boolean reached(CountDownLatch latch) {
        try {
            return latch.await(WAIT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(WAIT.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("Not released within " + WAIT);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    // The live model, which holds the replay the first time it is asked whether it still reaches the live start of the
    // replay, which is after the replay read everything and before it takes the handover lock
    private final class HoldingTheLiveStartCheck implements CheckpointAwareSubscriptionModel, HistoryLossReportingSubscriptions, RepositionableSubscriptions {
        private final LiveStartLosingSubscriptionModel delegate;
        private final AtomicBoolean held = new AtomicBoolean();

        private HoldingTheLiveStartCheck(LiveStartLosingSubscriptionModel delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean canResumeFrom(Checkpoint checkpoint) {
            if (held.compareAndSet(false, true)) {
                replayHeldBeforeTheHandover.countDown();
                awaitLatch(releaseTheReplay);
            }
            return delegate.canResumeFrom(checkpoint);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return delegate.globalCheckpoint();
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribe(subscriptionId, filter, startAt, action);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            return delegate.resumeSubscription(subscriptionId, startAt);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return delegate.resumeSubscription(subscriptionId);
        }

        @Override
        public void addHistoryLossListener(HistoryLossListener listener) {
            delegate.addHistoryLossListener(listener);
        }

        @Override
        public void removeHistoryLossListener(HistoryLossListener listener) {
            delegate.removeHistoryLossListener(listener);
        }

        @Override
        public boolean restartsAt(Checkpoint checkpoint) {
            return delegate.restartsAt(checkpoint);
        }

        @Override
        public void stop() {
            delegate.stop();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            delegate.start(resumeSubscriptionsAutomatically);
        }

        @Override
        public boolean isRunning() {
            return delegate.isRunning();
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return delegate.isRunning(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return delegate.isPaused(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            delegate.pauseSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            delegate.cancelSubscription(subscriptionId);
        }
    }
}
