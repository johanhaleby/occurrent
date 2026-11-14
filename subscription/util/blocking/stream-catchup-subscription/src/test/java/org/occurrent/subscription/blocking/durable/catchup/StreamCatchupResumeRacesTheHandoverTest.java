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
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
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
    private final CountDownLatch openPending = new CountDownLatch(1);
    private final CountDownLatch releaseTheOpen = new CountDownLatch(1);
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
        releaseTheOpen.countDown();
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
        boolean resumeEndedWhileTheReplayWasHeld = reached(resumed);
        Checkpoint storedWhileTheReplayWasHeld = storage.read(ID);
        releaseTheReplay.countDown();
        storage.releaseTheResume.countDown();
        Throwable thrown = resumed.orTimeout(WAIT.toSeconds(), TimeUnit.SECONDS).join();
        await().atMost(WAIT).untilAsserted(() -> assertThat(liveModel.openedAt).as("live starts the live model opened").isNotEmpty());

        // Then
        assertAll("the test entered the window, with the replay held between its reconcile and the handover lock and the resume returned or thrown",
                () -> assertThat(resumeEndedWhileTheReplayWasHeld).as("window: the resume returned or threw while the replay was held").isTrue(),
                () -> assertThat(storage.presentPositionSaved).as("window: the resume saved no present position while the replay was held").isNotDone(),
                () -> assertThat(storedWhileTheReplayWasHeld).as("window: stored position while the replay was held is what the replay stored").isEqualTo(storedByTheReplay),
                () -> assertThat(received).as("window: the event committed after the live start was not delivered by the replay, so only a live start at or before it delivers it")
                        .extracting(DomainEvent::eventId).doesNotContain(committedAfterTheLiveStart.eventId())
        );
        assertThat(received).as("events delivered, each of the replayed events once").extracting(DomainEvent::eventId)
                .containsExactlyInAnyOrderElementsOf(replayed.stream().map(DomainEvent::eventId).toList());
        // The catch-up refuses a resume of a subscription its subscribe replay runs for, before it stores anything,
        // and the replay goes live from its own live start
        assertThat(thrown).as("the catch-up refuses a resume of a subscription its subscribe replay runs for")
                .isExactlyInstanceOf(SubscriptionAlreadyRunningException.class);
        assertAll("the resume threw " + thrown + ", the stored position at the end is " + storage.read(ID),
                () -> assertThat(liveModel.openedAt).as("live starts the live model opened")
                        .containsExactly(liveStartOfTheReplay),
                () -> assertThat(storage.read(ID)).as("stored position at the end is the live start the handover of the replay stored").isEqualTo(liveStartOfTheReplay)
        );
    }

    @Test
    void goes_live_from_the_live_start_of_the_replay_and_refuses_a_resume_from_now_that_comes_after_the_handover_while_the_live_model_still_opens() {
        // Given
        liveModel = new LiveStartLosingSubscriptionModel(inMemorySubscriptionModel, true, false, () -> {
        }).neverLosingAnOpen().beforeTheFirstOpen(() -> {
            openPending.countDown();
            awaitLatch(releaseTheOpen);
        });
        catchup = new StreamCatchupSubscriptionModel(liveModel, eventStore, config());
        catchup.subscribe(ID, StartAt.checkpoint(GlobalCheckpoint.of(0)), action());
        assertThat(reached(openPending)).as("the replay handed over and the open of the live model is pending").isTrue();
        Checkpoint liveStartOfTheReplay = liveModel.readLiveStarts.get(0);
        write(nameDefined(LocalDateTime.now(), "committed-after-the-live-start-of-the-replay"));

        // When
        CompletableFuture<Throwable> resumed = CompletableFuture.supplyAsync(() -> {
            storage.theResumeThread = Thread.currentThread();
            return catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.now()));
        });
        boolean resumeEnded = reached(resumed);
        boolean openedBeforeTheResumeWasReleased = !liveModel.openedAt.isEmpty();
        releaseTheOpen.countDown();
        boolean opened = false;
        try {
            await().atMost(WAIT).until(() -> !liveModel.openedAt.isEmpty());
            opened = true;
        } catch (org.awaitility.core.ConditionTimeoutException ignored) {
        }
        storage.releaseTheResume.countDown();
        Throwable thrown = resumed.orTimeout(WAIT.toSeconds(), TimeUnit.SECONDS).join();

        // Then
        boolean openedInTime = opened;
        assertAll("the test entered the window, with the open of the live model pending while the resume ran",
                () -> assertThat(resumeEnded).as("window: the resume returned or threw while the open was pending").isTrue(),
                () -> assertThat(openedBeforeTheResumeWasReleased).as("window: the live model had not opened before the open was released").isFalse(),
                () -> assertThat(openedInTime).as("window: the live model opened once the open was released").isTrue(),
                () -> assertThat(storage.read(ID)).as("window: a position is stored").isNotNull()
        );
        assertAll("the resume threw " + thrown + ", the live model opened at " + liveModel.openedAt + ", the live start of the replay is " + liveStartOfTheReplay,
                () -> assertThat(thrown).as("the resume is refused while the live model still opens the subscription")
                        .isExactlyInstanceOf(SubscriptionAlreadyRunningException.class),
                () -> assertThat(liveModel.openedAt).as("live starts the live model opened").containsExactly(liveStartOfTheReplay),
                () -> assertThat(storage.presentPositionSaved).as("the refused resume saved no present position").isNotDone(),
                () -> assertThat(storage.read(ID)).as("stored position at the end is the live start the handover of the replay stored").isEqualTo(liveStartOfTheReplay)
        );
    }

    @Test
    void goes_live_from_the_live_start_of_the_replay_and_stores_nothing_for_a_resume_from_now_after_a_pause_asked_for_during_the_subscribe_replay() {
        // Given
        catchup = new StreamCatchupSubscriptionModel(new HoldingTheLiveStartCheck(liveModel), eventStore, config());
        catchup.subscribe(ID, StartAt.checkpoint(GlobalCheckpoint.of(0)), action());
        assertThat(reached(replayHeldBeforeTheHandover)).as("the replay reached the check of its live start after the bulk read and the reconcile").isTrue();
        Checkpoint liveStartOfTheReplay = liveModel.readLiveStarts.get(0);
        catchup.pauseSubscription(ID);
        write(nameDefined(LocalDateTime.now(), "committed-after-the-live-start-of-the-replay"));

        // When
        CompletableFuture<Throwable> resumed = CompletableFuture.supplyAsync(() -> {
            storage.theResumeThread = Thread.currentThread();
            return catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.now()));
        });
        boolean resumeEnded = reached(resumed);
        releaseTheReplay.countDown();
        storage.releaseTheResume.countDown();
        Throwable thrown = resumed.orTimeout(WAIT.toSeconds(), TimeUnit.SECONDS).join();
        await().atMost(WAIT).untilAsserted(() -> assertThat(liveModel.openedAt).as("live starts the live model opened").isNotEmpty());

        // Then
        assertAll("the test entered the window, with the replay held and a pause asked for during it when the resume ran",
                () -> assertThat(catchup.isPaused(ID)).as("window: the subscription reads as paused after the pause asked for during the replay").isTrue(),
                () -> assertThat(resumeEnded).as("window: the resume returned or threw while the replay was held").isTrue()
        );
        assertAll("the resume threw " + thrown + ", the stored position at the end is " + storage.read(ID),
                () -> assertThat(liveModel.openedAt).as("live starts the live model opened").containsExactly(liveStartOfTheReplay),
                () -> assertThat(storage.presentPositionSaved).as("the resume saved no present position while the subscribe replay ran").isNotDone(),
                () -> assertThat(storage.read(ID)).as("stored position at the end is the live start the handover of the replay stored").isEqualTo(liveStartOfTheReplay),
                () -> assertThat(thrown).as("the resume is refused by the live model, which does not hold the subscription while its replay runs")
                        .isExactlyInstanceOf(IllegalStateException.class).hasMessage("Subscription " + ID + " is not paused")
        );
    }

    @Test
    void does_not_refuse_a_resume_from_now_with_the_already_running_exception_when_the_subscribe_replay_failed_before_the_live_model_got_the_subscription() {
        // Given
        catchup = new StreamCatchupSubscriptionModel(liveModel, eventStore, config());
        storage.failsTheNextSave.set(true);
        Subscription subscription = catchup.subscribe(ID, StartAt.checkpoint(GlobalCheckpoint.of(0)), action());
        Throwable replayEnded = catchThrowable(() -> subscription.waitUntilStarted(WAIT));
        assertThat(replayEnded).as("window: the subscribe replay ended with the failure of the save, so the subscription did not go live").isNotNull();
        assertThat(liveModel.openedAt).as("window: the live model never opened the subscription").isEmpty();

        Checkpoint storedBeforeTheResume = storage.read(ID);
        assertThat(storedBeforeTheResume).as("window: the failed save left no position stored, so the resume has none to put back").isNull();

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.now()));

        // Then
        assertThat(thrown).as("the resume is not refused by the catch-up but by the live model, which does not hold the subscription")
                .isExactlyInstanceOf(IllegalStateException.class).hasMessage("Subscription " + ID + " is not paused");
        assertThat(storage.read(ID)).as("stored position after the failed resume is the one stored before it").isEqualTo(storedBeforeTheResume);
    }

    @Test
    void does_not_refuse_a_resume_from_now_with_the_already_running_exception_when_the_live_model_refused_the_subscription_at_the_handover() {
        // Given
        liveModel = new LiveStartLosingSubscriptionModel(inMemorySubscriptionModel, true, true, () -> {
        }).neverLosingAnOpen().beforeTheFirstOpen(() -> {
            throw new IllegalStateException("the live model refuses the subscription at the handover");
        });
        catchup = new StreamCatchupSubscriptionModel(liveModel, eventStore, config());
        Subscription subscription = catchup.subscribe(ID, StartAt.checkpoint(GlobalCheckpoint.of(0)), action());
        Throwable replayEnded = catchThrowable(() -> subscription.waitUntilStarted(WAIT));
        assertThat(replayEnded).as("window: the subscribe replay ended with the failure of the handover, so the subscription did not go live").isNotNull();
        assertThat(liveModel.openedAt).as("window: the live model never opened the subscription").isEmpty();

        Checkpoint storedBeforeTheResume = storage.read(ID);

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.now()));

        // Then
        assertThat(thrown).as("the resume is not refused by the catch-up but by the live model, which does not hold the subscription")
                .isExactlyInstanceOf(IllegalStateException.class).hasMessage("Subscription " + ID + " is not paused");
        assertThat(storage.read(ID)).as("stored position after the failed resume is the one stored before it").isEqualTo(storedBeforeTheResume);
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
    // resume of the present position does, until the test lets it go
    private static final class PausingTheResumeAfterItSavesThePresentPosition extends InMemoryCheckpointStorage {
        final CompletableFuture<Void> presentPositionSaved = new CompletableFuture<>();
        final CountDownLatch releaseTheResume = new CountDownLatch(1);
        final AtomicBoolean failsTheNextSave = new AtomicBoolean();
        private final AtomicBoolean paused = new AtomicBoolean();
        volatile @Nullable Thread theResumeThread;

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            if (failsTheNextSave.compareAndSet(true, false)) {
                throw new IllegalStateException("the checkpoint storage failed");
            }
            Checkpoint saved = super.save(subscriptionId, checkpoint, condition);
            if (Thread.currentThread() == theResumeThread && !AbstractCatchupSubscriptionModel.isCatchupCheckpoint(checkpoint) && paused.compareAndSet(false, true)) {
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
