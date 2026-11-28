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

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.blocking.internal.CatchupReadScope;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.subscription.internal.CatchupCheckpoints;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A {@code stop()} that waits for a listener of subscription x, which waits for that stop on the replay thread under the
 * lock of x, gives up after the wait for code a caller supplied and leaves the model counting as stopped. The replay of x
 * parks itself once the listener returns. A {@code resumeSubscription(..)} of another paused subscription that comes in
 * between starts the stopped model, which clears the stopped flag, but it was no resume of x, so x must still park and
 * deliver nothing until it is resumed itself, or until {@code start(true)} resumes what a stop parked. Each give-up takes
 * about 10 s, since the wait cannot be made shorter.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class StreamCatchupStopThatGaveUpKeepsAReplayParkedTest {

    private static final String X = "x";
    private static final String Z = "z";
    // The wait for code a caller supplied is 10 s, so a call that gives up there returns inside this
    private static final long A_BOUND_PAST_THE_LIFECYCLE_WAIT_SECONDS = 25;
    private static final Duration LONG_ENOUGH_FOR_A_REPLAY_TO_DELIVER = Duration.ofSeconds(2);
    private static final Checkpoint LIVE_START = new StringBasedCheckpoint("token-1");
    // Replays from position 1, after the beginning
    private static final Checkpoint STORED = new StringBasedCheckpoint(CatchupCheckpoints.format(1, 1, 3, LIVE_START));

    private final CountDownLatch listenerWaiting = new CountDownLatch(1);
    private final CountDownLatch listenerMayReturn = new CountDownLatch(1);
    private final AtomicBoolean listenerWaited = new AtomicBoolean();
    private final AtomicInteger catchupStarts = new AtomicInteger();
    private final AtomicBoolean actionOfXFailed = new AtomicBoolean();
    private final AtomicReference<CompletableFuture<Void>> stop = new AtomicReference<>();
    private final AtomicReference<Throwable> stopThrew = new AtomicReference<>();
    private final CopyOnWriteArrayList<CloudEvent> eventsOfZ = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<String> idsDeliveredToX = new CopyOnWriteArrayList<>();
    private final List<String> history = new ArrayList<>();
    private volatile BooleanSupplier listenerWaitsNow = () -> false;
    private InMemorySubscriptionModel inMemory;
    private InMemoryEventStore eventStore;
    private GatedEventStore gatedEventStore;
    private InMemoryCheckpointStorage storage;
    private HistoryLosingLiveModel live;
    private StreamCatchupSubscriptionModel model;

    @AfterEach
    void shut_down() {
        listenerMayReturn.countDown();
        if (gatedEventStore != null) {
            gatedEventStore.releaseTheRead();
        }
        if (model == null) {
            return;
        }
        try {
            CompletableFuture.runAsync(() -> {
                model.shutdown();
                inMemory.shutdown();
            }).get(10, TimeUnit.SECONDS);
        } catch (Exception ignored) {
            // A shutdown that hangs is left on its daemon thread, the assertion of the test has already reported it
        }
    }

    @Test
    void a_replay_that_runs_again_from_its_origin_parks_and_delivers_nothing_when_a_resume_of_another_subscription_starts_the_model_after_the_stop_gave_up_and_before_the_listener_returns() throws Exception {
        // Given a model whose listener for x waits for a stop when the replay of x runs again from its origin after the live start was lost
        aModelWhoseReplayOfXRunsAgainFromItsOrigin(() -> catchupStarts.incrementAndGet() == 2);
        // And a paused subscription z, and a history for x to replay
        model.subscribe(Z, StartAt.now(), eventsOfZ::add);
        model.pauseSubscription(Z);
        writeAHistory();
        model.subscribe(X, StartAtTime.beginningOfTime(), event -> idsDeliveredToX.add(event.getId()));
        assertThat(listenerWaiting.await(15, TimeUnit.SECONDS)).as("whether the listener of x was told that the replay runs again and began to wait for the stop").isTrue();
        int deliveredToXBeforeTheStopGaveUp = idsDeliveredToX.size();

        // When the stop gives up, z is resumed so that the model starts, and then the listener returns
        resumeZAfterTheStopGaveUp();
        listenerMayReturn.countDown();

        // Then x delivers nothing more and parks itself
        assertThatXDeliversNothingMoreThan(deliveredToXBeforeTheStopGaveUp);
        eventually(10, () -> assertThat(model.hasParkedReplay(X)).as("whether the replay of x parked itself after the listener returned").isTrue());
        // And z delivers an event that came in meanwhile
        live.deliver(Z, cloudEvent());
        assertThat(eventsOfZ).as("the events z delivered once it was resumed").hasSize(1);

        // And a resume of x delivers its history again from the origin, since nothing stored a position, so no event of the history is lost
        idsDeliveredToX.clear();
        model.resumeSubscription(X);
        eventually(10, () -> assertThat(idsDeliveredToX).as("the events x delivered after its own resume").containsAll(history));
    }

    @Test
    void a_replay_that_a_resume_started_and_that_runs_again_after_a_failure_parks_and_delivers_nothing_when_a_resume_of_another_subscription_starts_the_model_after_the_stop_gave_up_and_before_the_listener_returns() throws Exception {
        // Given a replay of x that a resume started, whose action fails once, so that it runs again after its backoff while its listener waits for a stop
        aResumedReplayOfXWhoseListenerWaitsForAStopWhenItRunsAgain();
        int deliveredToXBeforeTheStopGaveUp = idsDeliveredToX.size();

        // When the stop gives up, z is resumed so that the model starts, and then the listener returns
        resumeZAfterTheStopGaveUp();
        listenerMayReturn.countDown();

        // Then x delivers nothing more and parks itself
        assertThatXDeliversNothingMoreThan(deliveredToXBeforeTheStopGaveUp);
        eventually(10, () -> assertThat(model.hasParkedReplay(X)).as("whether the replay of x parked itself after the listener returned").isTrue());
        // And z delivers an event that came in meanwhile
        live.deliver(Z, cloudEvent());
        assertThat(eventsOfZ).as("the events z delivered once it was resumed").hasSize(1);

        // And a resume of x delivers the events after its stored position, the one its action failed on included, since nothing delivered them
        model.resumeSubscription(X);
        eventually(10, () -> assertThat(idsDeliveredToX).as("the events x delivered after its own resume").containsAll(history.subList(1, 3)));
    }

    @Test
    void a_resume_of_x_made_while_its_replay_waits_for_its_next_read_after_the_listener_returned_is_not_undone_by_the_park_the_stop_that_gave_up_owed() throws Exception {
        // Given a replay of x that a resume started, whose listener waits for a stop when it runs again, and whose next opening of a read scope waits once the listener returned
        aResumedReplayOfXWhoseListenerWaitsForAStopWhenItRunsAgain();
        int deliveredToXBeforeTheStopGaveUp = idsDeliveredToX.size();
        resumeZAfterTheStopGaveUp();
        gatedEventStore.blockTheNextRead();
        listenerMayReturn.countDown();
        assertThat(gatedEventStore.readBlocked.await(15, TimeUnit.SECONDS)).as("whether the replay of x that ran again read the event store once the listener returned").isTrue();

        // When x is resumed while its replay is between the listener and its next check, and then the read returns
        CompletableFuture<Subscription> resumeOfX = CompletableFuture.supplyAsync(() -> model.resumeSubscription(X));
        assertThat(resumeOfX).as("the resume of x made while the replay of x waits for its read").succeedsWithin(15, TimeUnit.SECONDS);
        gatedEventStore.releaseTheRead();

        // Then x delivers the events after its stored position, and is not parked
        assertThat(deliveredToXBeforeTheStopGaveUp).as("the events x delivered before the stop gave up").isZero();
        eventually(10, () -> assertThat(idsDeliveredToX).as("the events x delivered after its own resume").containsAll(history.subList(1, 3)));
        assertThat(model.hasParkedReplay(X)).as("whether the replay of x is parked though x was resumed").isFalse();
    }

    @Test
    void a_replay_that_a_resume_started_and_whose_handover_failed_and_that_has_no_event_left_when_it_runs_again_parks_and_is_not_handed_to_the_live_model_when_a_resume_of_another_subscription_starts_the_model_after_the_stop_gave_up() throws Exception {
        // Given a replay of x that a resume started, whose handover to the live model fails once, so that the replay runs again after its backoff with no event left, while its listener waits for a stop
        aModelWhoseResumedReplayOfXRunsAgainAfterAFailure();
        model.subscribe(Z, StartAt.now(), eventsOfZ::add);
        model.pauseSubscription(Z);
        writeAHistory();
        model.subscribe(X, StartAt.checkpoint(LIVE_START), event -> idsDeliveredToX.add(event.getId()));
        model.pauseSubscription(X);
        storage.save(X, STORED);
        AtomicBoolean handoverFailed = new AtomicBoolean();
        Runnable failTheHandoverOnce = () -> {
            if (handoverFailed.compareAndSet(false, true)) {
                actionOfXFailed.set(true);
                throw new IllegalStateException("the handover of x to the live model fails once");
            }
        };
        live.beforeTheNextResume(failTheHandoverOnce).beforeTheNextResumeAt(failTheHandoverOnce);
        model.resumeSubscription(X);
        assertThat(listenerWaiting.await(15, TimeUnit.SECONDS)).as("whether the listener of x was told that the replay the resume started runs again and began to wait for the stop").isTrue();
        List<String> deliveredBeforeTheStopGaveUp = List.copyOf(idsDeliveredToX);
        assertThat(deliveredBeforeTheStopGaveUp).as("the events x delivered before its handover failed").containsExactlyElementsOf(history.subList(1, 3));

        // When the stop gives up, z is resumed so that the model starts, and then the listener returns
        resumeZAfterTheStopGaveUp();
        listenerMayReturn.countDown();

        // Then x is not handed to the live model, delivers nothing more and parks itself
        try {
            await().during(LONG_ENOUGH_FOR_A_REPLAY_TO_DELIVER).atMost(Duration.ofSeconds(10))
                    .untilAsserted(() -> assertThat(live.isRunning(X)).as("whether the live model runs x though nobody resumed x").isFalse());
        } catch (ConditionTimeoutException e) {
            if (e.getCause() instanceof AssertionError assertion) {
                throw assertion;
            }
            throw e;
        }
        eventually(10, () -> assertThat(model.hasParkedReplay(X)).as("whether the replay of x parked itself after the listener returned").isTrue());
        assertThat(idsDeliveredToX).as("the events x delivered after the stop gave up").containsExactlyElementsOf(deliveredBeforeTheStopGaveUp);

        // And a resume of x hands it to the live model, which then delivers a live event
        model.resumeSubscription(X);
        eventually(10, () -> assertThat(live.isRunning(X)).as("whether the live model runs x after its own resume").isTrue());
        CloudEvent liveEvent = cloudEvent();
        live.deliver(X, liveEvent);
        assertThat(idsDeliveredToX).as("the events x delivered after its own resume").contains(liveEvent.getId());
    }

    @Test
    void a_replay_that_runs_again_from_its_origin_goes_on_delivering_its_history_when_start_true_is_called_after_the_stop_gave_up_and_before_the_listener_returns() throws Exception {
        // Given a model whose listener for x waits for a stop when the replay of x runs again from its origin after the live start was lost
        aModelWhoseReplayOfXRunsAgainFromItsOrigin(() -> catchupStarts.incrementAndGet() == 2);
        writeAHistory();
        model.subscribe(X, StartAtTime.beginningOfTime(), event -> idsDeliveredToX.add(event.getId()));
        assertThat(listenerWaiting.await(15, TimeUnit.SECONDS)).as("whether the listener of x was told that the replay runs again and began to wait for the stop").isTrue();
        assertThatTheStopGaveUp();
        idsDeliveredToX.clear();

        // When start(true) is called while the listener still waits, and then the listener returns
        CompletableFuture<Void> start = CompletableFuture.runAsync(() -> model.start(true));
        assertThat(start).as("start(true) called while the listener of x waits").succeedsWithin(A_BOUND_PAST_THE_LIFECYCLE_WAIT_SECONDS, TimeUnit.SECONDS);
        listenerMayReturn.countDown();

        // Then x is not left parked, it delivers its history
        eventually(15, () -> assertThat(idsDeliveredToX).as("the events x delivered after start(true)").containsAll(history));
        assertThat(model.hasParkedReplay(X)).as("whether the replay of x is parked after start(true)").isFalse();
    }

    private void aResumedReplayOfXWhoseListenerWaitsForAStopWhenItRunsAgain() throws Exception {
        aModelWhoseResumedReplayOfXRunsAgainAfterAFailure();
        model.subscribe(Z, StartAt.now(), eventsOfZ::add);
        model.pauseSubscription(Z);
        writeAHistory();
        AtomicInteger deliveries = new AtomicInteger();
        model.subscribe(X, StartAt.checkpoint(LIVE_START), event -> {
            if (deliveries.incrementAndGet() == 1) {
                actionOfXFailed.set(true);
                throw new IllegalStateException("the action of x fails once");
            }
            idsDeliveredToX.add(event.getId());
        });
        model.pauseSubscription(X);
        storage.save(X, STORED);
        model.resumeSubscription(X);
        assertThat(listenerWaiting.await(15, TimeUnit.SECONDS)).as("whether the listener of x was told that the replay the resume started runs again and began to wait for the stop").isTrue();
    }

    private static void eventually(long seconds, Runnable assertion) {
        try {
            await().atMost(seconds, TimeUnit.SECONDS).untilAsserted(assertion::run);
        } catch (ConditionTimeoutException e) {
            // Awaitility wraps the last failed assertion, which is the one to report
            if (e.getCause() instanceof AssertionError failed) {
                throw failed;
            }
            throw e;
        }
    }

    private void aModelWhoseReplayOfXRunsAgainFromItsOrigin(BooleanSupplier listenerWaitsWhen) {
        inMemory = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(inMemory).withoutStreamPosition();
        live = new HistoryLosingLiveModel().withoutHistoryBack(new StringBasedCheckpoint("live-start-1"));
        model = new StreamCatchupSubscriptionModel(live, eventStore, new CatchupSubscriptionModelConfig(100));
        listenerWaitsNow = listenerWaitsWhen;
        model.listenForCatchup(X, aListenerThatWaitsForAStop());
    }

    private void aModelWhoseResumedReplayOfXRunsAgainAfterAFailure() {
        inMemory = new InMemorySubscriptionModel();
        gatedEventStore = new GatedEventStore(inMemory);
        eventStore = gatedEventStore;
        storage = new InMemoryCheckpointStorage();
        live = new HistoryLosingLiveModel();
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        model = new StreamCatchupSubscriptionModel(live, eventStore, config);
        listenerWaitsNow = actionOfXFailed::get;
        model.listenForCatchup(X, aListenerThatWaitsForAStop());
    }

    private CatchupListener aListenerThatWaitsForAStop() {
        return new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                if (listenerWaitsNow.getAsBoolean() && listenerWaited.compareAndSet(false, true)) {
                    CompletableFuture<Void> stopped = CompletableFuture.runAsync(() -> model.stop());
                    stop.set(stopped);
                    listenerWaiting.countDown();
                    try {
                        stopped.get(A_BOUND_PAST_THE_LIFECYCLE_WAIT_SECONDS, TimeUnit.SECONDS);
                    } catch (ExecutionException e) {
                        stopThrew.set(e.getCause());
                    } catch (TimeoutException e) {
                        stopThrew.set(e);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    awaitLatch(listenerMayReturn);
                }
            }

            @Override
            public void historyRead(Object episode) {
            }
        };
    }

    // The stop gives up, and z is resumed after that and before the listener returns, which starts the model the stop left stopped
    private void resumeZAfterTheStopGaveUp() {
        CompletableFuture<Subscription> resumeOfZ = CompletableFuture.supplyAsync(() -> model.resumeSubscription(Z));
        assertThatTheStopGaveUp();
        assertThat(resumeOfZ).as("the resume of z made while the listener waited").succeedsWithin(A_BOUND_PAST_THE_LIFECYCLE_WAIT_SECONDS, TimeUnit.SECONDS);
        assertThat(model.isPaused(Z)).as("whether z is paused after its resume").isFalse();
        assertThat(model.stopped).as("whether the resume of z started the model the stop left stopped").isFalse();
    }

    private void assertThatXDeliversNothingMoreThan(int delivered) {
        try {
            await().during(LONG_ENOUGH_FOR_A_REPLAY_TO_DELIVER).atMost(Duration.ofSeconds(10))
                    .untilAsserted(() -> assertThat(idsDeliveredToX).as("the events x delivered after the stop gave up and the listener returned, though nobody resumed x").hasSize(delivered));
        } catch (ConditionTimeoutException e) {
            // Awaitility wraps the assertion that held no longer, which is the one to report
            if (e.getCause() instanceof AssertionError assertion) {
                throw assertion;
            }
            throw e;
        }
    }

    private void assertThatTheStopGaveUp() {
        assertThat(stop.get()).as("the stop the listener waited for").failsWithin(A_BOUND_PAST_THE_LIFECYCLE_WAIT_SECONDS, TimeUnit.SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .havingCause().isInstanceOf(IllegalStateException.class)
                .withMessageEndingWith(AbstractCatchupSubscriptionModel.STOPPED_ALL_THE_SAME);
        eventually(5, () -> assertThat(stopThrew.get()).as("what the listener saw the stop throw").isInstanceOf(IllegalStateException.class));
    }

    private void writeAHistory() {
        for (int i = 0; i < 3; i++) {
            CloudEvent event = cloudEvent();
            history.add(event.getId());
            eventStore.write(UUID.randomUUID().toString(), List.of(event));
        }
    }

    // An event store whose next read waits, so a test decides what happens while a replay sits between two checks
    private static final class GatedEventStore extends InMemoryEventStore {
        final CountDownLatch readBlocked = new CountDownLatch(1);
        private final CountDownLatch readMayReturn = new CountDownLatch(1);
        private final AtomicBoolean blocksTheNextRead = new AtomicBoolean();

        GatedEventStore(InMemorySubscriptionModel subscriptionModel) {
            super(subscriptionModel);
        }

        void blockTheNextRead() {
            blocksTheNextRead.set(true);
        }

        void releaseTheRead() {
            readMayReturn.countDown();
        }

        // A replay opens a scope before its first read and its first check, so that is where it waits
        @Override
        public CatchupReadScope openCatchupReadScope(@Nullable String notBefore) {
            waitIfTheNextReadIsBlocked();
            return super.openCatchupReadScope(notBefore);
        }

        private void waitIfTheNextReadIsBlocked() {
            if (blocksTheNextRead.compareAndSet(true, false)) {
                readBlocked.countDown();
                awaitLatch(readMayReturn);
            }
        }
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("latch not released");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static CloudEvent cloudEvent() {
        return CloudEventBuilder.v1().withId(UUID.randomUUID().toString()).withSource(URI.create("urn:test")).withType("test.event").withTime(OffsetDateTime.now()).build();
    }
}
