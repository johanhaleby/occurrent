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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A {@code start(true)} nested inside another start, from the live delegate's own {@code start(..)} or from a
 * {@code StartAt.dynamic} function the outer start asks, resumes on a thread that still holds the outer start's model
 * lock. When the catch-up has a takeover pending for a subscription, after the live delegate lost the history from its
 * live start, that resume has to store the takeover's position, which it must not do under the lock. The takeover then
 * runs once the lock is free, so the outer start must return normally, and the subscription must replay what the live
 * delegate lost exactly once. A pause made while the live delegate answers that the catch-up takes the subscription
 * over, or one the live delegate refuses after it paused the subscription for the takeover, keeps it paused too.
 * <p>
 * Each test asserts with a bounded wait on a start run off the test thread, so a start that never returns, or one that
 * throws, fails on its assertion rather than on a timeout.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupNestedStartRunsPendingTakeoverTest {

    private static final String ID = "subscription";

    private InMemorySubscriptionModel inMemory;
    private InMemoryEventStore eventStore;
    private SavesRecordingStorage storage;
    private LiveStartLosingSubscriptionModel liveModel;
    private StreamCatchupSubscriptionModel catchup;
    private final List<String> history = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
    private final CountDownLatch takeoverAsked = new CountDownLatch(1);
    private final CountDownLatch letTheTakeoverThreadGo = new CountDownLatch(1);
    private final CountDownLatch nestedStartReturned = new CountDownLatch(1);
    private final CountDownLatch outerStartReturned = new CountDownLatch(1);
    private final AtomicReference<@Nullable Throwable> nestedStartThrew = new AtomicReference<>();
    private final AtomicReference<@Nullable Throwable> outerStartThrew = new AtomicReference<>();
    private final AtomicBoolean nestedStartMade = new AtomicBoolean();

    @BeforeEach
    void create_a_catch_up_whose_live_model_loses_its_live_start_and_whose_takeover_is_held() {
        inMemory = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(inMemory);
        storage = new SavesRecordingStorage();
        // The first live start the live model opens is lost, and the takeover that follows is held until the test lets it go
        liveModel = new LiveStartLosingSubscriptionModel(inMemory, true, false, () -> {
        }).betweenTheFirstTakeOverPauseAndWhatFollows(() -> {
            takeoverAsked.countDown();
            await().atMost(60, TimeUnit.SECONDS).until(() -> letTheTakeoverThreadGo.getCount() == 0);
        });
        catchup = new StreamCatchupSubscriptionModel(liveModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        for (int i = 1; i <= 3; i++) {
            history.add(write());
        }
    }

    @AfterEach
    void shut_down_without_waiting_for_a_start_that_never_returned() {
        letTheTakeoverThreadGo.countDown();
        shutDownOnADaemonThread(catchup::shutdown);
        shutDownOnADaemonThread(inMemory::shutdown);
    }

    @Test
    @Timeout(60)
    void a_start_nested_in_the_start_of_the_live_delegate_returns_and_runs_the_pending_takeover_once() throws Exception {
        // Given a takeover that is asked for and pending, and a model that is stopped
        givenAPendingTakeoverInAStoppedModel(() -> liveModel.beforeTheNextStart(this::nestedStart));
        String writtenWhilePending = write();

        // When the model is started, and the live delegate's start starts it again
        startOffTheTestThread(true);

        // Then
        assertThatBothStartsReturnedWithoutThrowing();
        assertThatTheTakeoverRanOnceAndReplayedTheLostHistory(writtenWhilePending);
    }

    @Test
    @Timeout(60)
    void a_start_nested_in_a_start_at_function_the_outer_start_asks_returns_and_runs_the_pending_takeover_once() throws Exception {
        // Given a subscription whose start at function stops the model and starts it, a takeover that is asked for and pending, and a model that is stopped
        AtomicBoolean armed = new AtomicBoolean();
        catchup.subscribe(ID, null, StartAt.dynamic(() -> {
            if (armed.get() && nestedStartMade.compareAndSet(false, true)) {
                catchup.stop();
                nestedStart();
            }
            return StartAt.checkpoint(GlobalCheckpoint.of(0));
        }), action());
        awaitTheTakeoverAsked();
        catchup.stop();
        armed.set(true);
        String writtenWhilePending = write();

        // When the model is started
        startOffTheTestThread(true);

        // Then
        assertThatBothStartsReturnedWithoutThrowing();
        assertThatTheTakeoverRanOnceAndReplayedTheLostHistory(writtenWhilePending);
    }

    @Test
    @Timeout(60)
    void a_start_nested_in_a_start_that_resumes_nothing_returns_and_runs_the_pending_takeover_once() throws Exception {
        // Given a takeover that is asked for and pending, and a model that is stopped
        givenAPendingTakeoverInAStoppedModel(() -> liveModel.beforeTheNextStart(this::nestedStart));
        String writtenWhilePending = write();

        // When the model is started without resuming its subscriptions, and the live delegate's start starts it again with
        // resuming, which leaves the nested start the only call that can run the takeover. The takeover starts on a thread
        // of its own once the lock the outer start holds is free, so the outer start doesn't wait for it
        startOffTheTestThread(false);

        // Then
        assertThatBothStartsReturnedWithoutThrowing();
        assertThatTheTakeoverRanOnceAndReplayedTheLostHistory(writtenWhilePending);
    }

    /**
     * A subscription that the start of the live delegate pauses, after it started the model again, stays paused, and
     * replays what the live delegate lost once it is resumed.
     */
    @Test
    @Timeout(60)
    void a_subscription_paused_after_a_nested_start_with_a_pending_takeover_stays_paused_until_it_is_resumed() throws Exception {
        // Given a takeover that is asked for and pending, and a model that is stopped
        givenAPendingTakeoverInAStoppedModel(() -> liveModel.beforeTheNextStart(() -> {
            nestedStart();
            catchup.pauseSubscription(ID);
        }));
        String writtenWhilePending = write();

        // When the model is started without resuming its subscriptions, and the live delegate's start starts it again and then pauses the subscription
        startOffTheTestThread(false);

        // Then the takeover stores where it replays from, and neither replays nor goes live while the subscription is paused
        assertThatBothStartsReturnedWithoutThrowing();
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> assertThat(storage.savedAtTheOriginOfTheReplay()).as("the saves of the position the takeover replays from").hasSize(1));
        letTheTakeoverThreadGo.countDown();
        assertThat(waitUpTo(2000, () -> received.size() > history.size())).as("whether the paused subscription replayed the lost history, got " + received.size() + " events").isFalse();
        assertThat(received).as("the events the paused subscription got").containsExactlyElementsOf(history);
        assertThat(liveModel.openedAt).as("the live starts the live model opened for the paused subscription").isEmpty();

        // And when it is resumed, it replays the history the live delegate lost
        catchup.resumeSubscription(ID);
        assertThatTheTakeoverRanOnceAndReplayedTheLostHistory(writtenWhilePending);
    }

    /**
     * A pause made while the live delegate answers that the catch-up takes the subscription over, before it holds the
     * subscription paused, keeps the subscription paused once the takeover has run, until it is resumed.
     */
    @Test
    @Timeout(60)
    void a_pause_made_before_the_live_delegate_holds_the_subscription_paused_for_the_takeover_keeps_it_paused_until_it_is_resumed() throws Exception {
        // Given a subscription that is paused by the thread that finds its live start lost, before the live delegate holds it paused
        AtomicReference<@Nullable Throwable> pauseThrew = new AtomicReference<>();
        liveModel.beforeTheFirstTakeOverIsHeldPaused(() -> pauseOffTheLiveThread(pauseThrew));
        catchup.subscribe(ID, StartAt.checkpoint(GlobalCheckpoint.of(0)), action());
        awaitTheTakeoverAsked();
        String writtenWhilePending = write();

        // When the live delegate lets the takeover go
        letTheTakeoverThreadGo.countDown();

        // Then the takeover stores where it replays from, and neither replays nor goes live while the subscription is paused
        assertThat(pauseThrew.get()).as("what the pause threw").isNull();
        assertThatThePausedSubscriptionStaysPausedAfterTheTakeover();

        // And when it is resumed, it replays the history the live delegate lost
        catchup.resumeSubscription(ID);
        assertThatTheTakeoverRanOnceAndReplayedTheLostHistory(writtenWhilePending);
    }

    /**
     * A pause that the live delegate refuses, since it paused the subscription for the takeover in between, counts as
     * the pause, and keeps the subscription paused once the takeover has run, until it is resumed.
     */
    @Test
    @Timeout(60)
    void a_pause_the_live_delegate_refuses_after_it_paused_the_subscription_for_the_takeover_keeps_it_paused_until_it_is_resumed() throws Exception {
        // Given a live delegate that refuses a pause of a subscription it holds paused, and holds the thread that found the live start lost
        CountDownLatch liveThreadHeld = new CountDownLatch(1);
        CountDownLatch letTheLiveThreadGo = new CountDownLatch(1);
        liveModel.refusingAPauseOfAHeldSubscription()
                .beforeTheFirstTakeOverIsHeldPaused(() -> {
                    liveThreadHeld.countDown();
                    await().atMost(60, TimeUnit.SECONDS).until(() -> letTheLiveThreadGo.getCount() == 0);
                })
                // Runs inside the pause the test thread makes, so the live delegate holds the subscription paused before it answers
                .beforeTheFirstPause(() -> {
                    letTheLiveThreadGo.countDown();
                    await().atMost(15, TimeUnit.SECONDS).until(() -> takeoverAsked.getCount() == 0);
                });
        catchup.subscribe(ID, StartAt.checkpoint(GlobalCheckpoint.of(0)), action());
        assertThat(liveThreadHeld.await(15, TimeUnit.SECONDS)).as("whether the live delegate answered that the catch-up takes the subscription over").isTrue();
        String writtenWhilePending = write();

        // When the subscription is paused, and the live delegate pauses it for the takeover before it answers the pause
        Throwable pauseThrew = null;
        try {
            catchup.pauseSubscription(ID);
        } catch (Throwable e) {
            pauseThrew = e;
        }

        // Then the takeover stores where it replays from, and neither replays nor goes live while the subscription is paused
        assertThat(pauseThrew).as("what the pause threw").isNull();
        letTheTakeoverThreadGo.countDown();
        assertThatThePausedSubscriptionStaysPausedAfterTheTakeover();

        // And when it is resumed, it replays the history the live delegate lost
        catchup.resumeSubscription(ID);
        assertThatTheTakeoverRanOnceAndReplayedTheLostHistory(writtenWhilePending);
    }

    private void pauseOffTheLiveThread(AtomicReference<@Nullable Throwable> pauseThrew) {
        try {
            catchup.pauseSubscription(ID);
        } catch (Throwable e) {
            pauseThrew.set(e);
        }
    }

    private void assertThatThePausedSubscriptionStaysPausedAfterTheTakeover() {
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> assertThat(storage.savedAtTheOriginOfTheReplay()).as("the saves of the position the takeover replays from").hasSize(1));
        assertThat(waitUpTo(2000, () -> received.size() > history.size())).as("whether the paused subscription replayed the lost history, got " + received.size() + " events").isFalse();
        assertThat(received).as("the events the paused subscription got").containsExactlyElementsOf(history);
        assertThat(liveModel.openedAt).as("the live starts the live model opened for the paused subscription").isEmpty();
    }

    private void givenAPendingTakeoverInAStoppedModel(Runnable beforeTheStart) {
        catchup.subscribe(ID, StartAt.checkpoint(GlobalCheckpoint.of(0)), action());
        awaitTheTakeoverAsked();
        catchup.stop();
        beforeTheStart.run();
    }

    private void awaitTheTakeoverAsked() {
        try {
            assertThat(takeoverAsked.await(15, TimeUnit.SECONDS)).as("whether the live model lost the live start and asked the catch-up to take the subscription over").isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        assertThat(received).as("the history the first replay delivered before the live start was lost").containsExactlyElementsOf(history);
    }

    private void nestedStart() {
        nestedStartMade.set(true);
        try {
            catchup.start(true);
        } catch (Throwable e) {
            nestedStartThrew.set(e);
        } finally {
            nestedStartReturned.countDown();
        }
    }

    private void startOffTheTestThread(boolean resumeSubscriptionsAutomatically) {
        CompletableFuture.runAsync(() -> {
            try {
                catchup.start(resumeSubscriptionsAutomatically);
            } catch (Throwable e) {
                outerStartThrew.set(e);
            } finally {
                outerStartReturned.countDown();
            }
        }, daemonExecutor("outer-start"));
    }

    private void assertThatBothStartsReturnedWithoutThrowing() throws Exception {
        assertThat(nestedStartReturned.await(15, TimeUnit.SECONDS)).as("whether the nested start returned").isTrue();
        assertThat(outerStartReturned.await(15, TimeUnit.SECONDS)).as("whether the outer start returned").isTrue();
        assertThat(nestedStartThrew.get()).as("what the nested start threw").isNull();
        assertThat(outerStartThrew.get()).as("what the outer start threw").isNull();
    }

    // The takeover replays from where the replay that handed over started, so it redelivers the history, and the event
    // written while the takeover was pending, which no live subscription saw, comes with it. It stores the position it
    // replays from once, so a takeover that runs twice, such as one the start made and one the held thread of the live
    // model made, would store it twice and replay twice
    private void assertThatTheTakeoverRanOnceAndReplayedTheLostHistory(String writtenWhilePending) {
        List<String> everyDelivery = Stream.concat(Stream.concat(history.stream(), history.stream()), Stream.of(writtenWhilePending)).toList();
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> assertThat(received).as("the events the subscription got").containsExactlyElementsOf(everyDelivery));
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> assertThat(liveModel.openedAt).as("the live starts the live model opened once the takeover replay handed over").hasSize(1));
        // The held thread of the live model asks for the takeover once more when it is let go, which finds it done
        letTheTakeoverThreadGo.countDown();
        assertThat(waitUpTo(2000, () -> received.size() > everyDelivery.size())).as("whether the subscription replayed again after the takeover").isFalse();
        assertThat(received).as("the events the subscription got").containsExactlyElementsOf(everyDelivery);
        assertThat(storage.savedAtTheOriginOfTheReplay()).as("the saves of the position the takeover replays from").hasSize(1);
        assertThat(liveModel.openedAt).as("the live starts the live model opened").hasSize(1);
        assertThat(liveModel.restartedFromThePresent).as("restarts from the present").isEmpty();
    }

    private Consumer<CloudEvent> action() {
        return event -> received.add(event.getId());
    }

    private String write() {
        CloudEvent event = CloudEventBuilder.v1().withId(UUID.randomUUID().toString()).withSource(URI.create("urn:test")).withType("test.event").withTime(OffsetDateTime.now()).build();
        eventStore.write(UUID.randomUUID().toString(), List.of(event));
        return event.getId();
    }

    private static boolean waitUpTo(long millis, BooleanSupplier condition) {
        long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        while (System.nanoTime() < until) {
            if (condition.getAsBoolean()) {
                return true;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return condition.getAsBoolean();
            }
        }
        return condition.getAsBoolean();
    }

    private static Executor daemonExecutor(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            thread.start();
        };
    }

    private static void shutDownOnADaemonThread(Runnable shutdown) {
        try {
            CompletableFuture.runAsync(shutdown, daemonExecutor("shutdown")).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            // A start that never returned holds the lock, and the daemon threads end with the test JVM
        }
    }

    private static final class SavesRecordingStorage extends InMemoryCheckpointStorage {
        final List<Checkpoint> saves = new CopyOnWriteArrayList<>();

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            saves.add(checkpoint);
            return super.save(subscriptionId, checkpoint, condition);
        }

        // The takeover stores the position it replays from, which no replay stores again, since each stores what it has
        // delivered, and the first event it delivers is position 1
        List<String> savedAtTheOriginOfTheReplay() {
            return saves.stream().map(Checkpoint::asString).filter(saved -> saved.startsWith("position:0;")).toList();
        }
    }
}
