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

package org.occurrent.subscription.blocking.durable.catchup.lifecycle;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.blocking.durable.catchup.StartAtTime;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.CatchupModelMessages.STOPPED_ALL_THE_SAME;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;
import static org.occurrent.subscription.blocking.durable.catchup.lifecycle.GatedHistory.cloudEvent;

/**
 * A checkpoint storage, or a live delegate, that doesn't answer for one subscription holds up that subscription's call alone.
 * Another subscription still delivers, and {@code stop()}, {@code start(true)} and {@code shutdown()} return, within
 * {@link #BOUND} and not after the storage answers. What the held call goes on to do once the storage answers is what the same
 * call made after the stop or the shutdown would have done. It reads again after a stop, and changes nothing after a
 * shutdown.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class CatchupLifecycleSlowStorageTest {

    private static final String X = "x";
    private static final String Y = "y";
    private static final String W = "w";
    private static final String Z = "z";
    private static final Duration BOUND = Duration.ofSeconds(15);

    private final HoldableStorage storage = new HoldableStorage();
    private final ControllableLiveModel live = new ControllableLiveModel();
    private final GatedHistory history = new GatedHistory(cloudEvent("1"));
    private final DurableSubscriptionModel durable = new DurableSubscriptionModel(live, storage);
    private final CatchupSubscriptionModel model = new CatchupSubscriptionModel(durable, history, new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage)));
    private final List<String> deliveredToY = new CopyOnWriteArrayList<>();
    private final CountDownLatch handoverEntered = new CountDownLatch(1);
    private final CountDownLatch handoverMayReturn = new CountDownLatch(1);

    @AfterEach
    void release_everything_and_shut_down() throws Exception {
        storage.releaseAll();
        handoverMayReturn.countDown();
        CompletableFuture.runAsync(model::shutdown, daemonExecutor("shutdown-of-the-model")).get(30, TimeUnit.SECONDS);
    }

    @Test
    void a_stop_returns_and_another_subscription_delivers_while_a_resume_waits_for_the_storage_and_the_resume_then_reads_again_and_takes_effect_after_the_stop() throws Exception {
        // Given x paused, y running, and a resume of x that waits inside the storage read
        givenXPausedAndYRunning();
        Hold hold = storage.holdNextReadOf(X);
        CompletableFuture<Subscription> resume = CompletableFuture.supplyAsync(() -> model.resumeSubscription(X), daemonExecutor("resume-of-x"));
        assertThat(hold.entered.await(10, TimeUnit.SECONDS)).as("whether the resume of x reached the storage read").isTrue();

        // When y receives an event, and a stop is made
        publishToY("live-1");
        CompletableFuture<Void> stop = CompletableFuture.runAsync(model::stop, daemonExecutor("stop-of-the-model"));

        // Then y delivers it and the stop returns, though the storage hasn't answered
        await().atMost(BOUND).untilAsserted(() -> assertThat(deliveredToY).as("the events y delivered while the resume of x waited for the storage").contains("live-1"));
        assertThat(stop).as("the stop made while the resume of x waited for the storage").succeedsWithin(BOUND);
        assertThat(model.isRunning()).as("whether the model runs after the stop").isFalse();
        assertThat(resume.isDone()).as("whether the resume of x is still waiting for the storage").isFalse();

        // And once the storage answers, the resume reads again and takes effect after the stop, as a resume made once the stop returned would
        int readsWhileWaiting = storage.readsOf(X);
        hold.release.countDown();
        assertThat(resume).as("the resume of x once the storage answered").succeedsWithin(BOUND);
        assertThat(storage.readsOf(X)).as("the reads of the storage for x once the resume went on, as a stop came while it read").isGreaterThan(readsWhileWaiting + 1);
        assertThat(model.isRunning()).as("whether the model runs once the resume made after the stop took effect").isTrue();
        assertThat(model.isRunning(X)).as("whether x runs once the resume took effect").isTrue();
    }

    @Test
    void a_stop_made_while_a_resume_reads_a_stopped_model_with_x_paused_is_not_overtaken_by_that_read_and_the_resume_reads_again() throws Exception {
        // Given a stopped model where x is paused, and a resume of x that waits inside the storage read
        givenXPausedAndYRunning();
        model.stop();
        assertThat(model.isRunning()).as("whether the model runs before the test starts").isFalse();
        Hold hold = storage.holdNextReadOf(X);
        CompletableFuture<Subscription> resume = CompletableFuture.supplyAsync(() -> model.resumeSubscription(X), daemonExecutor("resume-of-x"));
        assertThat(hold.entered.await(10, TimeUnit.SECONDS)).as("whether the resume of x reached the storage read").isTrue();
        int readsWhileWaiting = storage.readsOf(X);

        // When another stop is made, and returns, before the storage answers
        assertThat(CompletableFuture.runAsync(model::stop, daemonExecutor("second-stop-of-the-model"))).as("a stop made while the resume of x waited for the storage").succeedsWithin(BOUND);
        hold.release.countDown();
        assertThat(resume).as("the resume of x once the storage answered").succeedsWithin(BOUND);

        // Then the resume read what is stored for x again after that stop, and so did not go on from the read made before it
        assertThat(storage.readsOf(X)).as("the reads of the storage for x after the stop that came while the resume read, which the resume takes effect after").isGreaterThan(readsWhileWaiting + 1);
        assertThat(model.isRunning(X)).as("whether x runs once the resume took effect").isTrue();
    }

    @Test
    void a_start_that_resumes_resumes_the_other_subscriptions_while_the_resume_of_x_waits_for_the_storage() throws Exception {
        // Given x and w paused, a stopped model, and a resume of x that waits inside the storage read
        givenXPausedAndYRunning();
        model.subscribe(W, null, StartAt.now(), event -> {
        });
        model.pauseSubscription(W);
        model.stop();
        Hold hold = storage.holdNextReadOf(X);
        CompletableFuture<Subscription> resume = CompletableFuture.supplyAsync(() -> model.resumeSubscription(X), daemonExecutor("resume-of-x"));
        assertThat(hold.entered.await(10, TimeUnit.SECONDS)).as("whether the resume of x reached the storage read").isTrue();

        // When the model is started, resuming the paused subscriptions
        CompletableFuture<Void> start = CompletableFuture.runAsync(() -> model.start(true), daemonExecutor("start-of-the-model"));

        // Then the start returns, and w is resumed, though the storage hasn't answered for x
        assertThat(start).as("the start made while the resume of x waited for the storage").succeedsWithin(BOUND);
        assertThat(model.isRunning(W)).as("whether w runs once the start returned").isTrue();
        hold.release.countDown();
        assertThat(resume).as("the resume of x once the storage answered, which the start already ran").failsWithin(BOUND)
                .withThrowableOfType(ExecutionException.class)
                .havingCause().isInstanceOf(SubscriptionAlreadyRunningException.class);
        assertThat(model.isRunning(X)).as("whether x runs, the start having resumed it").isTrue();
    }

    @Test
    void a_shutdown_reaches_the_live_delegate_while_a_resume_waits_for_the_storage_and_the_late_resume_does_not_undo_it() throws Exception {
        // Given x paused, and a resume of x that waits inside the storage read
        givenXPausedAndYRunning();
        Hold hold = storage.holdNextReadOf(X);
        CompletableFuture<Subscription> resume = CompletableFuture.supplyAsync(() -> model.resumeSubscription(X), daemonExecutor("resume-of-x"));
        assertThat(hold.entered.await(10, TimeUnit.SECONDS)).as("whether the resume of x reached the storage read").isTrue();

        // When the model is shut down
        assertThat(CompletableFuture.runAsync(model::shutdown, daemonExecutor("shutdown-of-the-model"))).as("the shutdown made while the resume of x waited for the storage").succeedsWithin(BOUND);

        // Then the live delegate got the shutdown, once, and when the storage answers the resume changes nothing
        assertThat(live.shutdowns.get()).as("the shutdowns the live delegate got").isEqualTo(1);
        hold.release.countDown();
        assertThat(catchThrowable(() -> resume.get(BOUND.toSeconds(), TimeUnit.SECONDS))).as("what the resume of x threw once the storage answered, as the model was shut down meanwhile")
                .satisfiesAnyOf(thrown -> assertThat(thrown).isNull(), thrown -> assertThat(thrown).isInstanceOf(ExecutionException.class));
        assertThat(live.isRunning(X)).as("whether x runs in the live delegate once the late resume went on").isFalse();
        assertThat(live.isRunning()).as("whether the live delegate runs once the late resume went on").isFalse();
        assertThat(live.shutdowns.get()).as("the shutdowns the live delegate got in all").isEqualTo(1);
    }

    @Test
    void a_stop_made_while_a_catchup_listener_of_a_replay_waits_returns_and_the_replay_delivers_nothing_once_the_listener_returns_until_x_is_resumed() throws Exception {
        // Given a replay of x whose catch-up listener waits on the replay thread
        CopyOnWriteArrayList<String> deliveredToX = new CopyOnWriteArrayList<>();
        model.listenForCatchup(X, new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                handoverEntered.countDown();
                awaitLatch(handoverMayReturn);
            }

            @Override
            public void historyRead(Object episode) {
            }
        });
        model.subscribe(X, StartAtTime.beginningOfTime(), event -> deliveredToX.add(event.getId()));
        assertThat(handoverEntered.await(20, TimeUnit.SECONDS)).as("whether the listener of x began to wait").isTrue();

        // When a stop is made, and then the listener returns
        assertThat(CompletableFuture.runAsync(model::stop, daemonExecutor("stop-of-the-model"))).as("the stop made while the listener of x waited").succeedsWithin(BOUND);
        handoverMayReturn.countDown();

        // Then the replay of x is parked and delivers nothing, until x is resumed
        await().during(Duration.ofSeconds(2)).atMost(BOUND).untilAsserted(() -> assertThat(deliveredToX).as("the events x delivered after the stop").isEmpty());
        assertThat(model.isRunning()).as("whether the model runs after the stop").isFalse();
        model.resumeSubscription(X);
        await().atMost(BOUND).untilAsserted(() -> assertThat(deliveredToX).as("the events x delivered once it was resumed").containsExactly("1"));
    }

    @Test
    void a_cancel_handed_over_behind_a_resume_while_the_replay_of_the_subscription_waits_for_a_store_holds_up_nothing_but_that_subscription_and_takes_effect_once_the_store_returns() throws Exception {
        // Given z paused, a replay of x waiting inside its checkpoint save, and a live action of y whose cancel of x goes behind a handed resume of z
        model.subscribe(Z, null, StartAt.now(), event -> {
        });
        model.pauseSubscription(Z);
        model.subscribe(Y, null, StartAt.now(), event -> {
            deliveredToY.add(event.getId());
            if (event.getId().equals("live-1")) {
                model.resumeSubscription(Z);
                model.cancelSubscription(X);
            }
        });
        Hold hold = storage.holdNextSaveOf(X);
        model.subscribe(X, StartAtTime.beginningOfTime(), event -> {
        });
        assertThat(hold.entered.await(20, TimeUnit.SECONDS)).as("whether the replay of x reached the storage save").isTrue();

        // When y receives an event whose action hands the cancel of x over, and then its next one
        publishToY("live-1");
        publishToY("live-2");

        // Then y delivers both, as the action returned at once
        await().atMost(BOUND).untilAsserted(() -> assertThat(deliveredToY).as("the events y delivered while the replay of x waited for its save").contains("live-1", "live-2"));
        // And a resume of another paused subscription runs and delivers
        List<String> deliveredToW = new CopyOnWriteArrayList<>();
        model.subscribe(W, null, StartAt.now(), event -> deliveredToW.add(event.getId()));
        model.pauseSubscription(W);
        assertThat(CompletableFuture.supplyAsync(() -> model.resumeSubscription(W), daemonExecutor("resume-of-w"))).as("the resume of w made while the store of the replay of x waited").succeedsWithin(BOUND);
        publishToY("live-3");
        await().atMost(BOUND).untilAsserted(() -> assertThat(deliveredToW).as("the events w delivered once it was resumed").contains("live-3"));
        // And a stop returns within the bound, and a shutdown reaches the live delegate
        Throwable stopThrew = catchThrowable(() -> CompletableFuture.runAsync(model::stop, daemonExecutor("stop-of-the-model")).get(BOUND.toSeconds(), TimeUnit.SECONDS));
        assertThat(stopThrew).as("what the stop made while the store of the replay of x waited threw").satisfiesAnyOf(
                thrown -> assertThat(thrown).isNull(),
                thrown -> assertThat(thrown).isInstanceOf(ExecutionException.class).hasRootCauseInstanceOf(IllegalStateException.class).rootCause().hasMessageEndingWith(STOPPED_ALL_THE_SAME));
        assertThat(CompletableFuture.runAsync(model::shutdown, daemonExecutor("shutdown-of-the-model"))).as("the shutdown made while the store of the replay of x waited").succeedsWithin(BOUND);
        assertThat(live.shutdowns.get()).as("the shutdowns the live delegate got before the store returned").isEqualTo(1);

        // When the store returns, the handed cancel takes effect
        hold.release.countDown();
        await().atMost(BOUND).untilAsserted(() -> assertThat(live.subscriptions).as("the subscriptions of the live delegate once the store returned").doesNotContainKey(X));
        assertThat(live.isRunning(X)).as("whether x runs in the live delegate").isFalse();
        assertThat(model.isRunning()).as("whether the model runs after the shutdown").isFalse();
        assertThat(live.shutdowns.get()).as("the shutdowns the live delegate got in all").isEqualTo(1);
    }

    @Test
    @Timeout(60)
    void a_stop_made_while_the_replay_a_resume_started_waits_inside_the_storage_in_its_handover_gives_up_within_the_bound_and_the_replay_delivers_nothing_more_once_the_storage_answers() throws Exception {
        // Given the replay a resume of x started, which stays current through its handover, waiting for the storage under the handover lock of x
        try (ResumeReplayHeldInItsHandover held = new ResumeReplayHeldInItsHandover()) {
            // When a stop is made from another thread
            CompletableFuture<Void> stop = CompletableFuture.runAsync(held.resumer::stop, daemonExecutor("stop-of-the-model"));

            // Then it gives up within the bound, saying the model is stopped all the same, though the storage hasn't answered
            assertThat(catchThrowable(() -> stop.get(BOUND.toSeconds(), TimeUnit.SECONDS))).as("what the stop made while the handover of x waited for the storage threw")
                    .isInstanceOf(ExecutionException.class).hasRootCauseInstanceOf(IllegalStateException.class).rootCause().hasMessageEndingWith(STOPPED_ALL_THE_SAME);
            assertThat(held.hold.release.getCount()).as("whether the storage still waited when the stop returned").isEqualTo(1);

            // And once the storage answers, x delivers nothing more
            int deliveredBefore = held.deliveredToX.size();
            held.hold.release.countDown();
            await().during(Duration.ofSeconds(2)).atMost(BOUND).untilAsserted(() -> assertThat(held.deliveredToX).as("the events x delivered after the storage answered").hasSize(deliveredBefore));

            // And x stays parked, the live model not having resumed it, so a live event doesn't reach it
            assertThat(held.resumer.isRunning(X)).as("whether x runs on the catch-up model after the stop").isFalse();
            assertThat(live.isRunning(X)).as("whether the live model resumed x after the stop").isFalse();
            CheckpointAwareCloudEvent liveEvent = new CheckpointAwareCloudEvent(cloudEvent("live-1"), new StringBasedCheckpoint("checkpoint-live-1"));
            daemonExecutor("live-delivery-live-1").execute(() -> live.publish(liveEvent));
            await().during(Duration.ofSeconds(2)).atMost(BOUND).untilAsserted(() -> assertThat(held.deliveredToX).as("the events x got while parked").doesNotContain("live-1"));

            // And a resume from the test thread brings x back, so it gets the live event
            held.resumer.resumeSubscription(X);
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(held.deliveredToX).as("the events x got once resumed").contains("live-1"));
        }
    }

    @Test
    @Timeout(60)
    void a_shutdown_made_while_a_stop_waits_for_the_handover_lock_of_a_replay_a_resume_started_returns_within_the_bound_and_reaches_the_live_delegate() throws Exception {
        // Given the replay a resume of x started waiting for the storage in its handover, and a stop that waits for the handover lock of x
        try (ResumeReplayHeldInItsHandover held = new ResumeReplayHeldInItsHandover()) {
            CompletableFuture<Void> stop = CompletableFuture.runAsync(held.resumer::stop, daemonExecutor("stop-of-the-model"));
            await().pollDelay(Duration.ofSeconds(1)).atMost(BOUND).untilAsserted(() -> assertThat(stop.isDone()).as("whether the stop already returned, though the handover of x waits for the storage").isFalse());

            // When a shutdown is made from another thread while the stop waits
            CompletableFuture<Void> shutdown = CompletableFuture.runAsync(held.resumer::shutdown, daemonExecutor("shutdown-of-the-model"));

            // Then the live delegate gets its shutdown without the model lock while the shutdown is still waiting for it
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(live.shutdowns.get()).as("the shutdowns the live delegate got while the shutdown waited").isGreaterThanOrEqualTo(1));
            assertThat(shutdown.isDone()).as("whether the shutdown already returned when the live delegate got its first shutdown").isFalse();

            // And it returns within the bound
            assertThat(shutdown).as("the shutdown made while the stop waited").succeedsWithin(BOUND);
            assertThat(live.shutdowns.get()).as("the shutdowns the live delegate got").isGreaterThanOrEqualTo(1);
            assertThat(catchThrowable(() -> stop.get(BOUND.toSeconds(), TimeUnit.SECONDS))).as("what the stop threw within the bound").satisfiesAnyOf(
                    thrown -> assertThat(thrown).isNull(),
                    thrown -> assertThat(thrown).isInstanceOf(ExecutionException.class).hasRootCauseInstanceOf(IllegalStateException.class));
        }
    }

    // Another node replays x from the start and stalls in its second event, with the position after the first stored, while
    // this node holds x paused. Resuming x here then replays from that position, and the read its handover makes waits
    private final class ResumeReplayHeldInItsHandover implements AutoCloseable {
        final CatchupSubscriptionModel resumer;
        final CatchupSubscriptionModel other;
        final Hold hold;
        final CopyOnWriteArrayList<String> deliveredToX = new CopyOnWriteArrayList<>();

        ResumeReplayHeldInItsHandover() throws Exception {
            CatchupSubscriptionModelConfig persisting = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
            resumer = new CatchupSubscriptionModel(durable, new GatedHistory(cloudEvent("1"), cloudEvent("2"), cloudEvent("3")), persisting);
            other = new CatchupSubscriptionModel(new DurableSubscriptionModel(new ControllableLiveModel(), storage), new GatedHistory(cloudEvent("1"), cloudEvent("2"), cloudEvent("3")), persisting);
            resumer.subscribePaused(X, null, StartAt.subscriptionModelDefault(), event -> deliveredToX.add(event.getId()));
            CountDownLatch otherStalled = new CountDownLatch(1);
            AtomicInteger deliveredToOther = new AtomicInteger();
            other.subscribe(X, StartAtTime.beginningOfTime(), event -> {
                if (deliveredToOther.incrementAndGet() > 1) {
                    otherStalled.countDown();
                    awaitLatch(handoverMayReturn);
                }
            });
            assertThat(otherStalled.await(20, TimeUnit.SECONDS)).as("whether the other node stalled in its second event").isTrue();
            hold = storage.holdTheReadOfAReplayThreadAfter(0);
            CompletableFuture.runAsync(() -> resumer.resumeSubscription(X), daemonExecutor("resume-of-x"));
            assertThat(hold.entered.await(20, TimeUnit.SECONDS)).as("whether the handover of the replay of x reached the storage read").isTrue();
        }

        @Override
        public void close() throws Exception {
            handoverMayReturn.countDown();
            storage.releaseAll();
            CompletableFuture.runAsync(other::shutdown, daemonExecutor("shutdown-of-the-other-node")).get(30, TimeUnit.SECONDS);
            CompletableFuture.runAsync(resumer::shutdown, daemonExecutor("shutdown-of-the-resuming-node")).get(30, TimeUnit.SECONDS);
        }
    }

    @Test
    void a_start_that_resumes_reads_the_stored_position_of_a_subscription_outside_the_lock_so_a_resume_of_another_subscription_and_a_stop_do_not_wait_for_a_read_that_is_held() throws Exception {
        // Given a stopped model where x and w are paused, and a storage that holds every read of x
        List<String> deliveredToW = new CopyOnWriteArrayList<>();
        model.subscribe(W, null, StartAt.now(), event -> deliveredToW.add(event.getId()));
        model.subscribe(X, null, StartAt.now(), event -> {
        });
        model.pauseSubscription(W);
        model.pauseSubscription(X);
        model.stop();
        Hold hold = storage.holdEveryReadOf(X);

        // When start(true) is called on a thread of its own, and waits for the read of x
        CompletableFuture<Void> start = CompletableFuture.runAsync(() -> model.start(true), daemonExecutor("start-of-the-model"));
        assertThat(hold.entered.await(20, TimeUnit.SECONDS)).as("whether the start reached the read of x").isTrue();

        // Then w is resumed from another thread and delivers, and a stop returns, though the read of x is held
        assertThat(CompletableFuture.supplyAsync(() -> model.resumeSubscription(W), daemonExecutor("resume-of-w"))).as("the resume of w made while the start waited for the read of x").succeedsWithin(BOUND);
        publishToY("live-1");
        assertThat(CompletableFuture.runAsync(model::stop, daemonExecutor("stop-of-the-model"))).as("the stop made while the start waited for the read of x").succeedsWithin(BOUND);
        assertThat(start.isDone()).as("whether the start returned, though the read of x is held").isFalse();
        hold.release.countDown();
        assertThat(catchThrowable(() -> start.get(BOUND.toSeconds(), TimeUnit.SECONDS))).as("what the start threw once the read of x returned").satisfiesAnyOf(
                thrown -> assertThat(thrown).isNull(),
                thrown -> assertThat(thrown).isInstanceOf(ExecutionException.class));
    }

    @Test
    void a_resume_and_a_cancel_of_the_same_subscription_a_live_action_hands_over_while_the_storage_waits_leave_it_unknown_and_not_running() throws Exception {
        // Given x paused, a live action of y that resumes x and then cancels it, and a storage that holds the read of the resume
        Hold hold = storage.holdNextReadOf(X);
        model.subscribe(X, null, StartAt.now(), event -> {
        });
        model.pauseSubscription(X);
        model.subscribe(Y, null, StartAt.now(), event -> {
            if (event.getId().equals("live-1")) {
                model.resumeSubscription(X);
                model.cancelSubscription(X);
            }
            deliveredToY.add(event.getId());
        });

        // When y receives an event, and the storage then answers
        publishToY("live-1");
        assertThat(hold.entered.await(20, TimeUnit.SECONDS)).as("whether the handed resume of x reached the storage read").isTrue();
        await().atMost(BOUND).untilAsserted(() -> assertThat(deliveredToY).as("the events y delivered while the handed resume of x waited for the storage").contains("live-1"));
        hold.release.countDown();

        // Then the cancel ran after the resume, so x is unknown to the model and does not run
        await().atMost(BOUND).untilAsserted(() -> assertThat(catchThrowable(() -> model.resumeSubscription(X))).as("what a resume of x threw once the handed calls ran").isInstanceOf(UnknownSubscriptionException.class));
        assertThat(model.isRunning(X)).as("whether x runs after the handed resume and the handed cancel ran").isFalse();
        assertThat(model.isPaused(X)).as("whether x is paused after the handed resume and the handed cancel ran").isFalse();
        assertThat(live.isRunning(X)).as("whether x runs in the live delegate after the handed resume and the handed cancel ran").isFalse();
    }

    private void givenXPausedAndYRunning() {
        model.subscribe(Y, null, StartAt.now(), event -> deliveredToY.add(event.getId()));
        model.subscribe(X, null, StartAt.now(), event -> {
        });
        model.pauseSubscription(X);
        assertThat(model.isPaused(X)).as("whether x is paused before the test starts").isTrue();
    }

    private void publishToY(String eventId) {
        CloudEvent event = new CheckpointAwareCloudEvent(cloudEvent(eventId), new StringBasedCheckpoint("checkpoint-" + eventId));
        daemonExecutor("live-delivery-" + eventId).execute(() -> live.publish(event));
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(60, TimeUnit.SECONDS)) {
                throw new IllegalStateException("latch not released");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static Executor daemonExecutor(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            thread.start();
        };
    }

    // The next read or save for one subscription waits for release, after entered counted down
    private static final class Hold {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
    }

    private static final class HoldableStorage extends InMemoryCheckpointStorage {
        private final Map<String, Hold> readHolds = new ConcurrentHashMap<>();
        private final Map<String, Hold> standingReadHolds = new ConcurrentHashMap<>();
        private final Map<String, Hold> saveHolds = new ConcurrentHashMap<>();
        private volatile Hold replayThreadReadHold;
        private volatile int replayThreadReadsToLetThrough;
        private final Map<String, AtomicInteger> reads = new ConcurrentHashMap<>();
        private final List<Hold> all = new CopyOnWriteArrayList<>();

        Hold holdNextReadOf(String subscriptionId) {
            Hold hold = new Hold();
            all.add(hold);
            readHolds.put(subscriptionId, hold);
            return hold;
        }

        Hold holdNextSaveOf(String subscriptionId) {
            Hold hold = new Hold();
            all.add(hold);
            saveHolds.put(subscriptionId, hold);
            return hold;
        }

        // The read a catch-up replay thread makes after letting readsToLetThrough of them through waits, and nothing else does
        Hold holdTheReadOfAReplayThreadAfter(int readsToLetThrough) {
            Hold hold = new Hold();
            all.add(hold);
            replayThreadReadsToLetThrough = readsToLetThrough;
            replayThreadReadHold = hold;
            return hold;
        }

        // Every read of the subscription waits, not only the next one
        Hold holdEveryReadOf(String subscriptionId) {
            Hold hold = new Hold();
            all.add(hold);
            standingReadHolds.put(subscriptionId, hold);
            return hold;
        }

        int readsOf(String subscriptionId) {
            AtomicInteger count = reads.get(subscriptionId);
            return count == null ? 0 : count.get();
        }

        void releaseAll() {
            all.forEach(hold -> hold.release.countDown());
        }

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            reads.computeIfAbsent(subscriptionId, __ -> new AtomicInteger()).incrementAndGet();
            Hold next = readHolds.remove(subscriptionId);
            Hold onReplayThread = replayThreadReadHold;
            if (next == null && onReplayThread != null && Thread.currentThread().getName().startsWith("occurrent-catchup-")) {
                synchronized (this) {
                    if (replayThreadReadHold == onReplayThread && replayThreadReadsToLetThrough-- == 0) {
                        replayThreadReadHold = null;
                        next = onReplayThread;
                    }
                }
            }
            wait(next != null ? next : standingReadHolds.get(subscriptionId));
            return super.read(subscriptionId);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            wait(saveHolds.remove(subscriptionId));
            return super.save(subscriptionId, checkpoint, condition);
        }

        private static void wait(@Nullable Hold hold) {
            if (hold == null) {
                return;
            }
            hold.entered.countDown();
            boolean interrupted = false;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            try {
                while (true) {
                    long left = deadline - System.nanoTime();
                    if (left <= 0) {
                        throw new IllegalStateException("storage call not released");
                    }
                    try {
                        if (hold.release.await(left, TimeUnit.NANOSECONDS)) {
                            return;
                        }
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
