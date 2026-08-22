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

package org.occurrent.subscription.push.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.RegisteringSubscribable;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A lifecycle call and a subscribe on the same id are one step against each other.
 * <p>
 * Registering on the live feed, keeping the handover, keeping the launcher and starting the replay used to happen
 * one after another with nothing holding them together, so a cancel arriving part way through left some of them
 * installed for a subscription that no longer exists.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class CatchupThenPushSubscriptionModelLifecycleAtomicityTest {

    @Test
    void a_cancel_arriving_while_subscribe_is_registering_leaves_nothing_behind_for_the_cancelled_id() throws Exception {
        AtomicReference<CatchupThenPushSubscriptionModel> modelRef = new AtomicReference<>();
        CountDownLatch cancelStarted = new CountDownLatch(1);
        CountDownLatch cancelFinished = new CountDownLatch(1);

        // The cancel is fired from inside the live feed's own registration call, which is the first thing
        // subscribe(..) does, so it arrives while the rest of the tail is still to run.
        PushSubscriptionModel feed = new PushSubscriptionModel() {
            @Override
            Subscription subscribeCatchupThenPush(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt,
                                                  RegisteringSubscribable.RoutingAction action) {
                Subscription subscription = super.subscribeCatchupThenPush(subscriptionId, filter, startAt, action);
                Thread.ofVirtual().start(() -> {
                    cancelStarted.countDown();
                    modelRef.get().cancelSubscription(subscriptionId);
                    cancelFinished.countDown();
                });
                awaitLatch(cancelStarted);
                return subscription;
            }
        };

        List<String> handled = new CopyOnWriteArrayList<>();
        PositionOrderedReader reader = reader(() -> Stream.of(cloudEvent("1"), cloudEvent("2"), cloudEvent("3")));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);
        modelRef.set(model);

        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> handled.add(ce.getId()));
        assertThat(cancelFinished.await(10, TimeUnit.SECONDS)).isTrue();

        // Long enough for a replay the cancel failed to stop to work through its three events and settle.
        Thread.sleep(1500);
        assertThat(model.isCatchingUp("sub")).as("no replay is left running for a cancelled id").isFalse();
        assertThat(model.isRunning("sub")).as("nothing is left registered for a cancelled id").isFalse();
        assertThat(model.isReadyForLiveDelivery("sub"))
                .as("no handover is left mapped for a cancelled id, which is what this answers off")
                .isFalse();
        assertThat(feed.subscriptionIds()).as("the live feed registration went with it").doesNotContain("sub");

        // start(true) has nothing to relaunch, since a cancel keeps no launcher. A launcher left behind by the
        // interrupted subscribe would replay a cancelled subscription's whole history here.
        int handledBeforeStart = handled.size();
        model.start(true);
        Thread.sleep(1500);
        assertThat(handled).as("no launcher survived the cancel, so start(true) replays nothing")
                .hasSize(handledBeforeStart);
    }

    /**
     * A pause asked for while the replay is finishing is either handed to the live feed or still waiting to be.
     * It used to be possible for the completion to read an empty pending-pause record, exit, and only then have the
     * pause written into it, which left the subscription delivering while {@code isPaused} answered true.
     */
    @Test
    void a_pause_asked_for_while_the_replay_is_completing_is_never_both_recorded_and_ignored() throws Exception {
        for (int attempt = 0; attempt < 40; attempt++) {
            PushSubscriptionModel feed = new PushSubscriptionModel();
            PositionOrderedReader reader = reader(() -> Stream.of(cloudEvent("1")));
            CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);

            CountDownLatch handling = new CountDownLatch(1);
            var subscription = model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> handling.countDown());
            assertThat(handling.await(5, TimeUnit.SECONDS)).isTrue();

            Thread pauser = Thread.ofVirtual().start(() -> model.pauseSubscription("sub"));
            subscription.waitUntilStarted(Duration.ofSeconds(5));
            pauser.join();

            assertThat(model.isPaused("sub"))
                    .as("attempt %d: a pause this model reports must have reached the live feed, or still be "
                            + "waiting for a replay that is still running", attempt)
                    .isEqualTo(feed.isPaused("sub") || model.isCatchingUp("sub"));
            model.shutdown();
        }
    }

    /**
     * A stop landing between the live feed being asked whether it is running and being told to pause must not turn
     * a catch-up that finished into one that failed. The live feed refuses a pause once it is stopped, and that
     * refusal used to escape from inside the completion step and reach whoever waited on the catch-up.
     */
    @Test
    void a_stop_landing_between_the_pause_check_and_the_pause_still_completes_the_catch_up() throws Exception {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        PositionOrderedReader reader = reader(() -> Stream.of(cloudEvent("1")));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);

        CountDownLatch replaying = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        AtomicReference<Thread> stopper = new AtomicReference<>();
        // Fired from the one gap a stop could get into. It takes the same monitor this step holds, so it waits
        // there rather than landing in the gap.
        model.runBetweenPauseCheckAndPause(() -> {
            stopper.set(Thread.ofVirtual().start(model::stop));
            sleepQuietly(200);
        });

        var subscription = model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> {
            replaying.countDown();
            awaitLatch(releaseReplay);
        });
        assertThat(replaying.await(5, TimeUnit.SECONDS)).isTrue();

        // Asked for while the replay is still running, so it is held for the completion to hand over.
        model.pauseSubscription("sub");
        assertThat(model.isPaused("sub")).isTrue();

        releaseReplay.countDown();

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (stopper.get() == null && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(stopper.get()).as("the completion reached the gap the stop was fired from").isNotNull();
        stopper.get().join();

        assertThat(subscription.waitUntilStarted(Duration.ofSeconds(5)))
                .as("the catch-up read its history and finished, so its handle answers true rather than throwing")
                .isTrue();
        assertThat(model.isPaused("sub"))
                .as("the pause either reached the live feed or the stop paused everything, never neither")
                .isTrue();
    }

    /**
     * start(false) says the operator wants to pick each subscription back up themselves. A replay that a stop
     * interrupted must not bring itself back just because the model is running again, and must still be there for
     * resumeSubscription to launch.
     * <p>
     * The start has to land after the replay has already decided to stop, or the replay simply carries on and
     * never reaches the decision this is about. Parking in the reader's own close, which runs once the replay has
     * broken out of its loop, is what puts it there.
     */
    @Test
    void a_replay_a_stop_interrupted_does_not_relaunch_itself_after_start_false() throws Exception {
        CountDownLatch replaying = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        CountDownLatch closing = new CountDownLatch(1);
        CountDownLatch releaseClose = new CountDownLatch(1);
        PushSubscriptionModel feed = new PushSubscriptionModel();
        PositionOrderedReader reader = reader(() -> Stream.of(cloudEvent("1"), cloudEvent("2")).onClose(() -> {
            closing.countDown();
            awaitLatch(releaseClose);
        }));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);

        List<String> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> {
            handled.add(ce.getId());
            if (handled.size() == 1) {
                replaying.countDown();
                awaitLatch(releaseReplay);
            }
        });
        assertThat(replaying.await(5, TimeUnit.SECONDS)).isTrue();

        model.stop();
        releaseReplay.countDown();
        assertThat(closing.await(5, TimeUnit.SECONDS))
                .as("the replay has decided to stop and is unwinding, before its own re-check runs")
                .isTrue();

        // Started without asking for subscriptions back, while the replay is parked in that unwind.
        model.start(false);
        releaseClose.countDown();

        Thread.sleep(1500);
        assertThat(model.isCatchingUp("sub"))
                .as("start(false) leaves an interrupted replay for resumeSubscription to pick up")
                .isFalse();
        assertThat(handled).as("nothing was replayed by the model itself").containsExactly("1");

        var resumed = model.resumeSubscription("sub");
        assertThat(resumed.waitUntilStarted(Duration.ofSeconds(5))).isTrue();
        assertThat(handled).as("resumeSubscription launches the replay start(false) left alone")
                .containsExactly("1", "1", "2");
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for the latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the latch", e);
        }
    }

    /**
     * The marker write and the ownership check that guards it are one step, so a cancel cannot take the id from an
     * attempt whose write is already running. That is what lets every later attempt trust a marker it finds: the
     * only attempt that can have written one is an attempt that read the whole history and owned the id throughout
     * the write, in this process and after a restart alike.
     * <p>
     * Asserted as an ordering rather than as a duration, so it says which of the two finished first rather than how
     * long either took. The cancel is released only once it is genuinely waiting on the monitor, which is what
     * keeps the ordering from being decided by the cancel simply not having started yet.
     */
    @Test
    void a_cancel_cannot_take_the_id_from_an_attempt_whose_marker_write_is_already_running() throws Exception {
        CountDownLatch saveEntered = new CountDownLatch(1);
        CountDownLatch releaseSave = new CountDownLatch(1);
        List<String> order = new CopyOnWriteArrayList<>();
        InMemoryCheckpointStorage backing = new InMemoryCheckpointStorage();
        CheckpointStorage marker = new CheckpointStorage() {
            @Override
            public @Nullable Checkpoint read(String subscriptionId) {
                return backing.read(subscriptionId);
            }

            @Override
            public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                saveEntered.countDown();
                awaitLatch(releaseSave);
                Checkpoint saved = backing.save(subscriptionId, checkpoint, condition);
                order.add("marker write returned");
                return saved;
            }

            @Override
            public OptionalLong writeVersion(String subscriptionId) {
                return backing.writeVersion(subscriptionId);
            }

            @Override
            public void delete(String subscriptionId) {
                backing.delete(subscriptionId);
            }

            @Override
            public boolean exists(String subscriptionId) {
                return backing.exists(subscriptionId);
            }
        };

        PushSubscriptionModel feed = new PushSubscriptionModel();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader(() -> Stream.of(cloudEvent("1"))), feed, marker);
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> {
        });

        assertThat(saveEntered.await(10, TimeUnit.SECONDS))
                .as("the attempt read its history and its marker write is running")
                .isTrue();

        Thread canceller = new Thread(() -> {
            model.cancelSubscription("sub");
            order.add("cancel returned");
        }, "canceller");
        canceller.start();
        // Either it is waiting for the monitor the write holds, which is the invariant, or it already ran, which is
        // the falsification. Waiting for one of the two rather than sleeping is what makes the assertion below mean
        // something in both cases.
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (canceller.getState() != Thread.State.BLOCKED && order.isEmpty() && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }

        releaseSave.countDown();
        canceller.join(TimeUnit.SECONDS.toMillis(10));

        assertThat(order)
                .as("the write the owning attempt started ran to completion before the cancel could take the id")
                .containsExactly("marker write returned", "cancel returned");
    }

    private static PositionOrderedReader reader(Supplier<Stream<CloudEvent>> history) {
        return new PositionOrderedReader() {
            @Override
            public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return history.get();
            }

            @Override
            public long currentPosition() {
                return 1L;
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType("Created")
                .build();
    }
}
