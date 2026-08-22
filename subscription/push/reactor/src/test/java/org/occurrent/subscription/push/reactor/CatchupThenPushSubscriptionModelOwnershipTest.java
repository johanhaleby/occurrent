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

package org.occurrent.subscription.push.reactor;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * What a catch-up attempt on this model may and may not do to an id it no longer owns.
 * <p>
 * The reactor model needs a record of ownership separate from the one that says a replay is running, because
 * {@code ReactiveHandover} releases the running record at the drain so {@code isCatchingUp} stays true while the
 * events buffered during the history read are delivered. For a catch-up with nothing buffered that release happens
 * before the catch-up reports done, and for one with anything buffered it happens after, so a guard reading the
 * running record would hold on one and not the other.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class CatchupThenPushSubscriptionModelOwnershipTest {

    @Test
    void an_old_replay_that_lost_the_id_stops_replaying_rather_than_running_the_rest_of_the_history() throws Exception {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        PositionOrderedReader reader = reader(() -> Flux.just(cloudEvent("1"), cloudEvent("2"), cloudEvent("3")));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, new InMemoryCheckpointStorage());

        CountDownLatch parked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<String> oldReplayHandled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> Mono.fromRunnable(() -> {
            oldReplayHandled.add(ce.getId());
            if (ce.getId().equals("1")) {
                parked.countDown();
                awaitLatch(release);
            }
        }));
        assertThat(parked.await(5, TimeUnit.SECONDS)).isTrue();

        model.cancelSubscription("sub");

        // The replacement parks on its own first event and stays there, so it still owns the id while the
        // cancelled attempt wakes up. Asking only whether anything owns the id would answer yes here.
        CountDownLatch newParked = new CountDownLatch(1);
        CountDownLatch newRelease = new CountDownLatch(1);
        List<String> newReplayHandled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> Mono.fromRunnable(() -> {
            newReplayHandled.add(ce.getId());
            if (newReplayHandled.size() == 1) {
                newParked.countDown();
                awaitLatch(newRelease);
            }
        }));
        assertThat(newParked.await(5, TimeUnit.SECONDS)).isTrue();

        release.countDown();

        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(oldReplayHandled)
                        .as("the cancelled attempt handled the event it was parked on and then stopped, rather than "
                                + "working through the rest of a history that belongs to nobody")
                        .containsExactly("1"));

        newRelease.countDown();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(newReplayHandled).containsExactly("1", "2", "3"));
    }

    /**
     * The failure path drops the launcher outside the ownership guard, because a failed catch-up has to release it
     * whether or not the id has moved on. That makes the identity on the removal the only thing keeping it from
     * taking a launcher a replacement already installed. The reactor twin of the blocking model's own
     * {@code an_old_replays_stale_failure_leaves_the_new_subscriptions_registration_and_pending_pause_untouched}.
     */
    @Test
    void an_old_replays_late_failure_does_not_evict_the_new_subscriptions_launcher() throws Exception {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        CountDownLatch parked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        // The first attempt reads its history, parks, and then fails. The second reads a history that works.
        List<Supplier<Flux<CloudEvent>>> histories = new CopyOnWriteArrayList<>(List.of(
                () -> Flux.just(cloudEvent("1")).concatWith(Flux.defer(() -> {
                    parked.countDown();
                    awaitLatch(release);
                    return Flux.error(new IllegalStateException("replay boom"));
                })),
                () -> Flux.just(cloudEvent("1"), cloudEvent("2"))));
        PositionOrderedReader reader = reader(() -> histories.isEmpty()
                ? Flux.just(cloudEvent("1"), cloudEvent("2"))
                : histories.remove(0).get());
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);

        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> Mono.empty());
        assertThat(parked.await(5, TimeUnit.SECONDS)).isTrue();

        model.cancelSubscription("sub");

        CountDownLatch newParked = new CountDownLatch(1);
        CountDownLatch newRelease = new CountDownLatch(1);
        List<String> newReplayHandled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> Mono.fromRunnable(() -> {
            newReplayHandled.add(ce.getId());
            if (newReplayHandled.size() == 1) {
                newParked.countDown();
                awaitLatch(newRelease);
            }
        }));
        assertThat(newParked.await(5, TimeUnit.SECONDS)).isTrue();

        // Stopped while parked, so the replacement keeps its launcher for start(true) below.
        model.stop();
        newRelease.countDown();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(model.isCatchingUp("sub")).isFalse());

        // The cancelled attempt fails now, well after the replacement took the id.
        release.countDown();
        Thread.sleep(500);

        model.start(true);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(newReplayHandled)
                        .as("the cancelled attempt's failure must leave the replacement's launcher alone, or "
                                + "start(true) has nothing to relaunch")
                        .containsExactly("1", "1", "2"));
    }

    @Test
    void an_old_replays_late_completion_does_not_evict_the_new_subscriptions_launcher() throws Exception {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        PositionOrderedReader reader = reader(() -> Flux.just(cloudEvent("1"), cloudEvent("2")));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);

        CountDownLatch parked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> Mono.fromRunnable(() -> {
            parked.countDown();
            awaitLatch(release);
        }));
        assertThat(parked.await(5, TimeUnit.SECONDS)).isTrue();

        model.cancelSubscription("sub");

        CountDownLatch newParked = new CountDownLatch(1);
        CountDownLatch newRelease = new CountDownLatch(1);
        List<String> newReplayHandled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> Mono.fromRunnable(() -> {
            newReplayHandled.add(ce.getId());
            if (newReplayHandled.size() == 1) {
                newParked.countDown();
                awaitLatch(newRelease);
            }
        }));
        assertThat(newParked.await(5, TimeUnit.SECONDS)).isTrue();

        // Stopped while the replacement is parked on its first event, so its replay ends without finishing and
        // keeps its launcher. That launcher is the only thing start(true) below has to work with.
        model.stop();
        newRelease.countDown();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(model.isCatchingUp("sub")).isFalse());

        // The cancelled attempt completes now, well after the replacement took the id.
        release.countDown();
        Thread.sleep(500);

        model.start(true);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(newReplayHandled)
                        .as("start(true) replays the replacement's history from the beginning, which it cannot do "
                                + "if the cancelled attempt's completion removed the replacement's launcher")
                        .containsExactly("1", "1", "2"));
    }

    @Test
    void an_old_replays_late_completion_does_not_apply_a_pause_meant_for_the_new_subscription() throws Exception {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        PositionOrderedReader reader = reader(() -> Flux.just(cloudEvent("1")));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);

        CountDownLatch parked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> Mono.fromRunnable(() -> {
            parked.countDown();
            awaitLatch(release);
        }));
        assertThat(parked.await(5, TimeUnit.SECONDS)).isTrue();

        model.cancelSubscription("sub");

        CountDownLatch newParked = new CountDownLatch(1);
        CountDownLatch newRelease = new CountDownLatch(1);
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> Mono.fromRunnable(() -> {
            newParked.countDown();
            awaitLatch(newRelease);
        }));
        assertThat(newParked.await(5, TimeUnit.SECONDS)).isTrue();

        model.pauseSubscription("sub");
        release.countDown();
        Thread.sleep(500);

        assertThat(feed.isPaused("sub"))
                .as("the pause was asked for while the replacement was replaying, so only the replacement's own "
                        + "completion may hand it to the live feed")
                .isFalse();
        assertThat(model.isPaused("sub")).as("and it is still waiting to be applied, not lost").isTrue();

        newRelease.countDown();
    }

    /**
     * The empty-buffer case, which is the ordinary one. Nothing is written during the replay, so the handover
     * signals its drain before the catch-up reports done, and the running record is already gone by the time the
     * completion runs. A guard reading that record instead of the ownership record never fires here, which leaves
     * the launcher in place for a catch-up that finished and a pause waiting forever.
     */
    @Test
    void a_catch_up_with_nothing_buffered_still_drops_its_launcher_and_applies_its_pending_pause() throws Exception {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        PositionOrderedReader reader = reader(() -> Flux.just(cloudEvent("1")));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);

        CountDownLatch parked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> Mono.fromRunnable(() -> {
            parked.countDown();
            awaitLatch(release);
        }));
        assertThat(parked.await(5, TimeUnit.SECONDS)).isTrue();

        model.pauseSubscription("sub");
        release.countDown();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(feed.isPaused("sub"))
                        .as("the catch-up finished, so the pause it was holding is handed to the live feed")
                        .isTrue());

        // A finished catch-up keeps no launcher, so a stop and a start have nothing to replay again.
        model.stop();
        model.start(true);

        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(model.isCatchingUp("sub"))
                        .as("start(true) has no launcher to call for a catch-up that already finished")
                        .isFalse());
    }

    /**
     * A resume landing in the window between the handover signalling its drain and this model dropping the
     * launcher must not read that as "stopped". It is a catch-up that succeeded, and replaying its whole history
     * again over a handover that has already gone live applies every historical event a second time, since the
     * replay path does not consult the de-dup cache. Run with no checkpoint storage, which is what removes the
     * marker that would otherwise absorb the mistake.
     */
    @Test
    void a_resume_landing_after_the_drain_does_not_replay_a_catch_up_that_already_succeeded() throws Exception {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        PositionOrderedReader reader = reader(() -> Flux.just(cloudEvent("1"), cloudEvent("2")));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);

        List<String> handled = new CopyOnWriteArrayList<>();
        var subscription = model.subscribe("sub", null, StartAt.subscriptionModelDefault(),
                ce -> Mono.fromRunnable(() -> handled.add(ce.getId())));
        subscription.waitUntilStarted().block(Duration.ofSeconds(5));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).containsExactly("1", "2"));

        // A resume is refused once the subscription is genuinely running, which is not what this is about. What
        // matters is that none of these reaches the relaunch check and takes a finished catch-up for a stopped one.
        for (int i = 0; i < 20; i++) {
            try {
                model.resumeSubscription("sub");
            } catch (SubscriptionAlreadyRunningException expected) {
                // The live feed refusing a resume it never paused is the correct answer here.
            }
        }

        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(handled)
                        .as("the catch-up already succeeded, so no resume may replay its history a second time")
                        .containsExactly("1", "2"));
    }

    /**
     * A cancel that lands while an attempt is already writing the catch-up-complete marker leaves a marker on
     * disk that describes a history the id's next owner never read. The write cannot be called off, so the model
     * records that it happened and the replacement reads its history rather than trusting what it finds.
     * <p>
     * Run with no in-memory shortcut, the marker really is written and really is read back, so nothing here
     * depends on the model noticing the cancel in time.
     */
    @Test
    void a_marker_written_by_an_attempt_that_lost_the_id_does_not_make_the_replacement_skip_its_history() throws Exception {
        CountDownLatch saveEntered = new CountDownLatch(1);
        CountDownLatch releaseSave = new CountDownLatch(1);
        InMemoryCheckpointStorage backing = new InMemoryCheckpointStorage();
        CheckpointStorage marker = new CheckpointStorage() {
            @Override
            public Mono<Checkpoint> read(String subscriptionId) {
                return backing.read(subscriptionId);
            }

            @Override
            public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                return Mono.<Checkpoint>fromRunnable(() -> {
                    saveEntered.countDown();
                    awaitLatch(releaseSave);
                }).then(backing.save(subscriptionId, checkpoint, condition));
            }

            @Override
            public Mono<Long> writeVersion(String subscriptionId) {
                return backing.writeVersion(subscriptionId);
            }

            @Override
            public Mono<Void> delete(String subscriptionId) {
                return backing.delete(subscriptionId);
            }
        };

        PushSubscriptionModel feed = new PushSubscriptionModel();
        PositionOrderedReader reader = reader(() -> Flux.just(cloudEvent("1"), cloudEvent("2")));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, marker);

        List<String> firstHandled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> Mono.fromRunnable(() -> firstHandled.add(ce.getId())));
        assertThat(saveEntered.await(5, TimeUnit.SECONDS))
                .as("the first attempt read its history and is writing the marker")
                .isTrue();
        assertThat(firstHandled).containsExactly("1", "2");

        // Ownership moves while that write is still in flight.
        model.cancelSubscription("sub");
        releaseSave.countDown();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(backing.read("sub").hasElement().block())
                        .as("the write the cancelled attempt had already started still lands")
                        .isTrue());

        List<String> replacementHandled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(),
                ce -> Mono.fromRunnable(() -> replacementHandled.add(ce.getId())));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(replacementHandled)
                        .as("the marker on disk was written for a history this subscription never received, so it "
                                + "reads that history rather than skipping it")
                        .containsExactly("1", "2"));
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

    private static PositionOrderedReader reader(Supplier<Flux<CloudEvent>> history) {
        return new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.defer(history::get);
            }

            @Override
            public Mono<Long> currentPosition() {
                return Mono.just(1L);
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
