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
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.DcbSubscriptionFilter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupThenPushSubscriptionModelTest {

    // A completed catch-up must leave nothing behind that makes the next one look like it is past its history read.
    // It did once: the catch-up-done signal removed the id inline before the history-done hook ran, so the hook put
    // the id back and only a shutdown ever took it out again. A second catch-up for the same id then reported that it
    // was reconciling for its whole history read, and a recording projection records during a reconciliation.
    @Test
    void a_second_catch_up_for_the_same_id_reads_its_history_as_history() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        List<Boolean> historyDuringReplay = new CopyOnWriteArrayList<>();
        AtomicReference<CatchupThenPushSubscriptionModel> self = new AtomicReference<>();
        AtomicInteger round = new AtomicInteger();
        PositionOrderedReader reader = reader(() -> Flux.just(cloudEvent("e" + round.incrementAndGet(), "Created"))
                .doOnNext(__ -> historyDuringReplay.add(self.get().isReplayingHistory("proj"))), 1);

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);
        self.set(model);

        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), __ -> Mono.empty()).waitUntilStarted().block();
        model.cancelSubscription("proj");
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), __ -> Mono.empty()).waitUntilStarted().block();

        assertThat(historyDuringReplay).containsExactly(true, true);
    }

    @Test
    void catches_up_history_then_delivers_the_live_feed() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        List<String> delivered = new CopyOnWriteArrayList<>();
        PositionOrderedReader reader = reader(() -> Flux.just(cloudEvent("1", "Created"), cloudEvent("2", "Updated"), cloudEvent("3", "Updated")), 3);

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);
        // The replay now runs on boundedElastic rather than on this thread, so the catch-up must be joined before the
        // replayed events can be asserted on.
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), recordInto(delivered)).waitUntilStarted().block();

        assertThat(delivered).containsExactly("1", "2", "3");

        feed.accept(cloudEvent("4", "Updated")).block();
        assertThat(delivered).containsExactly("1", "2", "3", "4");
    }

    @Test
    void an_event_both_replayed_and_delivered_live_during_catch_up_is_delivered_once() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        List<String> delivered = new CopyOnWriteArrayList<>();
        CloudEvent e1 = cloudEvent("1", "Created");
        CloudEvent e2 = cloudEvent("2", "Updated");
        CloudEvent e3 = cloudEvent("3", "Updated");
        // While the replay streams, e2 also arrives live on the feed.
        PositionOrderedReader reader = reader(() -> Flux.just(e1, e2, e3).doOnNext(ce -> {
            if (ce == e2) {
                feed.accept(e2).subscribe();
            }
        }), 3);

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), recordInto(delivered)).waitUntilStarted().block();

        assertThat(delivered).containsExactly("1", "2", "3");
    }

    @Test
    void a_late_committing_event_not_in_the_replay_arrives_via_the_feed_and_is_not_lost() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        List<String> delivered = new CopyOnWriteArrayList<>();
        CloudEvent e1 = cloudEvent("1", "Created");
        CloudEvent e2 = cloudEvent("2", "Updated");
        CloudEvent late = cloudEvent("late", "Updated");
        boolean[] pushed = {false};
        PositionOrderedReader reader = reader(() -> Flux.just(e1, e2).doOnNext(ce -> {
            if (!pushed[0]) {
                pushed[0] = true;
                feed.accept(late).subscribe();
            }
        }), 2);

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), recordInto(delivered)).waitUntilStarted().block();

        // waitUntilStarted() only joins the replay-then-marker phase, not the buffered-live drain that follows it
        // (ReactiveHandover completes the catch-up signal before draining the live buffer, on purpose). "late" is
        // live-buffered rather than replayed, so it is only folded during that later drain, off this thread.
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(delivered).containsExactly("1", "2", "late"));
    }

    @Test
    void a_restart_skips_the_replay_when_the_catchup_marker_exists() {
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();
        PositionOrderedReader reader = reader(() -> Flux.just(cloudEvent("1", "Created"), cloudEvent("2", "Updated")), 2);

        PushSubscriptionModel feed1 = new PushSubscriptionModel();
        List<String> firstRun = new CopyOnWriteArrayList<>();
        new CatchupThenPushSubscriptionModel(reader, feed1, marker)
                .subscribe("proj", null, StartAt.subscriptionModelDefault(), recordInto(firstRun))
                .waitUntilStarted().block();
        assertThat(firstRun).containsExactly("1", "2");

        // Restart: fresh feed and model, same reader and marker. The replay is skipped.
        PushSubscriptionModel feed2 = new PushSubscriptionModel();
        List<String> secondRun = new CopyOnWriteArrayList<>();
        new CatchupThenPushSubscriptionModel(reader, feed2, marker)
                .subscribe("proj", null, StartAt.subscriptionModelDefault(), recordInto(secondRun))
                .waitUntilStarted().block();
        assertThat(secondRun).isEmpty();

        feed2.accept(cloudEvent("3", "Updated")).block();
        assertThat(secondRun).containsExactly("3");
    }

    @Test
    void starting_the_model_again_replays_a_catch_up_that_was_stopped() {
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();
        PushSubscriptionModel feed = new PushSubscriptionModel();
        CountDownLatch firstFolded = new CountDownLatch(1);
        CountDownLatch releaseFold = new CountDownLatch(1);
        PositionOrderedReader reader = reader(() -> Flux.just(cloudEvent("1", "Created"), cloudEvent("2", "Updated")), 2);

        List<String> folded = new CopyOnWriteArrayList<>();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, marker);
        // Park inside the first fold so stop() lands mid-replay rather than after it.
        Subscription subscription = model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce ->
                Mono.fromRunnable(() -> {
                    folded.add(ce.getId());
                    firstFolded.countDown();
                    awaitLatch(releaseFold);
                }));

        awaitLatch(firstFolded);
        model.stop();
        releaseFold.countDown();
        // Completes rather than errors, which is the "a stop is not a failure" half of ADR 104. Blocking without
        // asserting would let a stop start reporting itself as a failed catch-up without this test noticing.
        StepVerifier.create(subscription.waitUntilStarted()).verifyComplete();

        // This is what used to be impossible: the registration was cancelled on the stopped path, so start() brought
        // back only the live feed and the subscription never came back. A stop is not a failure (ADR 104).
        //
        // It is also the case that would break if the restart were wrong about the handover's unicast live sink. The
        // sink is subscribed only after the marker phase, and a stop errors the pipeline before that, so an
        // interrupted replay left it untouched and a second catchUp can subscribe it. If that reasoning were wrong,
        // the live delivery at the end of this test is what would fail.
        model.start(true);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(marker.read("proj").blockOptional()).isPresent());
        assertThat(model.isRunning("proj")).isTrue();
        // The whole history again, because nothing was marked and this model keeps no replay cursor.
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(folded).endsWith("1", "2"));

        feed.accept(cloudEvent("3", "Updated")).block(Duration.ofSeconds(5));
        assertThat(folded).endsWith("1", "2", "3");
    }

    @Test
    void a_failed_catch_up_is_not_replayed_by_starting_the_model_again() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(failingReader(), feed, null);

        Subscription subscription = model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> Mono.empty());
        assertThat(catchThrowable(() -> subscription.waitUntilStarted().block())).isInstanceOf(IllegalStateException.class);

        // Stopped and failed are not the same state. Restarting a replay that failed would turn a loud refusal into a
        // restart loop, so only a stop is reversible. A failure needs cancelSubscription and a fresh subscribe.
        model.stop();
        model.start(true);

        assertThat(catchThrowable(() -> feed.accept(cloudEvent("1", "Created")).block()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Catch-up failed");
    }

    @Test
    void overflowing_the_live_buffer_during_replay_fails_loud() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        CloudEvent e1 = cloudEvent("1", "Created");
        List<Throwable> ackErrors = new CopyOnWriteArrayList<>();
        boolean[] pushed = {false};
        // On the first replayed element, three live events arrive but the buffer cap is two.
        PositionOrderedReader reader = reader(() -> Flux.just(e1).doOnNext(ce -> {
            if (!pushed[0]) {
                pushed[0] = true;
                for (String id : List.of("l1", "l2", "l3")) {
                    feed.accept(cloudEvent(id, "Updated")).subscribe(v -> {
                    }, ackErrors::add);
                }
            }
        }), 1);

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null, new CatchupThenLiveOptions(10, 2));
        // The three live pushes above run inline on the replay thread while the first replayed element is folded, so
        // joining the catch-up guarantees they have already landed (or failed to land) by the time it returns.
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> Mono.empty()).waitUntilStarted().block();

        // The event that overflowed the buffer reports the failure to its caller (the listener), which can nack it.
        assertThat(ackErrors).hasSize(1);
        assertThat(ackErrors.get(0)).isInstanceOf(IllegalStateException.class).hasMessageContaining("buffer overflowed");
    }

    @Test
    void a_non_default_start_at_is_rejected() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        PositionOrderedReader reader = reader(Flux::empty, 0);

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);
        Throwable thrown = catchThrowable(() ->
                model.subscribe("proj", null, StartAt.now(), ce -> Mono.empty()));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("always replays a projection's history from the beginning");
    }

    @Test
    void a_dcb_subscription_filter_cannot_be_replayed() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        PositionOrderedReader reader = reader(Flux::empty, 0);

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);
        Throwable thrown = catchThrowable(() ->
                model.subscribe("proj", DcbSubscriptionFilter.filter(DcbCriteria.all()), StartAt.subscriptionModelDefault(), ce -> Mono.empty()));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Cannot catch-up-replay");
    }

    @Test
    void a_catch_up_failure_keeps_the_registration_refusing() {
        PushSubscriptionModel liveFeed = new PushSubscriptionModel();
        PositionOrderedReader failingReader = failingReader();

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(failingReader, liveFeed, null);

        Subscription subscription = model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> Mono.empty());
        Throwable replayFailure = catchThrowable(() -> subscription.waitUntilStarted().block());
        assertThat(replayFailure).isInstanceOf(IllegalStateException.class).hasMessageContaining("replay boom");

        // The registration is kept, so the handover that recorded the failure fails every later event's ack instead of
        // completing it. Completing is what would acknowledge the event to the broker and lose it (ADR 104).
        Throwable thrown = catchThrowable(() -> liveFeed.accept(cloudEvent("1", "Created")).block());

        assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("Catch-up failed");
        assertThat(model.isRunning("sub")).isTrue();
    }

    @Test
    void the_same_subscription_id_can_be_used_again_once_a_failed_catch_up_is_cancelled() {
        PushSubscriptionModel liveFeed = new PushSubscriptionModel();

        CatchupThenPushSubscriptionModel failingModel = new CatchupThenPushSubscriptionModel(failingReader(), liveFeed, null);
        Subscription failed = failingModel.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> Mono.empty());
        // The replay is subscribed on boundedElastic, so joining is what orders the failure against this thread.
        Throwable replayFailure = catchThrowable(() -> failed.waitUntilStarted().block());
        assertThat(replayFailure).isInstanceOf(IllegalStateException.class).hasMessageContaining("replay boom");

        // The recovery is explicit now: the id stays taken until someone releases it, which is what stops the failure
        // being papered over by the next subscribe. ADR 90 needs the registration slot to be a clearable reference
        // rather than a one-way latch, and this is what proves it still is.
        failingModel.cancelSubscription("sub");

        List<String> delivered = new CopyOnWriteArrayList<>();
        PositionOrderedReader workingReader = reader(Flux::empty, 0);
        CatchupThenPushSubscriptionModel workingModel = new CatchupThenPushSubscriptionModel(workingReader, liveFeed, null);
        Throwable secondSubscribeFailure = catchThrowable(() ->
                workingModel.subscribe("sub", null, StartAt.subscriptionModelDefault(), recordInto(delivered)));

        assertThat(secondSubscribeFailure).isNull();

        liveFeed.accept(cloudEvent("1", "Created")).block();
        assertThat(delivered).containsExactly("1");
    }

    @Test
    void a_subscription_on_its_own_feed_is_unaffected_by_another_ones_failed_catch_up() {
        // One feed per subscription, which is what ADR 90 asks for anyway. Sharing one feed between the two only ever
        // worked here because the failure released the slot, and it no longer does.
        PushSubscriptionModel failedFeed = new PushSubscriptionModel();
        PushSubscriptionModel healthyFeed = new PushSubscriptionModel();

        CatchupThenPushSubscriptionModel failingModel = new CatchupThenPushSubscriptionModel(failingReader(), failedFeed, null);
        Subscription failed = failingModel.subscribe("failed", null, StartAt.subscriptionModelDefault(), ce -> Mono.empty());
        Throwable replayFailure = catchThrowable(() -> failed.waitUntilStarted().block());
        assertThat(replayFailure).isInstanceOf(IllegalStateException.class).hasMessageContaining("replay boom");

        List<String> delivered = new CopyOnWriteArrayList<>();
        PositionOrderedReader workingReader = reader(Flux::empty, 0);
        CatchupThenPushSubscriptionModel healthyModel = new CatchupThenPushSubscriptionModel(workingReader, healthyFeed, null);
        healthyModel.subscribe("healthy", null, StartAt.subscriptionModelDefault(), recordInto(delivered)).waitUntilStarted().block();

        Throwable thrown = catchThrowable(() -> healthyFeed.accept(cloudEvent("1", "Created")).block());

        assertThat(thrown).isNull();
        assertThat(delivered).containsExactly("1");
        // The failed one still refuses on its own feed, so isolation runs both ways.
        assertThat(catchThrowable(() -> failedFeed.accept(cloudEvent("2", "Created")).block()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void a_catch_up_that_fails_asynchronously_after_subscribe_returned_still_leaves_the_registration_refusing() {
        PushSubscriptionModel liveFeed = new PushSubscriptionModel();

        CatchupThenPushSubscriptionModel failingModel = new CatchupThenPushSubscriptionModel(asynchronouslyFailingReader(), liveFeed, null);
        // Deliberately never joined, which is what startupMode = BACKGROUND does: nobody is waiting to be told, so the
        // source backing up on refused events is the only signal there is.
        failingModel.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> Mono.empty());

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(catchThrowable(() -> liveFeed.accept(cloudEvent("1", "Created")).block()))
                        .isInstanceOf(IllegalStateException.class).hasMessageContaining("Catch-up failed"));

        // And the id stays taken until it is released, asynchronous failure or not.
        List<String> delivered = new CopyOnWriteArrayList<>();
        CatchupThenPushSubscriptionModel workingModel = new CatchupThenPushSubscriptionModel(reader(Flux::empty, 0), liveFeed, null);
        assertThat(catchThrowable(() ->
                workingModel.subscribe("sub", null, StartAt.subscriptionModelDefault(), recordInto(delivered))))
                .isInstanceOf(IllegalArgumentException.class);

        failingModel.cancelSubscription("sub");
        workingModel.subscribe("sub", null, StartAt.subscriptionModelDefault(), recordInto(delivered)).waitUntilStarted().block();
        liveFeed.accept(cloudEvent("1", "Created")).block();
        assertThat(delivered).containsExactly("1");
    }

    @Test
    void a_reader_that_does_not_write_positions_fails_fast_at_construction() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        PositionOrderedReader reader = positionlessReader();

        Throwable thrown = catchThrowable(() -> new CatchupThenPushSubscriptionModel(reader, feed, null));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("writesPosition");
    }

    @Test
    void knows_a_subscription_that_is_still_replaying() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        AtomicReference<CatchupThenPushSubscriptionModel> subject = new AtomicReference<>();
        List<Set<String>> seenDuringReplay = new CopyOnWriteArrayList<>();
        // Read from inside the replay, which is the only moment the answer could come from the replay bookkeeping rather
        // than from the live feed.
        PositionOrderedReader reader = reader(() -> Flux.just(cloudEvent("1", "Created"))
                .doOnNext(__ -> seenDuringReplay.add(subject.get().subscriptionIds())), 1);

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);
        subject.set(model);
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), __ -> Mono.empty()).waitUntilStarted().block();

        assertThat(seenDuringReplay).containsExactly(Set.of("proj"));
        assertThat(model.subscriptionIds()).containsExactly("proj");
    }

    @Test
    void forgets_a_cancelled_subscription() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        PositionOrderedReader reader = reader(() -> Flux.just(cloudEvent("1", "Created")), 1);
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), __ -> Mono.empty()).waitUntilStarted().block();

        model.cancelSubscription("proj");

        assertThat(model.subscriptionIds()).isEmpty();
    }

    /**
     * The companion to the above, and the reason {@code isCatchingUp} exists at all: a saga gates its timers on being
     * live, {@code isRunning(id)} is true for the whole replay, so without a separate signal a timeout could fire
     * against state that is only half folded up.
     */
    @Test
    void a_subscription_reports_catching_up_while_its_replay_is_still_in_flight() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        CountDownLatch replayReached = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        PositionOrderedReader reader = reader(() -> Flux.just(cloudEvent("1", "Created")), 1);

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);
        Subscription subscription = model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce ->
                Mono.fromRunnable(() -> {
                    replayReached.countDown();
                    awaitLatch(releaseReplay);
                }));

        awaitLatch(replayReached);

        assertThat(model.isCatchingUp("proj")).isTrue();
        // Running throughout, which is exactly why it cannot answer the handover question on its own.
        assertThat(model.isRunning("proj")).isTrue();

        releaseReplay.countDown();
        subscription.waitUntilStarted().block();

        assertThat(model.isCatchingUp("proj")).isFalse();
    }

    @Test
    void an_id_the_model_has_never_seen_is_not_catching_up() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        PositionOrderedReader reader = reader(Flux::empty, 0);

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);

        assertThat(model.isCatchingUp("never-subscribed")).isFalse();
    }

    @Test
    void a_failed_catch_up_is_not_catching_up_but_keeps_the_registration_running() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(failingReader(), feed, null);

        Subscription subscription = model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> Mono.empty());
        Throwable replayFailure = catchThrowable(() -> subscription.waitUntilStarted().block());
        assertThat(replayFailure).isInstanceOf(IllegalStateException.class).hasMessageContaining("replay boom");

        // The replay entry is forgotten on failure, so it no longer answers "catching up", but the registration on the
        // live feed is kept refusing rather than released (ADR 104), which is why isRunning stays true.
        assertThat(model.isCatchingUp("sub")).isFalse();
        assertThat(model.isRunning("sub")).isTrue();
    }

    @Test
    void is_catching_up_rejects_a_null_subscription_id() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        PositionOrderedReader reader = reader(Flux::empty, 0);
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);

        Throwable thrown = catchThrowable(() -> model.isCatchingUp(null));

        assertThat(thrown).isInstanceOf(NullPointerException.class);
    }

    // --- helpers ---

    private static Function<CloudEvent, Mono<Void>> recordInto(List<String> delivered) {
        return ce -> Mono.fromRunnable(() -> delivered.add(ce.getId()));
    }

    private static PositionOrderedReader reader(Supplier<Flux<CloudEvent>> flux, long head) {
        return new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.defer(flux::get);
            }

            @Override
            public Mono<Long> currentPosition() {
                return Mono.just(head);
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
    }

    private static PositionOrderedReader positionlessReader() {
        return new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.empty();
            }

            @Override
            public Mono<Long> currentPosition() {
                return Mono.just(0L);
            }

            @Override
            public boolean writesPosition() {
                return false;
            }
        };
    }

    private static PositionOrderedReader asynchronouslyFailingReader() {
        return new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                // Fails on another thread, after subscribe has already returned, which is the case the eager release
                // exists for. The synchronous failingReader cannot distinguish an eager release from an accidental
                // in-line one.
                return Flux.error(new IllegalStateException("replay boom"))
                        .delaySubscription(Duration.ofMillis(50))
                        .subscribeOn(Schedulers.parallel())
                        .cast(CloudEvent.class);
            }

            @Override
            public Mono<Long> currentPosition() {
                return Mono.just(0L);
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
    }

    private static PositionOrderedReader failingReader() {
        return new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.error(new IllegalStateException("replay boom"));
            }

            @Override
            public Mono<Long> currentPosition() {
                return Mono.just(0L);
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
    }

    private static CloudEvent cloudEvent(String id, String type) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType(type)
                .build();
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for the latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the latch", e);
        }
    }

}
