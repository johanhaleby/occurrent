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
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.DcbSubscriptionFilter;
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
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

    // A second catch-up for the same id announces itself and reads its own history as history. The listener is
    // registered once, before either subscribe, and stays registered across the cancel, since the recorder behind it
    // has a standing interest in this id's catch-ups rather than in any one of them.
    @Test
    void a_second_catch_up_for_the_same_id_reads_its_history_as_history() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        AtomicInteger round = new AtomicInteger();
        PositionOrderedReader reader = reader(() -> Flux.just(cloudEvent("e" + round.incrementAndGet(), "Created")), 1);

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);
        EpisodeLog log = new EpisodeLog();
        assertThat(model.listenForCatchup("proj", log)).isTrue();

        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), __ -> Mono.empty()).waitUntilStarted().block();
        model.cancelSubscription("proj");
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), __ -> Mono.empty()).waitUntilStarted().block();

        await().untilAsserted(() -> assertThat(log.signals())
                .containsExactly("started:0", "historyRead:0", "started:1", "historyRead:1"));
    }

    // A payload buffered while the history was being read is delivered once and never again, so the boundary has to
    // fall before that drain rather than after it. Otherwise a recording projection sees it as history and records
    // nothing for it, and nothing else ever delivers it again.
    @Test
    void a_payload_buffered_during_the_history_read_is_delivered_as_part_of_the_catch_up() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        List<String> timeline = new CopyOnWriteArrayList<>();
        CloudEvent buffered = cloudEvent("buffered", "Updated");
        PositionOrderedReader reader = reader(() -> Flux.just(cloudEvent("history", "Created"))
                .doOnNext(__ -> feed.accept(buffered).subscribe()), 1);

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);
        model.listenForCatchup("proj", new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                timeline.add("started");
            }

            @Override
            public void historyRead(Object episode) {
                timeline.add("historyRead");
            }
        });
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), event -> {
            timeline.add(event.getId());
            return Mono.empty();
        }).waitUntilStarted().block();

        await().untilAsserted(() -> assertThat(timeline).containsExactly("started", "history", "historyRead", "buffered"));
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

    // A cancel and a fresh subscribe can run between an attempt taking the subscription id and announcing its
    // catch-up. If the announce sits outside the step that takes the id, the stale attempt's start arrives after its
    // replacement's, the recorder adopts a catch-up that is already over, and the replacement's own boundary is then
    // ignored, so everything the replacement records is lost.
    //
    // No sleep decides anything here. The first attempt is held inside its own announce until the second one
    // announces, so under a broken ordering the second is recorded first, deterministically. Under a correct one the
    // second cannot announce at all while the first holds the id, so the first attempt's wait runs out instead and
    // the two are recorded in the order they were announced.
    @Test
    void a_start_is_never_announced_by_an_attempt_that_has_lost_the_id() throws Exception {
        PositionOrderedReader reader = reader(() -> Flux.just(cloudEvent("1", "Created")), 1);
        PushSubscriptionModel feed = new PushSubscriptionModel();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);

        List<Object> announced = new CopyOnWriteArrayList<>();
        AtomicReference<Object> firstToEnter = new AtomicReference<>();
        AtomicBoolean firstEntry = new AtomicBoolean(true);
        CountDownLatch firstAnnounceEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstAnnounce = new CountDownLatch(1);

        model.listenForCatchup("proj", new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                if (firstEntry.compareAndSet(true, false)) {
                    firstToEnter.set(episode);
                    firstAnnounceEntered.countDown();
                    // Released by whichever attempt announces next, and otherwise given up on, since a correct
                    // ordering means no other attempt can announce while this one holds the id.
                    awaitAtMost(releaseFirstAnnounce, Duration.ofSeconds(1));
                } else {
                    releaseFirstAnnounce.countDown();
                }
                announced.add(episode);
            }

            @Override
            public void historyRead(Object episode) {
            }
        });

        Thread first = new Thread(() -> model.subscribe("proj", null, StartAt.subscriptionModelDefault(), __ -> Mono.empty()));
        first.start();
        assertThat(firstAnnounceEntered.await(5, TimeUnit.SECONDS))
                .as("the first attempt reached its announce")
                .isTrue();

        Thread replacement = new Thread(() -> {
            model.cancelSubscription("proj");
            model.subscribe("proj", null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());
        });
        replacement.start();

        replacement.join(TimeUnit.SECONDS.toMillis(10));
        first.join(TimeUnit.SECONDS.toMillis(10));

        assertThat(announced).hasSize(2);
        assertThat(announced.get(0))
                .as("the attempt that reached its announce first is the one announced first, so a start never "
                        + "arrives behind the start of the attempt that replaced it")
                .isSameAs(firstToEnter.get());
    }

    // isCatchingUp answers for the whole catch-up, marker included, so it must not turn false while the marker
    // write is still in flight. A crash in that window leaves no marker on a subscription that already said it was
    // live, and the saga timer gate and every readiness probe read the same answer.
    @Test
    void a_catch_up_with_nothing_buffered_still_reports_catching_up_until_its_marker_is_written() throws Exception {
        HeldMarkerStorage marker = new HeldMarkerStorage();
        PositionOrderedReader reader = reader(() -> Flux.just(cloudEvent("1", "Created")), 1);
        PushSubscriptionModel feed = new PushSubscriptionModel();

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, marker);
        // Nothing is pushed to the feed while the replay runs, so the live buffer is empty at the drain.
        Subscription subscription = model.subscribe("proj", null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        // Parked inside the marker write, which is after the replay and after the drain point, and before the
        // handover has finished.
        assertThat(marker.writeStarted.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(model.isCatchingUp("proj"))
                .as("the marker is not written yet, so this catch-up is not over")
                .isTrue();

        marker.release.countDown();
        subscription.waitUntilStarted().block(Duration.ofSeconds(5));

        await().untilAsserted(() -> assertThat(model.isCatchingUp("proj")).isFalse());
    }

    // Holds the marker write open so a test can look at the model while the handover is half done.
    private static final class HeldMarkerStorage implements CheckpointStorage {
        final CountDownLatch writeStarted = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return Mono.empty();
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            return Mono.fromCallable(() -> {
                writeStarted.countDown();
                awaitLatch(release);
                return checkpoint;
            }).subscribeOn(Schedulers.boundedElastic());
        }

        @Override
        public Mono<Long> writeVersion(String subscriptionId) {
            return Mono.empty();
        }

        @Override
        public Mono<Void> delete(String subscriptionId) {
            return Mono.empty();
        }
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
        CatchupThenPushSubscriptionModel restarted = new CatchupThenPushSubscriptionModel(reader, feed2, marker);
        EpisodeLog log = new EpisodeLog();
        restarted.listenForCatchup("proj", log);
        restarted.subscribe("proj", null, StartAt.subscriptionModelDefault(), recordInto(secondRun))
                .waitUntilStarted().block();
        assertThat(secondRun).isEmpty();

        // Both signals, even though no replay ran. This restart is the case that decides where the boundary goes:
        // it skips the replay entirely and never reaches replayCompleted(), so a boundary placed there would leave
        // the projection reading history for the rest of the model's life.
        await().untilAsserted(() -> assertThat(log.signals()).containsExactly("started:0", "historyRead:0"));

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

    /**
     * The reactor mirror of the blocking
     * {@code a_catch_up_failure_reports_not_deliverable_rather_than_delivered_on_the_broker_path} test. A refusal
     * decided before any dispatch was attempted (ReactiveHandover's catchUpFailure) must report
     * {@link RoutingOutcome#NOT_DELIVERABLE}, never {@link RoutingOutcome#DELIVERED}, so a caller applies its own
     * failure policy instead of acknowledging a message nothing consumed.
     */
    @Test
    void a_catch_up_failure_reports_refused_rather_than_delivered() {
        List<RoutingOutcome> observed = new CopyOnWriteArrayList<>();
        PushSubscriptionModel liveFeed = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent ce, RoutingOutcome outcome) -> observed.add(outcome));
        PositionOrderedReader failingReader = failingReader();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(failingReader, liveFeed, null);

        Subscription subscription = model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> Mono.empty());
        Throwable replayFailure = catchThrowable(() -> subscription.waitUntilStarted().block());
        assertThat(replayFailure).isInstanceOf(IllegalStateException.class).hasMessageContaining("replay boom");

        Throwable thrown = catchThrowable(() -> liveFeed.accept(cloudEvent("1", "Created")).block());

        assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("Catch-up failed");
        assertThat(observed).containsExactly(RoutingOutcome.REFUSED);
    }

    /**
     * The reactor mirror of the blocking
     * {@code a_handlers_own_illegalstateexception_reports_delivered_rather_than_not_deliverable_on_the_broker_path}
     * test. Catching every {@code IllegalStateException} the handover errors with, rather than only
     * {@code ReactiveHandover.PreDispatchRefusalException}, would wrap a handler's own thrown
     * {@code IllegalStateException} as a {@code RoutingAction.Refusal} too, misreporting a handler that genuinely
     * ran and failed as {@link RoutingOutcome#NOT_DELIVERABLE} instead of {@link RoutingOutcome#DELIVERED}.
     */
    @Test
    void a_handlers_own_illegalstateexception_reports_delivered_rather_than_not_deliverable() {
        List<RoutingOutcome> observed = new CopyOnWriteArrayList<>();
        PushSubscriptionModel liveFeed = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent ce, RoutingOutcome outcome) -> observed.add(outcome));
        RuntimeException handlerFailure = new IllegalStateException("handler boom");
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader(Flux::empty, 0), liveFeed, null);

        Subscription subscription = model.subscribe("sub", null, StartAt.subscriptionModelDefault(),
                ce -> Mono.error(handlerFailure));
        assertThat(subscription.waitUntilStarted().block(Duration.ofSeconds(5))).isNull();

        Throwable thrown = catchThrowable(() -> liveFeed.accept(cloudEvent("1", "Created")).block());

        assertThat(thrown).isSameAs(handlerFailure);
        assertThat(observed).containsExactly(RoutingOutcome.DELIVERED);
    }

    /**
     * The reactor twin of the blocking
     * {@code a_refusal_from_a_handover_the_handler_reached_into_reports_delivered_for_this_registration}. A
     * handler that reaches into a second model whose own catch-up has failed lets that model's refusal out through
     * this one. This handler ran, so its own outcome is DELIVERED.
     */
    @Test
    void a_refusal_from_a_handover_the_handler_reached_into_reports_delivered_for_this_registration() {
        List<RoutingOutcome> observed = new CopyOnWriteArrayList<>();
        PushSubscriptionModel liveFeed = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent ce, RoutingOutcome outcome) -> observed.add(outcome));

        PushSubscriptionModel otherFeed = new PushSubscriptionModel();
        CatchupThenPushSubscriptionModel otherModel = new CatchupThenPushSubscriptionModel(failingReader(), otherFeed, null);
        Subscription otherSubscription = otherModel.subscribe("other", null, StartAt.subscriptionModelDefault(), ce -> Mono.empty());
        assertThat(catchThrowable(() -> otherSubscription.waitUntilStarted().block(Duration.ofSeconds(5))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("replay boom");

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader(Flux::empty, 0), liveFeed, null);
        Subscription subscription = model.subscribe("sub", null, StartAt.subscriptionModelDefault(),
                ce -> otherFeed.accept(ce));
        assertThat(subscription.waitUntilStarted().block(Duration.ofSeconds(5))).isNull();

        Throwable thrown = catchThrowable(() -> liveFeed.accept(cloudEvent("1", "Created")).block());

        assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("Catch-up failed");
        assertThat(observed)
                .as("this registration's handler ran, so its own outcome is DELIVERED even though what it called "
                        + "into refused")
                .containsExactly(RoutingOutcome.DELIVERED);
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

    // Names each catch-up by the order it was announced in, so an assertion can say which one a signal belongs to
    // without depending on what the model uses to identify it.
    private static final class EpisodeLog implements CatchupListener {
        private final List<String> signals = new CopyOnWriteArrayList<>();
        private final List<Object> episodes = new ArrayList<>();

        @Override
        public void catchupStarted(Object episode) {
            signals.add("started:" + indexOf(episode));
        }

        @Override
        public void historyRead(Object episode) {
            signals.add("historyRead:" + indexOf(episode));
        }

        List<String> signals() {
            return signals;
        }

        private synchronized int indexOf(Object episode) {
            for (int i = 0; i < episodes.size(); i++) {
                if (episodes.get(i) == episode) {
                    return i;
                }
            }
            episodes.add(episode);
            return episodes.size() - 1;
        }
    }

    private static void awaitAtMost(CountDownLatch latch, Duration timeout) {
        try {
            latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
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
