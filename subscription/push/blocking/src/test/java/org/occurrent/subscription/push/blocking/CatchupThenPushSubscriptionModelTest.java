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
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.DcbSubscriptionFilter;
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupThenPushSubscriptionModelTest {

    @Test
    void catches_up_from_the_store_then_delivers_the_live_feed() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        // Forward every written event to the feed, exactly as an application forwarding to a broker would.
        InMemoryEventStore store = new InMemoryEventStore(feed::accept);
        // History written before the projection existed. The feed dropped it (no subscribers yet), so it must be
        // recovered from the store.
        store.write("s1", List.of(cloudEvent("1", "Created"), cloudEvent("2", "Updated"), cloudEvent("3", "Updated")));

        List<String> delivered = new ArrayList<>();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, null);
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> delivered.add(ce.getId())).waitUntilStarted();

        assertThat(delivered).containsExactly("1", "2", "3");

        // A live write is forwarded to the feed and delivered without another store read.
        store.write("s1", List.of(cloudEvent("4", "Updated")));
        assertThat(delivered).containsExactly("1", "2", "3", "4");
    }

    @Test
    void an_event_both_replayed_and_delivered_live_during_catch_up_is_delivered_once() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        CloudEvent e1 = cloudEvent("1", "Created");
        CloudEvent e2 = cloudEvent("2", "Updated");
        CloudEvent e3 = cloudEvent("3", "Updated");
        // While the replay is streaming, e2 also arrives live on the feed (the overlap between replay and live).
        PositionOrderedReader reader = readerThatOnEachElementPushes(List.of(e1, e2, e3), e2, feed);

        List<String> delivered = new ArrayList<>();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> delivered.add(ce.getId())).waitUntilStarted();

        // e2 deduped by id: delivered once, via the replay.
        assertThat(delivered).containsExactly("1", "2", "3");
    }

    @Test
    void a_late_committing_event_not_in_the_replay_arrives_via_the_feed_and_is_not_lost() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        CloudEvent e1 = cloudEvent("1", "Created");
        CloudEvent e2 = cloudEvent("2", "Updated");
        CloudEvent late = cloudEvent("late", "Updated");
        // "late" is not in the replay (it committed after the head read) but is forwarded to the feed while replaying.
        PositionOrderedReader reader = readerThatOnFirstElementPushes(List.of(e1, e2), late, feed);

        List<String> delivered = new ArrayList<>();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> delivered.add(ce.getId())).waitUntilStarted();

        assertThat(delivered).containsExactly("1", "2", "late");
    }

    @Test
    void a_restart_skips_the_replay_when_the_catchup_marker_exists() {
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();
        AtomicReference<PushSubscriptionModel> sink = new AtomicReference<>();
        InMemoryEventStore store = new InMemoryEventStore(events -> {
            PushSubscriptionModel current = sink.get();
            if (current != null) {
                current.accept(events);
            }
        });
        store.write("s1", List.of(cloudEvent("1", "Created"), cloudEvent("2", "Updated")));

        // First run catches up and records the marker.
        PushSubscriptionModel feed1 = new PushSubscriptionModel();
        sink.set(feed1);
        List<String> firstRun = new ArrayList<>();
        new CatchupThenPushSubscriptionModel(store, feed1, marker)
                .subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> firstRun.add(ce.getId()))
                .waitUntilStarted();
        assertThat(firstRun).containsExactly("1", "2");

        // Restart: fresh feed and model, same store and marker. The replay is skipped.
        PushSubscriptionModel feed2 = new PushSubscriptionModel();
        sink.set(feed2);
        List<String> secondRun = new ArrayList<>();
        new CatchupThenPushSubscriptionModel(store, feed2, marker)
                .subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> secondRun.add(ce.getId()))
                .waitUntilStarted();
        assertThat(secondRun).isEmpty();

        // Only live events flow after the restart, resumed by the broker (here, the forwarding store).
        store.write("s1", List.of(cloudEvent("3", "Updated")));
        assertThat(secondRun).containsExactly("3");
    }

    @Test
    void overflowing_the_live_buffer_during_replay_fails_loud() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        CloudEvent e1 = cloudEvent("1", "Created");
        CloudEvent l1 = cloudEvent("l1", "Updated");
        CloudEvent l2 = cloudEvent("l2", "Updated");
        CloudEvent l3 = cloudEvent("l3", "Updated");
        // On the first replayed element, three live events arrive but the buffer cap is two.
        PositionOrderedReader reader = readerThatOnFirstElementPushesMany(List.of(e1), List.of(l1, l2, l3), feed);

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null, new CatchupThenLiveOptions(10, 2));
        // The overflow is thrown by the handover on the replay thread, so it surfaces from waitUntilStarted rather
        // than from subscribe.
        var subscription = model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> {
        });
        Throwable thrown = catchThrowable(subscription::waitUntilStarted);

        assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("buffer overflowed");
    }

    /**
     * A full live buffer is also decided before any dispatch was attempted, the payload never reaches the handler,
     * so it must report {@link RoutingOutcome#NOT_DELIVERABLE} for an observer configured on the write path, the
     * same as a permanently failed catch-up, not {@link RoutingOutcome#DELIVERED}.
     */
    @Test
    void overflowing_the_live_buffer_during_replay_reports_not_deliverable_rather_than_delivered_with_an_observer() {
        List<RoutingOutcome> observed = new ArrayList<>();
        PushSubscriptionModel feed = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent ce, RoutingOutcome outcome) -> observed.add(outcome));
        CloudEvent e1 = cloudEvent("1", "Created");
        CloudEvent l1 = cloudEvent("l1", "Updated");
        CloudEvent l2 = cloudEvent("l2", "Updated");
        CloudEvent l3 = cloudEvent("l3", "Updated");
        // On the first replayed element, three live events arrive but the buffer cap is two.
        PositionOrderedReader reader = readerThatOnFirstElementPushesMany(List.of(e1), List.of(l1, l2, l3), feed);

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null, new CatchupThenLiveOptions(10, 2));
        var subscription = model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> {
        });
        Throwable thrown = catchThrowable(subscription::waitUntilStarted);

        assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("buffer overflowed");
        // l1 and l2 buffer normally on the write path, reported DELIVERED same as always, l3 overflows the
        // two-slot buffer and must not be reported DELIVERED for a handler that never ran.
        assertThat(observed).containsExactly(RoutingOutcome.DELIVERED, RoutingOutcome.DELIVERED, RoutingOutcome.NOT_DELIVERABLE);
    }

    @Test
    void a_dcb_subscription_filter_cannot_be_replayed() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        PositionOrderedReader reader = readerThatOnFirstElementPushes(List.of(), null, feed);

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);
        Throwable thrown = catchThrowable(() ->
                model.subscribe("proj", DcbSubscriptionFilter.filter(DcbCriteria.all()), StartAt.subscriptionModelDefault(), ce -> {
                }));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Cannot catch-up-replay");
    }

    @Test
    void a_non_default_start_at_is_rejected() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        PositionOrderedReader reader = readerThatOnFirstElementPushes(List.of(), null, feed);

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null);
        Throwable thrown = catchThrowable(() ->
                model.subscribe("proj", null, StartAt.now(), ce -> {
                }));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("always replays a projection's history from the beginning");
    }

    @Test
    void a_catch_up_failure_keeps_the_registration_refusing() {
        PushSubscriptionModel liveFeed = new PushSubscriptionModel();
        PositionOrderedReader failingReader = failingReader();

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(failingReader, liveFeed, null);

        var subscription = model.subscribe("sub", null, StartAt.subscriptionModelDefault(), cloudEvent -> {
        });
        Throwable replayFailure = catchThrowable(subscription::waitUntilStarted);
        assertThat(replayFailure).isInstanceOf(IllegalStateException.class).hasMessageContaining("replay boom");

        // The registration is kept, so the handover that recorded the failure refuses every later event instead of
        // returning normally. Returning normally is what would acknowledge the event to the broker and lose it, which
        // is the whole of ADR 104: nobody chose this failure, so nobody chose to discard what arrives after it.
        Throwable thrown = catchThrowable(() -> liveFeed.accept(cloudEvent("1", "Created")));

        assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("Catch-up failed");
        // Registered and refusing reads as running, unlike the released registration this used to leave behind.
        assertThat(model.isRunning("sub")).isTrue();
        assertThat(model.isCatchingUp("sub")).isFalse();
    }

    /**
     * The broker-path counterpart to the test above, and the regression guard for a Copilot review finding: a
     * refusal decided before any dispatch was attempted must report {@link RoutingOutcome#NOT_DELIVERABLE}, never
     * {@link RoutingOutcome#DELIVERED}, so a bridge applies its configured failure policy instead of acknowledging
     * a message nothing consumed.
     */
    @Test
    void a_catch_up_failure_reports_not_deliverable_rather_than_delivered_on_the_broker_path() {
        List<RoutingOutcome> observed = new ArrayList<>();
        PushSubscriptionModel liveFeed = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent ce, RoutingOutcome outcome) -> observed.add(outcome));
        PositionOrderedReader failingReader = failingReader();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(failingReader, liveFeed, null);

        var subscription = model.subscribe("sub", null, StartAt.subscriptionModelDefault(), cloudEvent -> {
        });
        Throwable replayFailure = catchThrowable(subscription::waitUntilStarted);
        assertThat(replayFailure).isInstanceOf(IllegalStateException.class).hasMessageContaining("replay boom");

        Throwable thrown = catchThrowable(() -> liveFeed.acceptRedeliverable(cloudEvent("1", "Created")));

        assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("Catch-up failed");
        assertThat(observed).containsExactly(RoutingOutcome.NOT_DELIVERABLE);
    }

    /**
     * The write-path counterpart to the two tests above, and a second Copilot review finding on the same PR: the
     * write path (bufferIfNotLive true) wraps a catchUpFailure IllegalStateException as a Refusal exactly like the
     * broker path does, so an observer configured on the write path also sees NOT_DELIVERABLE rather than
     * DELIVERED for a refusal decided before any dispatch was attempted, and {@code route(CloudEvent)} unwraps the
     * Refusal back to the plain IllegalStateException before it reaches this call's own caller.
     */
    @Test
    void a_catch_up_failure_reports_not_deliverable_rather_than_delivered_on_the_write_path_with_an_observer() {
        List<RoutingOutcome> observed = new ArrayList<>();
        PushSubscriptionModel liveFeed = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent ce, RoutingOutcome outcome) -> observed.add(outcome));
        PositionOrderedReader failingReader = failingReader();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(failingReader, liveFeed, null);

        var subscription = model.subscribe("sub", null, StartAt.subscriptionModelDefault(), cloudEvent -> {
        });
        Throwable replayFailure = catchThrowable(subscription::waitUntilStarted);
        assertThat(replayFailure).isInstanceOf(IllegalStateException.class).hasMessageContaining("replay boom");

        Throwable thrown = catchThrowable(() -> liveFeed.accept(cloudEvent("1", "Created")));

        assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("Catch-up failed");
        assertThat(observed).containsExactly(RoutingOutcome.NOT_DELIVERABLE);
    }

    /**
     * The regression this guards: a Copilot review of this PR found that catching every {@code IllegalStateException}
     * from the handover, rather than only the one it throws for its own pre-dispatch refusal, wrapped a handler's own
     * thrown {@code IllegalStateException} as a {@code Refusal} too, misreporting a handler that genuinely ran and
     * failed as {@link RoutingOutcome#NOT_DELIVERABLE} instead of {@link RoutingOutcome#DELIVERED}.
     */
    @Test
    void a_handlers_own_illegalstateexception_reports_delivered_rather_than_not_deliverable_on_the_write_path() throws Exception {
        List<RoutingOutcome> observed = new ArrayList<>();
        PushSubscriptionModel liveFeed = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent ce, RoutingOutcome outcome) -> observed.add(outcome));
        RuntimeException handlerFailure = new IllegalStateException("handler boom");
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader(Stream::empty, 0), liveFeed, null);

        var subscription = model.subscribe("sub", null, StartAt.subscriptionModelDefault(), cloudEvent -> {
            throw handlerFailure;
        });
        assertThat(subscription.waitUntilStarted(Duration.ofSeconds(5))).isTrue();

        Throwable thrown = catchThrowable(() -> liveFeed.accept(cloudEvent("1", "Created")));

        assertThat(thrown).isSameAs(handlerFailure);
        assertThat(observed).containsExactly(RoutingOutcome.DELIVERED);
    }

    @Test
    void a_handlers_own_illegalstateexception_reports_delivered_rather_than_not_deliverable_on_the_broker_path() throws Exception {
        List<RoutingOutcome> observed = new ArrayList<>();
        PushSubscriptionModel liveFeed = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent ce, RoutingOutcome outcome) -> observed.add(outcome));
        RuntimeException handlerFailure = new IllegalStateException("handler boom");
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader(Stream::empty, 0), liveFeed, null);

        var subscription = model.subscribe("sub", null, StartAt.subscriptionModelDefault(), cloudEvent -> {
            throw handlerFailure;
        });
        assertThat(subscription.waitUntilStarted(Duration.ofSeconds(5))).isTrue();

        Throwable thrown = catchThrowable(() -> liveFeed.acceptRedeliverable(cloudEvent("1", "Created")));

        assertThat(thrown).isSameAs(handlerFailure);
        assertThat(observed).containsExactly(RoutingOutcome.DELIVERED);
    }

    @Test
    void the_same_subscription_id_can_be_used_again_once_a_failed_catch_up_is_cancelled() {
        PushSubscriptionModel liveFeed = new PushSubscriptionModel();

        CatchupThenPushSubscriptionModel failingModel = new CatchupThenPushSubscriptionModel(failingReader(), liveFeed, null);
        var failed = failingModel.subscribe("sub", null, StartAt.subscriptionModelDefault(), cloudEvent -> {
        });
        Throwable replayFailure = catchThrowable(failed::waitUntilStarted);
        assertThat(replayFailure).isInstanceOf(IllegalStateException.class).hasMessageContaining("replay boom");

        // The recovery is explicit now: the id stays taken until someone releases it, which is what stops the failure
        // being papered over by the next subscribe. ADR 90 needs the registration slot to be a clearable reference
        // rather than a one-way latch, and this is what proves it still is.
        failingModel.cancelSubscription("sub");

        List<String> delivered = new ArrayList<>();
        PositionOrderedReader workingReader = reader(Stream::empty, 0);
        CatchupThenPushSubscriptionModel workingModel = new CatchupThenPushSubscriptionModel(workingReader, liveFeed, null);
        Throwable secondSubscribeFailure = catchThrowable(() ->
                workingModel.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> delivered.add(ce.getId())).waitUntilStarted());

        assertThat(secondSubscribeFailure).isNull();

        liveFeed.accept(cloudEvent("1", "Created"));
        assertThat(delivered).containsExactly("1");
    }

    @Test
    void a_subscription_on_its_own_feed_is_unaffected_by_another_ones_failed_catch_up() {
        // One feed per subscription, which is what ADR 90 asks for anyway. Sharing one feed between the two only ever
        // worked here because the failure released the slot, and it no longer does.
        PushSubscriptionModel failedFeed = new PushSubscriptionModel();
        PushSubscriptionModel healthyFeed = new PushSubscriptionModel();

        CatchupThenPushSubscriptionModel failingModel = new CatchupThenPushSubscriptionModel(failingReader(), failedFeed, null);
        var failed = failingModel.subscribe("failed", null, StartAt.subscriptionModelDefault(), cloudEvent -> {
        });
        Throwable replayFailure = catchThrowable(failed::waitUntilStarted);
        assertThat(replayFailure).isInstanceOf(IllegalStateException.class).hasMessageContaining("replay boom");

        List<String> delivered = new ArrayList<>();
        PositionOrderedReader workingReader = reader(Stream::empty, 0);
        CatchupThenPushSubscriptionModel healthyModel = new CatchupThenPushSubscriptionModel(workingReader, healthyFeed, null);
        healthyModel.subscribe("healthy", null, StartAt.subscriptionModelDefault(), ce -> delivered.add(ce.getId())).waitUntilStarted();

        Throwable thrown = catchThrowable(() -> healthyFeed.accept(cloudEvent("1", "Created")));

        assertThat(thrown).isNull();
        assertThat(delivered).containsExactly("1");
        // The failed one still refuses on its own feed, so isolation runs both ways.
        assertThat(catchThrowable(() -> failedFeed.accept(cloudEvent("2", "Created")))).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void a_reader_that_does_not_write_positions_fails_fast_at_construction() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        PositionOrderedReader reader = positionlessReader();

        Throwable thrown = catchThrowable(() -> new CatchupThenPushSubscriptionModel(reader, feed, null));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("writesPosition");
    }

    @Test
    void a_subscription_reports_running_once_it_has_handed_over_to_the_live_feed() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryEventStore store = new InMemoryEventStore(feed::accept);
        store.write("s1", List.of(cloudEvent("1", "Created")));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, null);

        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> {
        }).waitUntilStarted();

        // Load-bearing beyond introspection: a @Saga's timer poller is gated on isRunning(id), so a model that answers
        // false here stops that saga firing timers at all, silently and for good.
        assertThat(model.isRunning("proj")).isTrue();
        assertThat(model.isRunning()).isTrue();
        assertThat(model.isPaused("proj")).isFalse();
        assertThat(model.isCatchingUp("proj")).isFalse();
    }

    /**
     * The companion to the above, and the reason {@code isCatchingUp} exists at all: a saga gates its timers on being
     * live, {@code isRunning(id)} is true for the whole replay, so without a separate signal a timeout could fire
     * against state that is only half folded up.
     */
    @Test
    void a_subscription_reports_catching_up_while_its_replay_is_still_in_flight() throws Exception {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryEventStore store = new InMemoryEventStore(feed::accept);
        store.write("s1", List.of(cloudEvent("1", "Created")));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, null);
        CountDownLatch replayReached = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);

        Subscription subscription = model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> {
            replayReached.countDown();
            try {
                releaseReplay.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        assertThat(replayReached.await(10, TimeUnit.SECONDS)).isTrue();

        assertThat(model.isCatchingUp("proj")).isTrue();
        // Running throughout, which is exactly why it cannot answer the handover question on its own.
        assertThat(model.isRunning("proj")).isTrue();

        releaseReplay.countDown();
        subscription.waitUntilStarted();

        assertThat(model.isCatchingUp("proj")).isFalse();
    }

    @Test
    void an_id_the_model_has_never_seen_is_not_catching_up() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryEventStore store = new InMemoryEventStore(feed::accept);

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, null);

        assertThat(model.isCatchingUp("never-subscribed")).isFalse();
    }

    @Test
    void stopping_the_model_stops_delivering_the_live_feed() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryEventStore store = new InMemoryEventStore(feed::accept);
        List<String> delivered = new ArrayList<>();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, null);
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> delivered.add(ce.getId())).waitUntilStarted();

        model.stop();
        feed.accept(cloudEvent("1", "Created"));

        assertThat(delivered).isEmpty();
    }

    @Test
    void a_paused_subscription_withholds_live_events_and_resuming_brings_it_back() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryEventStore store = new InMemoryEventStore(feed::accept);
        List<String> delivered = new ArrayList<>();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, null);
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> delivered.add(ce.getId())).waitUntilStarted();

        model.pauseSubscription("proj");
        assertThat(model.isPaused("proj")).isTrue();
        feed.accept(cloudEvent("1", "Created"));
        assertThat(delivered).isEmpty();

        model.resumeSubscription("proj");
        feed.accept(cloudEvent("2", "Updated"));

        // Dropped, not deferred: "1" arrived while paused and is gone, which is the documented contract (ADR 85).
        assertThat(delivered).containsExactly("2");
    }

    @Test
    void cancelling_a_subscription_releases_it_from_the_live_feed() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryEventStore store = new InMemoryEventStore(feed::accept);
        List<String> delivered = new ArrayList<>();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, null);
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> delivered.add(ce.getId())).waitUntilStarted();

        model.cancelSubscription("proj");
        feed.accept(cloudEvent("1", "Created"));

        assertThat(delivered).isEmpty();
        assertThat(model.isRunning("proj")).isFalse();
    }

    @Test
    void wait_until_started_returns_only_once_the_whole_replay_has_been_folded() throws Exception {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryEventStore store = new InMemoryEventStore(feed::accept);
        store.write("s1", List.of(cloudEvent("1", "Created"), cloudEvent("2", "Updated"), cloudEvent("3", "Updated")));

        CountDownLatch reachedLast = new CountDownLatch(1);
        CountDownLatch releaseLast = new CountDownLatch(1);
        List<String> delivered = new CopyOnWriteArrayList<>();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, null);

        // Gate the LAST fold rather than the first. The reactor twin's handover records that gating the first does not
        // reproduce the analogous bug, because a prefetch can let the pipeline advance while an early item is still
        // folding.
        Subscription subscription = model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> {
            delivered.add(ce.getId());
            if (ce.getId().equals("3")) {
                reachedLast.countDown();
                awaitLatch(releaseLast);
            }
        });

        assertThat(reachedLast.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(subscription.waitUntilStarted(Duration.ofMillis(100))).isFalse();

        releaseLast.countDown();

        assertThat(subscription.waitUntilStarted(Duration.ofSeconds(5))).isTrue();
        assertThat(delivered).containsExactly("1", "2", "3");
    }

    @Test
    void events_pushed_during_a_background_replay_are_delivered_after_the_drain() throws Exception {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryEventStore store = new InMemoryEventStore(__ -> {
        });
        store.write("s1", List.of(cloudEvent("1", "Created"), cloudEvent("2", "Updated")));

        CountDownLatch replayStarted = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        List<String> delivered = new CopyOnWriteArrayList<>();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, null);

        Subscription subscription = model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> {
            delivered.add(ce.getId());
            if (ce.getId().equals("1")) {
                replayStarted.countDown();
                awaitLatch(releaseReplay);
            }
        });

        assertThat(replayStarted.await(5, TimeUnit.SECONDS)).isTrue();
        // Live while the replay is parked. The handover buffers it rather than folding it out of order.
        feed.accept(cloudEvent("live", "Updated"));
        releaseReplay.countDown();
        assertThat(subscription.waitUntilStarted(Duration.ofSeconds(5))).isTrue();

        assertThat(delivered).containsExactly("1", "2", "live");
    }

    @Test
    void stopping_the_model_halts_a_replay_in_flight_and_leaves_the_marker_unwritten() throws Exception {
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();
        AtomicReference<PushSubscriptionModel> sink = new AtomicReference<>();
        InMemoryEventStore store = new InMemoryEventStore(events -> {
            PushSubscriptionModel current = sink.get();
            if (current != null) {
                current.accept(events);
            }
        });
        store.write("s1", List.of(cloudEvent("1", "Created"), cloudEvent("2", "Updated"), cloudEvent("3", "Updated")));

        PushSubscriptionModel feed1 = new PushSubscriptionModel();
        sink.set(feed1);
        CountDownLatch firstFolded = new CountDownLatch(1);
        CountDownLatch releaseFold = new CountDownLatch(1);
        List<String> firstRun = new CopyOnWriteArrayList<>();
        CatchupThenPushSubscriptionModel stopped = new CatchupThenPushSubscriptionModel(store, feed1, marker);
        // Park inside the first fold so stop() genuinely lands mid-replay. Without the park the three events replay
        // faster than the test can stop anything, and the assertions below would pass against a completed catch-up.
        Subscription subscription = stopped.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> {
            firstRun.add(ce.getId());
            firstFolded.countDown();
            awaitLatch(releaseFold);
        });

        assertThat(firstFolded.await(5, TimeUnit.SECONDS)).isTrue();
        stopped.stop();
        releaseFold.countDown();

        // Not started, but not a failure either, so this reports false rather than throwing.
        assertThat(subscription.waitUntilStarted(Duration.ofSeconds(5))).isFalse();
        assertThat(stopped.isRunning("proj")).isFalse();
        assertThat(firstRun).isNotEmpty();

        // The whole point: a partial replay must not look like a finished one. If the marker were written here, the
        // next start would skip the replay and the events never folded would be lost with nothing to show for it.
        assertThat(marker.exists("proj")).isFalse();

        PushSubscriptionModel feed2 = new PushSubscriptionModel();
        sink.set(feed2);
        List<String> secondRun = new CopyOnWriteArrayList<>();
        new CatchupThenPushSubscriptionModel(store, feed2, marker)
                .subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> secondRun.add(ce.getId()))
                .waitUntilStarted();

        assertThat(secondRun).containsExactly("1", "2", "3");
        assertThat(marker.exists("proj")).isTrue();
    }

    @Test
    void starting_the_model_again_replays_a_catch_up_that_was_stopped() throws Exception {
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryEventStore store = new InMemoryEventStore(feed::accept);
        store.write("s1", List.of(cloudEvent("1", "Created"), cloudEvent("2", "Updated"), cloudEvent("3", "Updated")));

        CountDownLatch firstFolded = new CountDownLatch(1);
        CountDownLatch releaseFold = new CountDownLatch(1);
        List<String> folded = new CopyOnWriteArrayList<>();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, marker);
        // Park inside the first fold so stop() lands mid-replay rather than after it.
        Subscription subscription = model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> {
            folded.add(ce.getId());
            firstFolded.countDown();
            awaitLatch(releaseFold);
        });

        assertThat(firstFolded.await(5, TimeUnit.SECONDS)).isTrue();
        model.stop();
        releaseFold.countDown();
        assertThat(subscription.waitUntilStarted(Duration.ofSeconds(5))).isFalse();

        // This is what used to be impossible. The registration was cancelled on the stopped path, so start() brought
        // back only the live feed and the subscription never came back: isRunning() said true while isRunning(id)
        // said false, and the projection was silently dead. A stop is not a failure (ADR 104), so it replays again,
        // which is the answer CatchupProjectionFeed.stopCatchUp() already recorded.
        model.start(true);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!marker.exists("proj") && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }

        assertThat(marker.exists("proj")).isTrue();
        assertThat(model.isRunning("proj")).isTrue();
        assertThat(model.isCatchingUp("proj")).isFalse();
        // The whole history again, because nothing was marked and this model keeps no replay cursor. The first,
        // partly folded run is still in there, which is the at-least-once contract the fold has to tolerate anyway.
        assertThat(folded).endsWith("1", "2", "3");

        // And the live feed works afterwards, so the handover really did hand over rather than just stop replaying.
        feed.accept(cloudEvent("4", "Updated"));
        assertThat(folded).endsWith("2", "3", "4");
    }

    /**
     * A Copilot review finding: {@code launchReplay}'s completion used to remove its {@code interruptibleReplays}
     * entry by key alone, the same mistake {@code forget(..)} already guards against for {@code replayingSubscriptions}
     * by comparing the {@link java.util.concurrent.Future} identity instead. An old attempt under {@code "sub"},
     * still blocked when {@code cancelSubscription("sub")} runs, can finish (fail, here) after a fresh
     * {@code subscribe("sub", ...)} has already put a new launcher in the map for a replay this test then stops. If
     * the old attempt's completion evicted that launcher by key, {@code start(true)} would find nothing to relaunch
     * and the subscription would stay silently dead. It has to survive instead.
     */
    @Test
    void an_old_replays_late_completion_after_a_cancel_and_resubscribe_does_not_evict_the_new_replays_launcher() throws Exception {
        InMemoryEventStore store = new InMemoryEventStore();
        store.write("s1", List.of(cloudEvent("1", "Created"), cloudEvent("2", "Updated"), cloudEvent("3", "Updated")));
        PushSubscriptionModel feed = new PushSubscriptionModel();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, null);

        CountDownLatch oldReplayParked = new CountDownLatch(1);
        CountDownLatch releaseOldReplay = new CountDownLatch(1);
        RuntimeException oldReplayFailure = new RuntimeException("old replay boom");
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> {
            oldReplayParked.countDown();
            awaitLatch(releaseOldReplay);
            throw oldReplayFailure;
        });
        assertThat(oldReplayParked.await(5, TimeUnit.SECONDS)).isTrue();

        // Cancelling does not stop the old replay's thread, only the registration and the map entries it owned at
        // this moment. The old replay is left running, blocked, entirely unaware it has been cancelled.
        model.cancelSubscription("sub");

        CountDownLatch newReplayParked = new CountDownLatch(1);
        CountDownLatch releaseNewReplay = new CountDownLatch(1);
        List<String> newReplayFolded = new CopyOnWriteArrayList<>();
        Subscription newSubscription = model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> {
            newReplayFolded.add(ce.getId());
            newReplayParked.countDown();
            awaitLatch(releaseNewReplay);
        });
        assertThat(newReplayParked.await(5, TimeUnit.SECONDS)).isTrue();

        // Stopped legitimately, mid-replay, exactly like starting_the_model_again_replays_a_catch_up_that_was_stopped
        // above, so interruptibleReplays keeps this launcher on purpose, for start(true) to relaunch later.
        model.stop();
        releaseNewReplay.countDown();
        assertThat(newSubscription.waitUntilStarted(Duration.ofSeconds(5))).isFalse();

        // The old replay finishes (fails) only now, well after the new one was legitimately stopped and kept.
        // Given a moment to run its cleanup before relaunching, so start(true) below races against the completed
        // cleanup rather than the still-running failure handler.
        releaseOldReplay.countDown();
        Thread.sleep(200);

        model.start(true);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while ((!model.isRunning("sub") || model.isCatchingUp("sub")) && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }

        assertThat(model.isRunning("sub")).as("the stopped launcher survived the old replay's late, stale-by-key completion")
                .isTrue();
        assertThat(model.isCatchingUp("sub")).isFalse();
        assertThat(newReplayFolded).endsWith("1", "2", "3");
    }

    /**
     * Adversarial verification companion to the failure-path test above: the same self-referencing compare-and-remove
     * (interruptibleReplays.remove(subscriptionId, ownLaunch.get())) also has to hold on the SUCCESS completion path,
     * not only the failure one. An old attempt under "sub", parked folding its last event when
     * cancelSubscription("sub") runs, finishes successfully (reaches live, no exception) only after a fresh
     * subscribe("sub", ...) has already put a new launcher in the map for a replay this test then stops. If the old
     * attempt's successful completion evicted that launcher by key rather than by identity, start(true) would find
     * nothing to relaunch and the subscription would stay silently dead, the same hazard the failure path has.
     */
    @Test
    void an_old_replays_late_successful_completion_after_a_cancel_and_resubscribe_does_not_evict_the_new_replays_launcher() throws Exception {
        InMemoryEventStore store = new InMemoryEventStore();
        store.write("s1", List.of(cloudEvent("1", "Created"), cloudEvent("2", "Updated"), cloudEvent("3", "Updated")));
        PushSubscriptionModel feed = new PushSubscriptionModel();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, null);

        CountDownLatch oldReplayParkedOnLastEvent = new CountDownLatch(1);
        CountDownLatch releaseOldReplay = new CountDownLatch(1);
        List<String> oldReplayFolded = new CopyOnWriteArrayList<>();
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> {
            oldReplayFolded.add(ce.getId());
            if (ce.getId().equals("3")) {
                // The last event in the store: keepReplaying() has already passed for it, and the loop's own
                // hasNext() is what ends the replay next, not another keepReplaying() check, so parking here and
                // then returning normally (no throw) drives the replay into catchUp's genuine success path
                // (drainBufferAndGoLive + markCaughtUp + return true) regardless of what happens to this
                // subscription id while parked.
                oldReplayParkedOnLastEvent.countDown();
                awaitLatch(releaseOldReplay);
            }
        });
        assertThat(oldReplayParkedOnLastEvent.await(5, TimeUnit.SECONDS)).isTrue();

        // Cancelling does not stop the old replay's thread, only the registration and the map entries it owned at
        // this moment. The old replay is left running, blocked, entirely unaware it has been cancelled.
        model.cancelSubscription("sub");

        CountDownLatch newReplayParked = new CountDownLatch(1);
        CountDownLatch releaseNewReplay = new CountDownLatch(1);
        List<String> newReplayFolded = new CopyOnWriteArrayList<>();
        Subscription newSubscription = model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> {
            newReplayFolded.add(ce.getId());
            newReplayParked.countDown();
            awaitLatch(releaseNewReplay);
        });
        assertThat(newReplayParked.await(5, TimeUnit.SECONDS)).isTrue();

        // Stopped legitimately, mid-replay, exactly like starting_the_model_again_replays_a_catch_up_that_was_stopped
        // above, so interruptibleReplays keeps this launcher on purpose, for start(true) to relaunch later.
        model.stop();
        releaseNewReplay.countDown();
        assertThat(newSubscription.waitUntilStarted(Duration.ofSeconds(5))).isFalse();

        // The old replay reaches live (succeeds) only now, well after the new one was legitimately stopped and kept.
        // Given a moment to run its own completion cleanup before relaunching, so start(true) below races against
        // the completed cleanup rather than the still-running success handler.
        releaseOldReplay.countDown();
        Thread.sleep(200);
        assertThat(oldReplayFolded).containsExactly("1", "2", "3");

        model.start(true);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while ((!model.isRunning("sub") || model.isCatchingUp("sub")) && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }

        assertThat(model.isRunning("sub")).as("the stopped launcher survived the old replay's late, stale-by-key "
                + "successful completion").isTrue();
        assertThat(model.isCatchingUp("sub")).isFalse();
        assertThat(newReplayFolded).endsWith("1", "2", "3");
    }

    /**
     * A Copilot review of this PR caught what the compare-and-remove fix above still missed: it guards the map
     * removal, but {@code BlockingHandover.catchUp}'s per-payload ownership check only ever runs before a fold,
     * never after the last one. An old replay parked on its last event when {@code cancelSubscription} plus a
     * fresh {@code subscribe} moves the id on unblocks straight into {@code hasNext() == false}, skipping that
     * check entirely, and would have reached {@code markCaughtUp()} (and the new replay's own pending pause) for
     * a history the id's actual owner never folded. Fixed by asking the same ownership question once more, right
     * after the loop, before either side effect runs.
     */
    @Test
    void an_old_replays_late_completion_does_not_write_the_new_subscriptions_marker_or_consume_its_pending_pause() throws Exception {
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryEventStore store = new InMemoryEventStore(feed::accept);
        store.write("s1", List.of(cloudEvent("1", "Created"), cloudEvent("2", "Updated"), cloudEvent("3", "Updated")));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, marker);

        CountDownLatch oldReplayParkedOnLastEvent = new CountDownLatch(1);
        CountDownLatch releaseOldReplay = new CountDownLatch(1);
        List<String> oldReplayFolded = new CopyOnWriteArrayList<>();
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> {
            oldReplayFolded.add(ce.getId());
            if (ce.getId().equals("3")) {
                oldReplayParkedOnLastEvent.countDown();
                awaitLatch(releaseOldReplay);
            }
        });
        assertThat(oldReplayParkedOnLastEvent.await(5, TimeUnit.SECONDS)).isTrue();

        // Cancelling does not stop the old replay's thread, only the registration and the map entries it owned at
        // this moment. The old replay is left running, blocked, entirely unaware it has been cancelled.
        model.cancelSubscription("sub");

        CountDownLatch newReplayParkedOnFirstEvent = new CountDownLatch(1);
        CountDownLatch releaseNewReplay = new CountDownLatch(1);
        List<String> newReplayFolded = new CopyOnWriteArrayList<>();
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> {
            newReplayFolded.add(ce.getId());
            newReplayParkedOnFirstEvent.countDown();
            awaitLatch(releaseNewReplay);
        });
        assertThat(newReplayParkedOnFirstEvent.await(5, TimeUnit.SECONDS)).isTrue();

        // Registered as replaying (still parked on its own first event), so this records a pause request for the
        // handover to apply once ITS OWN replay finishes, not the stale one's.
        model.pauseSubscription("sub");
        assertThat(model.isPaused("sub")).isTrue();

        // The old replay reaches its own, late, stale completion only now, well after the id moved on.
        releaseOldReplay.countDown();
        Thread.sleep(200);
        assertThat(oldReplayFolded).containsExactly("1", "2", "3");

        // Neither of the new subscription's own state was touched by the stale replay's completion. The marker
        // it would write says the whole history is durably applied, which is false for the projection actually
        // registered under this id right now, and the live feed itself must not have been paused yet either, or
        // the pending request the assertion above already confirmed would have been consumed for nothing.
        assertThat(marker.exists("sub")).as("the old replay's late completion must not mark the new subscription "
                        + "caught up for a history the projection registered under this id never folded")
                .isFalse();
        assertThat(feed.isPaused("sub")).as("the pending pause survives the old replay's late completion, still "
                + "waiting for the new replay's own completion to apply it").isFalse();
        assertThat(model.isPaused("sub")).as("still pending, not lost").isTrue();

        // The new replay finishes for real now, and its own completion is what applies the pause that was always
        // meant for it, and writes the marker that actually reflects what this registration folded.
        releaseNewReplay.countDown();
        long pauseDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!feed.isPaused("sub") && System.nanoTime() < pauseDeadline) {
            Thread.sleep(10);
        }
        assertThat(feed.isPaused("sub")).as("the new replay's own completion applies the pause that was always "
                + "meant for it").isTrue();
        assertThat(marker.exists("sub")).as("only the new replay's own completion marks this subscription "
                + "caught up").isTrue();
    }

    /**
     * A fresh-context verify of the fix above caught a narrower window it still left open: the post-loop
     * {@code keepReplaying()} check and {@code markCaughtUp()} are separate steps with nothing re-checked in
     * between, so an old replay whose ownership lapses after that check has already passed, but before
     * {@code markCaughtUp()} and the pending-pause consumption actually run, still reaches both. Parking the old
     * replay on a buffered live event's fold during {@code drainBufferAndGoLive()}, which only runs after the
     * post-loop check has already passed, reproduces exactly that window.
     */
    @Test
    void an_old_replays_ownership_lapsing_after_the_post_loop_check_still_does_not_write_the_new_subscriptions_marker_or_consume_its_pending_pause() throws Exception {
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryEventStore store = new InMemoryEventStore(feed::accept);
        store.write("s1", List.of(cloudEvent("1", "Created")));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, marker);

        CountDownLatch oldReplayParkedOnHistoricalEvent = new CountDownLatch(1);
        CountDownLatch releaseHistoricalEvent = new CountDownLatch(1);
        CountDownLatch oldReplayParkedOnBufferedLiveEvent = new CountDownLatch(1);
        CountDownLatch releaseBufferedLiveEvent = new CountDownLatch(1);
        List<String> oldReplayFolded = new CopyOnWriteArrayList<>();
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> {
            oldReplayFolded.add(ce.getId());
            if (ce.getId().equals("1")) {
                oldReplayParkedOnHistoricalEvent.countDown();
                awaitLatch(releaseHistoricalEvent);
            } else if (ce.getId().equals("2-live")) {
                oldReplayParkedOnBufferedLiveEvent.countDown();
                awaitLatch(releaseBufferedLiveEvent);
            }
        });
        assertThat(oldReplayParkedOnHistoricalEvent.await(5, TimeUnit.SECONDS)).isTrue();

        // Written once the replay's own history query has already run, so this arrives as a live event the
        // still-replaying handover only buffers, not as part of the replay itself.
        store.write("s1", List.of(cloudEvent("2-live", "Updated")));

        // Unblocks the historical fold. hasNext() is now false, so the post-loop keepReplaying() check runs next,
        // still true (ownership has not moved yet), and drainBufferAndGoLive() delivers the one buffered event.
        releaseHistoricalEvent.countDown();
        assertThat(oldReplayParkedOnBufferedLiveEvent.await(5, TimeUnit.SECONDS)).isTrue();

        // The post-loop check has already passed by now. Ownership moves here, while the old replay is parked
        // mid-drain, after that check and before markCaughtUp().
        model.cancelSubscription("sub");
        CountDownLatch newReplayParkedOnFirstEvent = new CountDownLatch(1);
        CountDownLatch releaseNewReplay = new CountDownLatch(1);
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> {
            newReplayParkedOnFirstEvent.countDown();
            awaitLatch(releaseNewReplay);
        });
        assertThat(newReplayParkedOnFirstEvent.await(5, TimeUnit.SECONDS)).isTrue();
        model.pauseSubscription("sub");
        assertThat(model.isPaused("sub")).isTrue();

        // The old replay's drain finishes only now, well after the id moved on.
        releaseBufferedLiveEvent.countDown();
        Thread.sleep(200);
        assertThat(oldReplayFolded).containsExactly("1", "2-live");

        assertThat(marker.exists("sub")).as("the old replay's late completion must not mark the new subscription "
                        + "caught up, even though its own post-loop keepReplaying() check already passed before "
                        + "ownership moved")
                .isFalse();
        assertThat(feed.isPaused("sub")).as("the pending pause survives the old replay's late completion, still "
                + "waiting for the new replay's own completion to apply it").isFalse();
        assertThat(model.isPaused("sub")).as("still pending, not lost").isTrue();

        releaseNewReplay.countDown();
        long pauseDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!feed.isPaused("sub") && System.nanoTime() < pauseDeadline) {
            Thread.sleep(10);
        }
        assertThat(feed.isPaused("sub")).as("the new replay's own completion applies the pause that was always "
                + "meant for it").isTrue();
        assertThat(marker.exists("sub")).as("only the new replay's own completion marks this subscription "
                + "caught up").isTrue();
    }

    @Test
    void starting_the_model_without_resuming_subscriptions_leaves_a_stopped_replay_for_resume_to_pick_up() throws Exception {
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryEventStore store = new InMemoryEventStore(feed::accept);
        store.write("s1", List.of(cloudEvent("1", "Created"), cloudEvent("2", "Updated")));

        CountDownLatch firstFolded = new CountDownLatch(1);
        CountDownLatch releaseFold = new CountDownLatch(1);
        List<String> folded = new CopyOnWriteArrayList<>();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, marker);
        Subscription subscription = model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> {
            folded.add(ce.getId());
            firstFolded.countDown();
            awaitLatch(releaseFold);
        });

        assertThat(firstFolded.await(5, TimeUnit.SECONDS)).isTrue();
        model.stop();
        releaseFold.countDown();
        assertThat(subscription.waitUntilStarted(Duration.ofSeconds(5))).isFalse();

        // "Do not resume subscriptions automatically" has to mean the replay stays down too, or the flag would be
        // ignored for exactly the subscriptions whose catch-up is the thing that was stopped.
        model.start(false);
        Thread.sleep(200);

        assertThat(model.isCatchingUp("proj")).isFalse();
        assertThat(marker.exists("proj")).isFalse();

        // The caller picks it up one at a time instead, and resuming means replaying again: there is no cursor.
        assertThat(model.resumeSubscription("proj").waitUntilStarted(Duration.ofSeconds(5))).isTrue();

        assertThat(marker.exists("proj")).isTrue();
        assertThat(folded).endsWith("1", "2");
        feed.accept(cloudEvent("3", "Updated"));
        assertThat(folded).endsWith("1", "2", "3");
    }

    @Test
    void a_failed_catch_up_is_not_replayed_by_starting_the_model_again() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(failingReader(), feed, null);

        Subscription subscription = model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> {
        });
        assertThat(catchThrowable(subscription::waitUntilStarted)).isInstanceOf(IllegalStateException.class);

        // Stopped and failed are not the same state. Restarting a replay that failed would turn a loud refusal into a
        // restart loop, so only a stop is reversible. A failure needs cancelSubscription and a fresh subscribe.
        model.stop();
        model.start(true);

        assertThat(model.isCatchingUp("sub")).isFalse();
        assertThat(catchThrowable(() -> feed.accept(cloudEvent("1", "Created"))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Catch-up failed");
    }

    @Test
    void an_error_from_the_fold_surfaces_unchanged_and_leaves_the_registration_refusing() {
        PushSubscriptionModel liveFeed = new PushSubscriptionModel();
        PositionOrderedReader reader = reader(() -> {
            throw new NoClassDefFoundError("lazily loaded class boom");
        }, 0);
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, liveFeed, null);

        Subscription subscription = model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> {
        });
        Throwable thrown = catchThrowable(subscription::waitUntilStarted);

        assertThat(thrown).isInstanceOf(NoClassDefFoundError.class).hasMessageContaining("lazily loaded class boom");
        // An Error, not a RuntimeException, and it has to refuse just the same. The handover used to record only
        // RuntimeException, which was survivable while this model released the registration on failure. It no longer
        // does, so an unrecorded Error would leave a handover quietly buffering live events and acknowledging them
        // into a replay that is never coming back.
        assertThat(catchThrowable(() -> liveFeed.accept(cloudEvent("1", "Created"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Catch-up failed")
                .hasRootCauseInstanceOf(NoClassDefFoundError.class);
    }

    @Test
    void a_caller_that_never_waits_still_gets_a_refusing_registration_on_failure() throws Exception {
        PushSubscriptionModel liveFeed = new PushSubscriptionModel();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(failingReader(), liveFeed, null);

        // Deliberately no waitUntilStarted, which is what startupMode = BACKGROUND does.
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> {
        });

        // The failure is recorded on the replay thread, so it lands without anyone joining it. The registration stays,
        // which is the point: under BACKGROUND nobody is waiting to be told, so the queue backing up is the signal.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (model.isCatchingUp("sub") && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(model.isCatchingUp("sub")).isFalse();
        assertThat(liveFeed.subscriptionIds()).contains("sub");
        assertThat(catchThrowable(() -> liveFeed.accept(cloudEvent("1", "Created"))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Catch-up failed");
    }

    // --- isReadyForLiveDelivery(String), the accessor a CloudEvent-level broker bridge can optionally pace its own
    // consumption on (RabbitMqCloudEventBridge and KafkaCloudEventBridge's readinessSource), skipping a fetch it
    // can predict would only come back DEFERRED. acceptRedeliverable(...) already refuses rather than buffers, so
    // acknowledgement safety never depends on this. It only cuts down on refuse-and-redeliver round trips. ---

    @Test
    void an_id_the_model_has_never_seen_is_not_ready_for_live_delivery() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryEventStore store = new InMemoryEventStore(feed::accept);
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, null);

        assertThat(model.isReadyForLiveDelivery("never-subscribed")).isFalse();
    }

    @Test
    void is_ready_for_live_delivery_is_false_while_the_replay_is_still_running_and_true_once_it_drains() throws Exception {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryEventStore store = new InMemoryEventStore(feed::accept);
        store.write("s1", List.of(cloudEvent("1", "Created")));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, null);
        CountDownLatch replayReached = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);

        Subscription subscription = model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> {
            replayReached.countDown();
            awaitLatch(releaseReplay);
        });
        assertThat(replayReached.await(10, TimeUnit.SECONDS)).isTrue();

        // Not yet. The replay is still applying its history, and a bridge pacing itself on this answer skips a
        // fetch here rather than pulling a message off the broker only to have acceptRedeliverable(...) refuse it.
        assertThat(model.isReadyForLiveDelivery("proj")).isFalse();

        releaseReplay.countDown();
        subscription.waitUntilStarted();

        assertThat(model.isReadyForLiveDelivery("proj")).isTrue();
    }

    /**
     * The buffering case specifically, distinct from the mid-replay case above. A live event that arrives while the
     * replay is still in flight is only queued in the handover, not yet applied to the projection, so readiness must
     * stay false for it too until the drain actually runs, the same "buffered, not durable" state
     * {@code BlockingHandover} documents.
     */
    @Test
    void is_ready_for_live_delivery_is_false_while_a_live_event_is_only_buffered_ahead_of_the_drain() throws Exception {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryEventStore store = new InMemoryEventStore(__ -> {
        });
        store.write("s1", List.of(cloudEvent("1", "Created")));

        CountDownLatch replayStarted = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        List<String> delivered = new CopyOnWriteArrayList<>();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, null);

        Subscription subscription = model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> {
            delivered.add(ce.getId());
            replayStarted.countDown();
            awaitLatch(releaseReplay);
        });
        assertThat(replayStarted.await(5, TimeUnit.SECONDS)).isTrue();

        feed.accept(cloudEvent("live", "Updated"));
        // Buffered, not yet applied. Still not ready, even though the live feed already accepted the event.
        assertThat(model.isReadyForLiveDelivery("proj")).isFalse();
        assertThat(delivered).doesNotContain("live");

        releaseReplay.countDown();
        assertThat(subscription.waitUntilStarted(Duration.ofSeconds(5))).isTrue();

        assertThat(model.isReadyForLiveDelivery("proj")).isTrue();
        assertThat(delivered).containsExactly("1", "live");
    }

    @Test
    void is_ready_for_live_delivery_stays_false_across_a_stop_mid_replay_and_becomes_true_once_a_relaunch_drains() throws Exception {
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryEventStore store = new InMemoryEventStore(feed::accept);
        store.write("s1", List.of(cloudEvent("1", "Created"), cloudEvent("2", "Updated")));

        CountDownLatch firstFolded = new CountDownLatch(1);
        CountDownLatch releaseFold = new CountDownLatch(1);
        List<String> folded = new CopyOnWriteArrayList<>();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, marker);
        Subscription subscription = model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> {
            folded.add(ce.getId());
            firstFolded.countDown();
            awaitLatch(releaseFold);
        });

        assertThat(firstFolded.await(5, TimeUnit.SECONDS)).isTrue();
        model.stop();
        releaseFold.countDown();
        assertThat(subscription.waitUntilStarted(Duration.ofSeconds(5))).isFalse();

        // Stopped, same handover kept. Still not ready, not merely "unknown".
        assertThat(model.isReadyForLiveDelivery("proj")).isFalse();

        model.start(true);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!marker.exists("proj") && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }

        assertThat(model.isReadyForLiveDelivery("proj")).isTrue();
    }

    @Test
    void is_ready_for_live_delivery_is_permanently_false_after_a_catch_up_failure() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(failingReader(), feed, null);

        Subscription subscription = model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> {
        });
        assertThat(catchThrowable(subscription::waitUntilStarted)).isInstanceOf(IllegalStateException.class);

        assertThat(model.isReadyForLiveDelivery("sub")).isFalse();

        // Restarting a failed catch-up does not clear the failure (only cancelSubscription + a fresh subscribe does),
        // so this must keep answering false rather than flip true just because start(true) ran.
        model.stop();
        model.start(true);

        assertThat(model.isReadyForLiveDelivery("sub")).isFalse();
    }

    @Test
    void is_ready_for_live_delivery_is_false_once_a_subscription_is_cancelled() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryEventStore store = new InMemoryEventStore(feed::accept);
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, null);
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> {
        }).waitUntilStarted();
        assertThat(model.isReadyForLiveDelivery("proj")).isTrue();

        model.cancelSubscription("proj");

        // The handover is gone along with the registration, the same "nothing here is tracking this id" answer as
        // an id never subscribed at all.
        assertThat(model.isReadyForLiveDelivery("proj")).isFalse();
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for the latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    // --- helpers ---

    private static PositionOrderedReader readerThatOnEachElementPushes(List<CloudEvent> history, CloudEvent pushWhenSeen, PushSubscriptionModel feed) {
        return reader(() -> history.stream().peek(ce -> {
            if (ce == pushWhenSeen) {
                feed.accept(pushWhenSeen);
            }
        }), history.size());
    }

    private static PositionOrderedReader readerThatOnFirstElementPushes(List<CloudEvent> history, CloudEvent pushOnFirst, PushSubscriptionModel feed) {
        return reader(() -> {
            boolean[] pushed = {false};
            return history.stream().peek(ce -> {
                if (!pushed[0]) {
                    pushed[0] = true;
                    if (pushOnFirst != null) {
                        feed.accept(pushOnFirst);
                    }
                }
            });
        }, history.size());
    }

    private static PositionOrderedReader readerThatOnFirstElementPushesMany(List<CloudEvent> history, List<CloudEvent> pushOnFirst, PushSubscriptionModel feed) {
        return reader(() -> {
            boolean[] pushed = {false};
            return history.stream().peek(ce -> {
                if (!pushed[0]) {
                    pushed[0] = true;
                    pushOnFirst.forEach(feed::accept);
                }
            });
        }, history.size());
    }

    private static PositionOrderedReader reader(Supplier<Stream<CloudEvent>> stream, long head) {
        return new PositionOrderedReader() {
            @Override
            public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return stream.get();
            }

            @Override
            public long currentPosition() {
                return head;
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
            public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Stream.empty();
            }

            @Override
            public long currentPosition() {
                return 0;
            }

            @Override
            public boolean writesPosition() {
                return false;
            }
        };
    }

    private static PositionOrderedReader failingReader() {
        return new PositionOrderedReader() {
            @Override
            public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                throw new IllegalStateException("replay boom");
            }

            @Override
            public long currentPosition() {
                return 0;
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

}
