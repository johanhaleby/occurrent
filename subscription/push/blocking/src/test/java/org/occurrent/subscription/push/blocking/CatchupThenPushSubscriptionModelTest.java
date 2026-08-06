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
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.DcbSubscriptionFilter;
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
    void a_catch_up_failure_releases_the_registration() {
        PushSubscriptionModel liveFeed = new PushSubscriptionModel();
        PositionOrderedReader failingReader = failingReader();

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(failingReader, liveFeed, null);

        var subscription = model.subscribe("sub", null, StartAt.subscriptionModelDefault(), cloudEvent -> {
        });
        Throwable replayFailure = catchThrowable(subscription::waitUntilStarted);
        assertThat(replayFailure).isInstanceOf(IllegalStateException.class).hasMessageContaining("replay boom");

        // The dead handler is released on the catch-up failure path, so a later live event is simply a no-op delivery
        // rather than resurrecting the stored failure.
        Throwable thrown = catchThrowable(() -> liveFeed.accept(cloudEvent("1", "Created")));

        assertThat(thrown).isNull();
    }

    @Test
    void the_same_subscription_id_can_be_used_again_after_a_catch_up_failure() {
        PushSubscriptionModel liveFeed = new PushSubscriptionModel();

        CatchupThenPushSubscriptionModel failingModel = new CatchupThenPushSubscriptionModel(failingReader(), liveFeed, null);
        var failed = failingModel.subscribe("sub", null, StartAt.subscriptionModelDefault(), cloudEvent -> {
        });
        // Waiting is also what orders the release: it runs on the replay thread before the task completes, so the id
        // is free by the time this returns.
        Throwable replayFailure = catchThrowable(failed::waitUntilStarted);
        assertThat(replayFailure).isInstanceOf(IllegalStateException.class).hasMessageContaining("replay boom");

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
    void a_subscription_registered_after_a_failed_one_still_receives_events() {
        PushSubscriptionModel liveFeed = new PushSubscriptionModel();

        CatchupThenPushSubscriptionModel failingModel = new CatchupThenPushSubscriptionModel(failingReader(), liveFeed, null);
        var failed = failingModel.subscribe("failed", null, StartAt.subscriptionModelDefault(), cloudEvent -> {
        });
        Throwable replayFailure = catchThrowable(failed::waitUntilStarted);
        assertThat(replayFailure).isInstanceOf(IllegalStateException.class).hasMessageContaining("replay boom");

        List<String> delivered = new ArrayList<>();
        PositionOrderedReader workingReader = reader(Stream::empty, 0);
        CatchupThenPushSubscriptionModel healthyModel = new CatchupThenPushSubscriptionModel(workingReader, liveFeed, null);
        healthyModel.subscribe("healthy", null, StartAt.subscriptionModelDefault(), ce -> delivered.add(ce.getId())).waitUntilStarted();

        Throwable thrown = catchThrowable(() -> liveFeed.accept(cloudEvent("1", "Created")));

        assertThat(thrown).isNull();
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
    void an_error_from_the_fold_surfaces_unchanged_and_releases_the_registration() {
        PushSubscriptionModel liveFeed = new PushSubscriptionModel();
        PositionOrderedReader reader = reader(() -> {
            throw new NoClassDefFoundError("lazily loaded class boom");
        }, 0);
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, liveFeed, null);

        Subscription subscription = model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> {
        });
        Throwable thrown = catchThrowable(subscription::waitUntilStarted);

        assertThat(thrown).isInstanceOf(NoClassDefFoundError.class).hasMessageContaining("lazily loaded class boom");
        assertThat(catchThrowable(() -> liveFeed.accept(cloudEvent("1", "Created")))).isNull();
    }

    @Test
    void a_caller_that_never_waits_still_gets_the_registration_released_on_failure() throws Exception {
        PushSubscriptionModel liveFeed = new PushSubscriptionModel();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(failingReader(), liveFeed, null);

        // Deliberately no waitUntilStarted, which is what startupMode = BACKGROUND does.
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> {
        });

        // The release runs on the replay thread, so it lands without anyone joining it.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (liveFeed.subscriptionIds().contains("sub") && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(liveFeed.subscriptionIds()).doesNotContain("sub");
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
