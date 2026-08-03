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
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.DcbSubscriptionFilter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.CatchupThenLiveOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupThenPushSubscriptionModelTest {

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
        InMemoryReactiveCheckpointStorage marker = new InMemoryReactiveCheckpointStorage();
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
    void a_catch_up_failure_releases_the_registration() {
        PushSubscriptionModel liveFeed = new PushSubscriptionModel();
        PositionOrderedReader failingReader = failingReader();

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(failingReader, liveFeed, null);

        Subscription subscription = model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> Mono.empty());
        Throwable replayFailure = catchThrowable(() -> subscription.waitUntilStarted().block());
        assertThat(replayFailure).isInstanceOf(IllegalStateException.class).hasMessageContaining("replay boom");

        // The dead handler is released on the catch-up failure path, so a later live event is simply a no-op delivery
        // rather than resurrecting the stored failure.
        Throwable thrown = catchThrowable(() -> liveFeed.accept(cloudEvent("1", "Created")).block());

        assertThat(thrown).isNull();
    }

    @Test
    void the_same_subscription_id_can_be_used_again_after_a_catch_up_failure() {
        PushSubscriptionModel liveFeed = new PushSubscriptionModel();

        CatchupThenPushSubscriptionModel failingModel = new CatchupThenPushSubscriptionModel(failingReader(), liveFeed, null);
        Subscription failed = failingModel.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> Mono.empty());
        // The replay is subscribed on boundedElastic, so the release is no longer guaranteed to have run by the time
        // subscribe() returns: it runs on a scheduler thread, asynchronously with this one. Joining is what orders it,
        // the same way waitUntilStarted() does for the blocking model: it completes only once the release has run, so
        // the id is guaranteed free by the time this returns.
        Throwable replayFailure = catchThrowable(() -> failed.waitUntilStarted().block());
        assertThat(replayFailure).isInstanceOf(IllegalStateException.class).hasMessageContaining("replay boom");

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
    void a_subscription_registered_after_a_failed_one_still_receives_events() {
        PushSubscriptionModel liveFeed = new PushSubscriptionModel();

        CatchupThenPushSubscriptionModel failingModel = new CatchupThenPushSubscriptionModel(failingReader(), liveFeed, null);
        Subscription failed = failingModel.subscribe("failed", null, StartAt.subscriptionModelDefault(), ce -> Mono.empty());
        // Joined for the same reason as the test above: the release that frees "failed" on the live feed now runs on
        // boundedElastic, so "healthy" must not be registered until that release has actually happened, or the two
        // would race and this would only pass by luck.
        Throwable replayFailure = catchThrowable(() -> failed.waitUntilStarted().block());
        assertThat(replayFailure).isInstanceOf(IllegalStateException.class).hasMessageContaining("replay boom");

        List<String> delivered = new CopyOnWriteArrayList<>();
        PositionOrderedReader workingReader = reader(Flux::empty, 0);
        CatchupThenPushSubscriptionModel healthyModel = new CatchupThenPushSubscriptionModel(workingReader, liveFeed, null);
        healthyModel.subscribe("healthy", null, StartAt.subscriptionModelDefault(), recordInto(delivered)).waitUntilStarted().block();

        Throwable thrown = catchThrowable(() -> liveFeed.accept(cloudEvent("1", "Created")).block());

        assertThat(thrown).isNull();
        assertThat(delivered).containsExactly("1");
    }

    @Test
    void a_catch_up_that_fails_asynchronously_after_subscribe_returned_still_releases_the_registration() {
        PushSubscriptionModel liveFeed = new PushSubscriptionModel();

        CatchupThenPushSubscriptionModel failingModel = new CatchupThenPushSubscriptionModel(asynchronouslyFailingReader(), liveFeed, null);
        // Deliberately never touched, so only the eager release inside subscribe can free the id.
        failingModel.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> Mono.empty());

        List<String> delivered = new CopyOnWriteArrayList<>();
        CatchupThenPushSubscriptionModel workingModel = new CatchupThenPushSubscriptionModel(reader(Flux::empty, 0), liveFeed, null);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            Throwable resubscribe = catchThrowable(() ->
                    workingModel.subscribe("sub", null, StartAt.subscriptionModelDefault(), recordInto(delivered)));
            assertThat(resubscribe).isNull();
        });

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

    private static final class InMemoryReactiveCheckpointStorage implements CheckpointStorage {
        private final Map<String, Checkpoint> checkpoints = new ConcurrentHashMap<>();

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return Mono.justOrEmpty(checkpoints.get(subscriptionId));
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint) {
            checkpoints.put(subscriptionId, checkpoint);
            return Mono.just(checkpoint);
        }

        @Override
        public Mono<Void> delete(String subscriptionId) {
            return Mono.fromRunnable(() -> checkpoints.remove(subscriptionId));
        }
    }
}
