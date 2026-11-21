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
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import org.occurrent.subscription.internal.SharedStorageMessages;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * The catch-up marker is the position the next catch-up resumes after (ADR 179). Every start in these tests is a fresh
 * model over the same reader and marker storage, which is what a restart is.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupThenPushSubscriptionModelResumePositionTest {

    private final InMemoryReactivePositionOrderedReader store = new InMemoryReactivePositionOrderedReader();
    private final InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();

    @Test
    void a_start_without_a_marker_replays_the_whole_history_and_stores_the_head() {
        append("1", "2");

        List<String> delivered = start(store);

        assertThat(delivered).containsExactly("1", "2");
        assertThat(storedPosition()).isEqualTo(2L);
    }

    @Test
    void a_start_with_a_stored_marker_replays_only_the_events_after_it_and_stores_the_new_head() {
        append("1", "2");
        start(store);
        append("3", "4");

        List<String> delivered = start(store);

        assertThat(delivered).as("replayed after the stored marker, nothing before it and nothing lost").containsExactly("3", "4");
        assertThat(storedPosition()).isEqualTo(4L);
    }

    @Test
    void a_start_with_a_stored_marker_and_nothing_after_it_replays_nothing_and_goes_live() {
        append("1", "2");
        start(store);

        PushSubscriptionModel feed = new PushSubscriptionModel();
        List<String> delivered = new CopyOnWriteArrayList<>();
        subscribe(store, feed, delivered);
        feed.accept(event("3")).block(Duration.ofSeconds(5));

        assertThat(delivered).containsExactly("3");
    }

    @Test
    void the_stored_marker_is_the_head_read_before_the_replay_and_not_the_head_when_it_is_marked() {
        append("1", "2");
        // An event is appended while the replay reads, so the head has moved by the time the marker is written.
        boolean[] appended = {false};
        PositionOrderedReader appendingWhileReading = decorated(store, replay -> replay.doOnNext(e -> {
            if (!appended[0]) {
                appended[0] = true;
                append("during");
            }
        }));

        start(appendingWhileReading);

        assertThat(store.currentPosition().block()).as("the head moved during the replay").isEqualTo(3L);
        assertThat(storedPosition()).as("the head read before the replay").isEqualTo(2L);
        assertThat(start(store)).as("the event appended during the replay is replayed by the next start").containsExactly("during");
    }

    @Test
    void a_catch_up_that_fails_does_not_store_its_head() {
        append("1", "2");
        start(store);
        append("3", "4");
        PositionOrderedReader failingAfterTheReplay = decorated(store, replay -> replay.concatWith(Flux.error(new IllegalStateException("replay boom"))));

        Throwable failure = catchThrowable(() -> new CatchupThenPushSubscriptionModel(failingAfterTheReplay, new PushSubscriptionModel(), marker)
                .subscribe("proj", null, StartAt.subscriptionModelDefault(), e -> Mono.empty())
                .waitUntilStarted().block(Duration.ofSeconds(5)));

        assertThat(failure).hasMessageContaining("replay boom");
        assertThat(marker.read("proj").map(GlobalCheckpoint::positionOf).blockOptional(Duration.ofSeconds(5)))
                .as("marker after the failed catch-up, which deleted it").isEmpty();
        assertThat(start(store)).as("replayed again from the beginning, the events of the failed catch-up among them").containsExactly("1", "2", "3", "4");
    }

    @Test
    void an_event_both_replayed_and_delivered_live_during_a_replay_after_a_marker_is_delivered_once() {
        append("1", "2");
        start(store);
        append("3", "4");
        PushSubscriptionModel feed = new PushSubscriptionModel();
        PositionOrderedReader overlapping = decorated(store, replay -> replay.doOnNext(e -> {
            if (e.getId().equals("4")) {
                feed.accept(e).subscribe();
            }
        }));

        List<String> delivered = new CopyOnWriteArrayList<>();
        subscribe(overlapping, feed, delivered);

        assertThat(delivered).containsExactly("3", "4");
    }

    @Test
    void an_event_committed_after_the_head_the_replay_read_is_delivered_by_the_live_path_and_the_marker_stores_that_head() {
        append("1", "2");
        PushSubscriptionModel feed = new PushSubscriptionModel();
        boolean[] committed = {false};
        // Committed and offered to the live feed while the replay reads, which is after the head was read
        PositionOrderedReader committingWhileReading = decorated(store, replay -> replay.doOnNext(e -> {
            if (!committed[0]) {
                committed[0] = true;
                append("after-the-head");
                feed.accept(event("after-the-head")).subscribe();
            }
        }));
        List<String> delivered = new CopyOnWriteArrayList<>();

        subscribe(committingWhileReading, feed, delivered);

        assertThat(store.currentPosition().block()).as("the head moved during the replay").isEqualTo(3L);
        assertThat(delivered).as("the replay, then the event the live path delivered").containsExactly("1", "2", "after-the-head");
        assertThat(storedPosition()).as("the head read before the replay").isEqualTo(2L);
    }

    @Test
    void cancelling_deletes_the_marker_so_the_next_subscribe_replays_from_the_beginning() {
        append("1", "2");
        PushSubscriptionModel feed = new PushSubscriptionModel();
        CatchupThenPushSubscriptionModel model = subscribe(store, feed, new CopyOnWriteArrayList<>());
        assertThat(storedPosition()).isEqualTo(2L);

        model.cancelSubscription("proj").block(Duration.ofSeconds(5));
        assertThat(marker.read("proj").hasElement().block()).as("marker after the cancel").isFalse();
        append("3");

        assertThat(start(store)).containsExactly("1", "2", "3");
    }

    @Test
    void a_marker_stored_by_0_34_0_is_read_as_a_resume_position() {
        append("1", "2", "3");
        marker.save("proj", GlobalCheckpoint.of(2)).block();

        assertThat(start(store)).containsExactly("3");
    }

    @Test
    void an_event_appended_while_the_marker_is_read_is_delivered_and_covered_by_the_stored_head() {
        append("1", "2");
        InMemoryCheckpointStorage appendingOnRead = new InMemoryCheckpointStorage() {
            private boolean appended;

            @Override
            public Mono<Checkpoint> read(String subscriptionId) {
                return Mono.defer(() -> {
                    if (!appended) {
                        appended = true;
                        append("3");
                    }
                    return super.read(subscriptionId);
                });
            }
        };
        appendingOnRead.save("proj", GlobalCheckpoint.of(2)).block(Duration.ofSeconds(5));
        List<String> delivered = new CopyOnWriteArrayList<>();

        new CatchupThenPushSubscriptionModel(store, new PushSubscriptionModel(), appendingOnRead)
                .subscribe("proj", null, StartAt.subscriptionModelDefault(), e -> Mono.fromRunnable(() -> delivered.add(e.getId())))
                .waitUntilStarted().block(Duration.ofSeconds(5));

        assertThat(delivered).as("the event appended while the marker was read").containsExactly("3");
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(GlobalCheckpoint.positionOf(appendingOnRead.read("proj").block(Duration.ofSeconds(5))))
                .as("stored marker, at least the position of the event appended while the marker was read").isGreaterThanOrEqualTo(3L));
    }

    @Test
    void a_stored_marker_that_is_not_a_catch_up_marker_fails_the_catch_up_naming_the_id_and_suggesting_a_new_one() {
        append("1");
        marker.save("proj", new StringBasedCheckpoint("resume-token-xyz")).block(Duration.ofSeconds(5));

        Throwable failure = catchThrowable(() -> new CatchupThenPushSubscriptionModel(store, new PushSubscriptionModel(), marker)
                .subscribe("proj", null, StartAt.subscriptionModelDefault(), e -> Mono.empty())
                .waitUntilStarted().block(Duration.ofSeconds(5)));

        assertThat(failure).hasMessageContaining("\"proj\"").hasMessageContaining("subscribe under a new id").hasMessageContaining(SharedStorageMessages.MARKER_WHEN_SEVERAL_INSTANCES_RUN_IT);
    }

    @Test
    void a_stored_checkpoint_that_is_not_a_catch_up_marker_is_still_stored_unchanged_after_the_catch_up_refused_it() {
        append("1");
        StringBasedCheckpoint foreign = new StringBasedCheckpoint("resume-token-xyz");
        marker.save("proj", foreign).block(Duration.ofSeconds(5));

        // The failure is reported after the forget attempts have run, so nothing is left in flight to wait for.
        assertThat(catchThrowable(() -> new CatchupThenPushSubscriptionModel(store, new PushSubscriptionModel(), marker)
                .subscribe("proj", null, StartAt.subscriptionModelDefault(), e -> Mono.empty())
                .waitUntilStarted().block(Duration.ofSeconds(5)))).as("the refused start").isNotNull();

        assertThat(marker.read("proj").block(Duration.ofSeconds(5))).as("checkpoint of another subscription under the same id").isEqualTo(foreign);
    }

    @Test
    void a_handler_that_feeds_back_an_event_while_the_catch_up_replays_has_its_marker_stored_when_the_start_completes() {
        append("1", "2");
        PushSubscriptionModel feed = new PushSubscriptionModel();
        List<String> handled = new CopyOnWriteArrayList<>();
        new CatchupThenPushSubscriptionModel(store, feed, marker)
                .subscribe("proj", null, StartAt.subscriptionModelDefault(), e -> Mono.defer(() -> {
                    handled.add(e.getId());
                    if (e.getId().startsWith("fed-back-")) {
                        return Mono.empty();
                    }
                    String fedBack = "fed-back-" + e.getId();
                    append(fedBack);
                    return feed.accept(event(fedBack));
                }))
                .waitUntilStarted().block(Duration.ofSeconds(5));

        assertThat(handled).as("handled by the model that fed them back").contains("1", "2", "fed-back-1", "fed-back-2");
        // Read right after the start completes, without the wait for the model to finish that the subscribe helper does.
        assertThat(marker.read("proj").hasElement().block(Duration.ofSeconds(5))).as("marker stored when the start completed").isTrue();
        assertThat(storedPosition()).as("the head read before the replay").isEqualTo(2L);
        assertThat(start(store)).as("a new model resumes after the marker").containsExactly("fed-back-1", "fed-back-2");
    }

    private List<String> start(PositionOrderedReader reader) {
        List<String> delivered = new CopyOnWriteArrayList<>();
        subscribe(reader, new PushSubscriptionModel(), delivered);
        return delivered;
    }

    private CatchupThenPushSubscriptionModel subscribe(PositionOrderedReader reader, PushSubscriptionModel feed, List<String> delivered) {
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, marker);
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), e -> Mono.fromRunnable(() -> delivered.add(e.getId())))
                .waitUntilStarted().block(Duration.ofSeconds(5));
        // The marker is written after the catch-up is reported done, so wait for the model to finish before reading it.
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(model.isCatchingUp("proj")).as("catch-up finished").isFalse());
        return model;
    }

    private void append(String... ids) {
        store.append(java.util.Arrays.stream(ids).map(CatchupThenPushSubscriptionModelResumePositionTest::event).toList());
    }

    private long storedPosition() {
        return GlobalCheckpoint.positionOf(marker.read("proj").block(Duration.ofSeconds(5)));
    }

    private static PositionOrderedReader decorated(PositionOrderedReader delegate, UnaryOperator<Flux<CloudEvent>> onRead) {
        return new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.defer(() -> onRead.apply(delegate.readInPositionOrder(filter, range)));
            }

            @Override
            public Mono<Long> currentPosition() {
                return delegate.currentPosition();
            }

            @Override
            public boolean writesPosition() {
                return delegate.writesPosition();
            }
        };
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:occurrent:test")).withType("Updated").build();
    }
}
