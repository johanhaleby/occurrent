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
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The catch-up marker is the position the next catch-up resumes after (ADR 179). Every start in these tests is a fresh
 * model over the same store and marker storage, which is what a restart is.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupThenPushSubscriptionModelResumePositionTest {

    private final AtomicReference<PushSubscriptionModel> liveFeed = new AtomicReference<>();
    private final InMemoryEventStore store = new InMemoryEventStore(events -> {
        PushSubscriptionModel current = liveFeed.get();
        if (current != null) {
            current.accept(events);
        }
    });

    @Test
    void a_marker_that_survives_a_cancel_makes_the_next_start_replay_only_the_events_after_it() throws Exception {
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage() {
            @Override
            public void delete(String subscriptionId) {
                // The delete the store loses in a failover.
            }
        };
        store.write("s", List.of(event("1"), event("2")));
        long head = store.currentPosition();
        CatchupThenPushSubscriptionModel first = start(store, marker, new CopyOnWriteArrayList<>());
        first.cancelSubscription("proj");
        assertThat(positionStoredBy(marker)).as("marker still stored after the cancel").isEqualTo(head);
        store.write("s", List.of(event("3")));

        List<String> delivered = new CopyOnWriteArrayList<>();
        start(store, marker, delivered);

        assertThat(delivered).as("replayed after the surviving marker, nothing before it and nothing lost").containsExactly("3");
    }

    @Test
    void a_late_save_of_an_older_head_only_lengthens_the_next_replay() throws Exception {
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();
        store.write("s", List.of(event("1"), event("2")));
        long olderHead = store.currentPosition();
        start(store, marker, new CopyOnWriteArrayList<>());
        store.write("s", List.of(event("3")));
        start(store, marker, new CopyOnWriteArrayList<>());
        assertThat(positionStoredBy(marker)).as("newer head stored by the second start").isGreaterThan(olderHead);
        marker.save("proj", GlobalCheckpoint.of(olderHead));
        store.write("s", List.of(event("4")));

        List<String> delivered = new CopyOnWriteArrayList<>();
        start(store, marker, delivered);

        assertThat(delivered).as("events after the older head, replayed again and none skipped").containsExactly("3", "4");
    }

    @Test
    void a_restart_replays_the_events_since_the_previous_start_and_delivers_a_replayed_and_live_event_once() throws Exception {
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();
        store.write("s", List.of(event("1"), event("2")));
        start(store, marker, new CopyOnWriteArrayList<>());
        store.write("s", List.of(event("3"), event("4")));

        // Event 4 also reaches the live feed while the replay is reading it, which is the replay-to-live overlap.
        PushSubscriptionModel feed = new PushSubscriptionModel();
        liveFeed.set(feed);
        PositionOrderedReader overlapping = decorated(store, stream -> stream.peek(e -> {
            if (e.getId().equals("4")) {
                feed.accept(e);
            }
        }));
        List<String> delivered = new CopyOnWriteArrayList<>();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(overlapping, feed, marker);
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> delivered.add(ce.getId())).waitUntilStarted();
        awaitCaughtUp(model);

        assertThat(delivered).containsExactly("3", "4");
    }

    @Test
    void the_stored_marker_is_the_head_read_before_the_replay_and_not_the_head_when_it_is_marked() throws Exception {
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();
        store.write("s", List.of(event("1"), event("2")));
        long headBeforeTheReplay = store.currentPosition();

        // An event is written while the replay reads, so the head has moved by the time the marker is written.
        PushSubscriptionModel feed = new PushSubscriptionModel();
        liveFeed.set(feed);
        boolean[] written = {false};
        PositionOrderedReader writingWhileReading = decorated(store, stream -> stream.peek(e -> {
            if (!written[0]) {
                written[0] = true;
                store.write("s", List.of(event("during")));
            }
        }));
        List<String> delivered = new CopyOnWriteArrayList<>();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(writingWhileReading, feed, marker);
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> delivered.add(ce.getId())).waitUntilStarted();
        awaitCaughtUp(model);

        assertThat(store.currentPosition()).as("the head moved during the replay").isGreaterThan(headBeforeTheReplay);
        assertThat(positionStoredBy(marker)).isEqualTo(headBeforeTheReplay);
        assertThat(delivered).as("the event written during the replay reaches the projection once, through the live feed").containsExactly("1", "2", "during");
    }

    @Test
    void an_event_that_commits_after_the_head_was_read_at_a_position_before_it_reaches_the_projection_once_through_the_live_feed() throws Exception {
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();
        store.write("s", List.of(event("1"), event("2")));
        long head = store.currentPosition();

        // The broker delivers a published event only to the registrations that exist when it is published. The late
        // event X holds a position at or before the head, so the replay, which reads what was committed before it,
        // does not return it, and only the live registration can.
        PushSubscriptionModel broker = new PushSubscriptionModel();
        liveFeed.set(broker);
        PositionOrderedReader reservedBeforeTheHeadCommittedAfter = new PositionOrderedReader() {
            private final AtomicBoolean published = new AtomicBoolean();

            @Override
            public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return store.readInPositionOrder(filter, range);
            }

            @Override
            public long currentPosition() {
                long read = store.currentPosition();
                if (published.compareAndSet(false, true)) {
                    broker.accept(event("X"));
                }
                return read;
            }

            @Override
            public boolean writesPosition() {
                return store.writesPosition();
            }
        };
        List<String> delivered = new CopyOnWriteArrayList<>();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reservedBeforeTheHeadCommittedAfter, broker, marker);
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> delivered.add(ce.getId())).waitUntilStarted();
        awaitCaughtUp(model);

        assertThat(delivered).as("X delivered once, by the live registration made before the head was read").containsExactlyInAnyOrder("1", "2", "X");
        assertThat(positionStoredBy(marker)).isEqualTo(head);

        List<String> afterRestart = new CopyOnWriteArrayList<>();
        start(store, marker, afterRestart);
        assertThat(afterRestart).as("the next start replays after the marker and X is not in the store").isEmpty();
    }

    @Test
    void a_marker_stored_by_0_34_0_is_read_as_a_resume_position() throws Exception {
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();
        store.write("s", List.of(event("1"), event("2"), event("3")));
        marker.save("proj", GlobalCheckpoint.of(store.currentPosition() - 1));

        List<String> delivered = new CopyOnWriteArrayList<>();
        start(store, marker, delivered);

        assertThat(delivered).containsExactly("3");
    }

    @Test
    void the_catchup_listener_hears_both_signals_on_a_start_from_the_beginning_and_on_a_start_after_a_marker() throws Exception {
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();
        store.write("s", List.of(event("1")));
        SignalLog fromTheBeginning = new SignalLog();
        start(store, marker, new CopyOnWriteArrayList<>(), fromTheBeginning);
        store.write("s", List.of(event("2")));
        SignalLog afterTheMarker = new SignalLog();
        start(store, marker, new CopyOnWriteArrayList<>(), afterTheMarker);

        assertThat(fromTheBeginning.signals).containsExactly("started", "historyRead");
        assertThat(afterTheMarker.signals).containsExactly("started", "historyRead");
    }

    private CatchupThenPushSubscriptionModel start(PositionOrderedReader reader, InMemoryCheckpointStorage marker, List<String> delivered) throws Exception {
        return start(reader, marker, delivered, null);
    }

    private CatchupThenPushSubscriptionModel start(PositionOrderedReader reader, InMemoryCheckpointStorage marker, List<String> delivered,
                                                   CatchupListener listener) throws Exception {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        liveFeed.set(feed);
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, marker);
        if (listener != null) {
            model.listenForCatchup("proj", listener);
        }
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> delivered.add(ce.getId())).waitUntilStarted();
        awaitCaughtUp(model);
        return model;
    }

    // The marker is written before the replay entry is removed, so a finished catch-up means the marker is stored.
    private static void awaitCaughtUp(CatchupThenPushSubscriptionModel model) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!(!model.isCatchingUp("proj") && model.isRunning("proj")) && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(model.isCatchingUp("proj")).as("catch-up finished").isFalse();
    }

    private static long positionStoredBy(InMemoryCheckpointStorage marker) {
        return GlobalCheckpoint.positionOf(marker.read("proj"));
    }

    private static PositionOrderedReader decorated(InMemoryEventStore delegate, java.util.function.UnaryOperator<Stream<CloudEvent>> onRead) {
        return new PositionOrderedReader() {
            @Override
            public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return onRead.apply(delegate.readInPositionOrder(filter, range));
            }

            @Override
            public long currentPosition() {
                return delegate.currentPosition();
            }

            @Override
            public boolean writesPosition() {
                return delegate.writesPosition();
            }
        };
    }

    private static final class SignalLog implements CatchupListener {
        private final List<String> signals = new CopyOnWriteArrayList<>();

        @Override
        public void catchupStarted(Object episode) {
            signals.add("started");
        }

        @Override
        public void historyRead(Object episode) {
            signals.add("historyRead");
        }
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:occurrent:test")).withType("Updated").build();
    }
}
