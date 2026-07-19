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
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.DcbSubscriptionFilter;
import org.occurrent.subscription.StartAt;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.subscription.api.blocking.CheckpointStorage;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> delivered.add(ce.getId()));

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
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> delivered.add(ce.getId()));

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
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> delivered.add(ce.getId()));

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
                .subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> firstRun.add(ce.getId()));
        assertThat(firstRun).containsExactly("1", "2");

        // Restart: fresh feed and model, same store and marker. The replay is skipped.
        PushSubscriptionModel feed2 = new PushSubscriptionModel();
        sink.set(feed2);
        List<String> secondRun = new ArrayList<>();
        new CatchupThenPushSubscriptionModel(store, feed2, marker)
                .subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> secondRun.add(ce.getId()));
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

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, null, 10, 2);
        Throwable thrown = catchThrowable(() ->
                model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> {
                }));

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
    void a_catch_up_failure_makes_the_live_feed_fail_fast() {
        PushSubscriptionModel liveFeed = new PushSubscriptionModel();
        PositionOrderedReader failingReader = failingReader();

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(failingReader, liveFeed, null);

        Throwable replayFailure = catchThrowable(() ->
                model.subscribe("sub", null, StartAt.subscriptionModelDefault(), cloudEvent -> {
                }));
        assertThat(replayFailure).isInstanceOf(IllegalStateException.class).hasMessageContaining("replay boom");

        Throwable thrown = catchThrowable(() -> liveFeed.accept(cloudEvent("1", "Created")));

        assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("Catch-up failed");
    }

    @Test
    void a_reader_that_does_not_write_positions_fails_fast_at_construction() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        PositionOrderedReader reader = positionlessReader();

        Throwable thrown = catchThrowable(() -> new CatchupThenPushSubscriptionModel(reader, feed, null));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("writesPosition");
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

    private static final class InMemoryCheckpointStorage implements CheckpointStorage {
        private final Map<String, Checkpoint> checkpoints = new HashMap<>();

        @Override
        public Checkpoint read(String subscriptionId) {
            return checkpoints.get(subscriptionId);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint) {
            checkpoints.put(subscriptionId, checkpoint);
            return checkpoint;
        }

        @Override
        public void delete(String subscriptionId) {
            checkpoints.remove(subscriptionId);
        }

        @Override
        public boolean exists(String subscriptionId) {
            return checkpoints.containsKey(subscriptionId);
        }
    }
}
