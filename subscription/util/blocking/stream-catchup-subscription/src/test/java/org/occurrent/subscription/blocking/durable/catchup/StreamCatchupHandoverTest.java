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

package org.occurrent.subscription.blocking.durable.catchup;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.*;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Handover-seam tests for the blocking {@link StreamCatchupSubscriptionModel} position path, driven by a fake position
 * store and a fake live source so the reserve-low-position-commit-late ordering and the during-replay overlap can be
 * reproduced deterministically. The blocking position path caches only the reconcile-phase ids (its live resume token
 * is exclusive, so bulk events below the token are not re-delivered), so the deduped overlap here is the events written
 * during the replay, which the live source re-delivers.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupHandoverTest {

    @Test
    void a_low_position_event_that_commits_after_the_handover_advanced_past_it_is_still_delivered_exactly_once() {
        // The bulk read sees positions 1, 3, 4, 5 but position 2 was reserved before commit (ADR 45) and committed late,
        // after the forward-only replay passed it, so neither bulk nor reconcile reads it. It committed after the
        // pre-bulk resume token, so the live source delivers it (e2). An id-based dedup delivers it; a position
        // watermark that dropped live events with position <= 5 would lose it.
        FakePositionStore store = FakePositionStore.withEventsAt(1, 3, 4, 5).heads(5, 5);
        FakeLiveModel live = new FakeLiveModel(events("e2"));
        CopyOnWriteArrayList<String> received = deliver(live, store, 1000);

        assertThat(received).containsExactly("e1", "e3", "e4", "e5", "e2");
    }

    @Test
    void an_overlap_larger_than_the_old_1000_cap_delivers_each_event_exactly_once_when_the_ceiling_covers_it() {
        // Bulk drains 1..1000, then the head advances to 3000, so reconcile drains the 2000 events written during the
        // replay (an overlap far past the old fixed 1000 cap). The live source re-delivers those 2000, and a ceiling
        // that covers them dedupes the whole re-delivery, so every event is delivered exactly once.
        FakePositionStore store = FakePositionStore.withEventsInRange(1, 3000).heads(1000, 3000);
        FakeLiveModel live = new FakeLiveModel(eventsInRange(1001, 3000));
        CopyOnWriteArrayList<String> received = deliver(live, store, 5000);

        assertThat(received).hasSize(3000);
        assertThat(received).doesNotHaveDuplicates();
        assertThat(Set.copyOf(received)).isEqualTo(idsInRange(1, 3000));
    }

    @Test
    void an_overlap_beyond_the_ceiling_may_be_delivered_more_than_once_but_is_never_lost() {
        // Bulk drains 1..100, then the head advances to 600, so reconcile drains 500 during-replay events. The ceiling
        // is 100, so the oldest reconcile ids are evicted and re-delivered by the live source. Eviction can only cause
        // a duplicate, never a loss, so every event still appears at least once.
        FakePositionStore store = FakePositionStore.withEventsInRange(1, 600).heads(100, 600);
        FakeLiveModel live = new FakeLiveModel(eventsInRange(101, 600));
        CopyOnWriteArrayList<String> received = deliver(live, store, 100);

        assertThat(Set.copyOf(received)).isEqualTo(idsInRange(1, 600));
        assertThat(received.size()).isGreaterThan(600); // duplicates occurred, which is allowed
    }

    private CopyOnWriteArrayList<String> deliver(FakeLiveModel live, FakePositionStore store, int ceiling) {
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel catchup = new StreamCatchupSubscriptionModel(live, store, new CatchupSubscriptionModelConfig(ceiling));
        boolean started = catchup.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> received.add(cloudEvent.getId()))
                .waitUntilStarted(Duration.ofSeconds(10));
        assertThat(started).isTrue();
        return received;
    }

    private static List<CloudEvent> events(String... ids) {
        return java.util.Arrays.stream(ids).map(StreamCatchupHandoverTest::event).toList();
    }

    private static List<CloudEvent> eventsInRange(long fromInclusive, long toInclusive) {
        return LongStream.rangeClosed(fromInclusive, toInclusive).mapToObj(position -> event(id(position))).toList();
    }

    private static Set<String> idsInRange(long fromInclusive, long toInclusive) {
        return LongStream.rangeClosed(fromInclusive, toInclusive).mapToObj(StreamCatchupHandoverTest::id).collect(java.util.stream.Collectors.toSet());
    }

    private static String id(long position) {
        return "e" + position;
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("type").build();
    }

    // A position-ordered store whose currentPosition answers a scripted sequence of heads (bulk head, then reconcile
    // snapshot) so a test can make the head advance mid-replay. readInPositionOrder honors the range bounds. The
    // time-based query paths must never run in position mode, so they fail loudly.
    private static final class FakePositionStore implements EventStoreQueries, PositionOrderedReader {
        private final TreeMap<Long, CloudEvent> byPosition = new TreeMap<>();
        private long[] heads = {0L};
        private int headIndex = 0;

        static FakePositionStore withEventsAt(long... positions) {
            FakePositionStore store = new FakePositionStore();
            for (long position : positions) {
                store.byPosition.put(position, event(id(position)));
            }
            return store;
        }

        static FakePositionStore withEventsInRange(long fromInclusive, long toInclusive) {
            FakePositionStore store = new FakePositionStore();
            LongStream.rangeClosed(fromInclusive, toInclusive).forEach(position -> store.byPosition.put(position, event(id(position))));
            return store;
        }

        FakePositionStore heads(long... heads) {
            this.heads = heads;
            return this;
        }

        @Override
        public synchronized long currentPosition() {
            long value = heads[Math.min(headIndex, heads.length - 1)];
            headIndex++;
            return value;
        }

        @Override
        public boolean writesPosition() {
            return true;
        }

        @Override
        public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            long fromExclusive = range.afterPosition().orElse(0L);
            long toInclusive = range.upToPosition().orElse(Long.MAX_VALUE);
            return byPosition.subMap(fromExclusive, false, toInclusive, true).values().stream();
        }

        @Override
        public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
            throw new AssertionError("Position mode must not use the time-based query path");
        }

        @Override
        public long count(Filter filter) {
            throw new AssertionError("Position mode must not use the time-based count path");
        }

        @Override
        public boolean exists(Filter filter) {
            throw new AssertionError("Position mode must not use the time-based exists path");
        }
    }

    // A live source whose subscribe replays a fixed, finite list to the delivery consumer the catch-up hands it (which
    // wraps the dedup cache), then returns a started subscription. globalCheckpoint is non-null so the fail-loud
    // handover proceeds.
    private static final class FakeLiveModel implements CheckpointAwareSubscriptionModel {
        private final List<CloudEvent> live;

        private FakeLiveModel(List<CloudEvent> live) {
            this.live = live;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            live.forEach(action);
            return new StartedSubscription(subscriptionId);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return new StringBasedCheckpoint("token");
        }

        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return true;
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return new StartedSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
        }
    }

    private record StartedSubscription(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }
}
