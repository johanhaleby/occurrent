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
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.subscription.*;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Handover-seam test for the blocking {@link DcbCatchupSubscriptionModel}, the DCB mirror of
 * {@link StreamCatchupHandoverTest}: a fake DCB reader and a fake live source reproduce the during-replay overlap
 * deterministically, so the (id, source) dedup can be proven without a database.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbCatchupHandoverTest {

    private static final DcbCriteria QUERY = DcbCriteria.tags(Tag.parse("name:1"));

    @Test
    void a_live_event_sharing_only_its_id_with_a_reconciled_event_is_delivered_and_not_suppressed() {
        // e1 from producer A is read during the reconcile phase (the head advances between the bulk read and the
        // reconcile snapshot) and recorded in the dedup cache. A live event that shares only its id, from producer B,
        // is a different event under CloudEvents' (id, source) identity and must still be delivered, not suppressed
        // as a re-delivery of the reconciled one.
        FakeDcbEventStore store = FakeDcbEventStore.withEventsAt(1).heads(0, 1);
        CloudEvent fromB = taggedEvent("e1", URI.create("urn:producer:b"));
        FakeLiveModel live = new FakeLiveModel(List.of(fromB));

        CopyOnWriteArrayList<String> received = deliver(live, store, 1000);

        assertThat(received).containsExactly("e1@urn:test", "e1@urn:producer:b");
    }

    private CopyOnWriteArrayList<String> deliver(FakeLiveModel live, FakeDcbEventStore store, int ceiling) {
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        DcbCatchupSubscriptionModel catchup = new DcbCatchupSubscriptionModel(live, store, QUERY, new CatchupSubscriptionModelConfig(ceiling));
        boolean started = catchup.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> received.add(cloudEvent.getId() + "@" + cloudEvent.getSource()))
                .waitUntilStarted(Duration.ofSeconds(10));
        assertThat(started).isTrue();
        return received;
    }

    private static CloudEvent taggedEvent(String id, URI source) {
        CloudEvent event = CloudEventBuilder.v1().withId(id).withSource(source).withType("type").build();
        return DcbCloudEvents.withTags(event, List.of(Tag.parse("name:1")));
    }

    private static String id(long position) {
        return "e" + position;
    }

    // A DCB store whose head read (DcbReadOptions.between(0, 0), the degenerate range DcbCatchupSubscriptionModel
    // uses for currentHead()) answers a scripted sequence (bulk head, then reconcile snapshot), so a test can make
    // the head advance mid-replay. A real window read never uses that degenerate range (the pipeline skips an
    // empty window without reading), so this only ever intercepts the head check.
    private static final class FakeDcbEventStore implements DcbEventStore {
        private final TreeMap<Long, CloudEvent> byPosition = new TreeMap<>();
        private long[] heads = {0L};
        private int headIndex = 0;

        static FakeDcbEventStore withEventsAt(long... positions) {
            FakeDcbEventStore store = new FakeDcbEventStore();
            for (long position : positions) {
                store.byPosition.put(position, taggedEvent(id(position), URI.create("urn:test")));
            }
            return store;
        }

        FakeDcbEventStore heads(long... heads) {
            this.heads = heads;
            return this;
        }

        private synchronized long nextHead() {
            long value = heads[Math.min(headIndex, heads.length - 1)];
            headIndex++;
            return value;
        }

        @Override
        public DcbEventStream read(DcbCriteria criteria, DcbReadOptions options) {
            PositionRange range = options.positionRange();
            long fromExclusive = range.afterPosition().orElse(0L);
            long toInclusive = range.upToPosition().orElse(0L);
            if (fromExclusive == 0 && toInclusive == 0) {
                return new DcbEventStream(List.of(), nextHead());
            }
            List<CloudEvent> events = byPosition.subMap(fromExclusive, false, toInclusive, true).values().stream().toList();
            return new DcbEventStream(events, 0);
        }

        @Override
        public DcbAppendResult append(List<CloudEvent> events) {
            throw new AssertionError("Not used by this test");
        }

        @Override
        public DcbAppendResult append(List<CloudEvent> events, DcbAppendCondition condition) {
            throw new AssertionError("Not used by this test");
        }
    }

    // A live source whose subscribe replays a fixed, finite list to the delivery consumer the catch-up hands it
    // (which wraps the dedup cache), then returns a started subscription. globalCheckpoint is non-null so the
    // fail-loud handover proceeds.
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
