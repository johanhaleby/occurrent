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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.dcb.*;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * No-Mongo regression guard for the dual-mode {@link CatchupSubscriptionModel} dispatcher: {@code stop},
 * {@code start}, {@code cancelSubscription} and {@code shutdown} must reach the shared
 * {@link CheckpointAwareSubscriptionModel} delegate exactly once, not once per inner model. A counting fake delegate
 * is enough here, no actual catch-up needs to run.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupSubscriptionModelDualModeLifecycleTest {

    @Test
    void cancel_subscription_invokes_the_shared_delegates_cancel_subscription_exactly_once_in_dual_mode() {
        CountingCheckpointAwareSubscriptionModel delegate = new CountingCheckpointAwareSubscriptionModel();
        CatchupSubscriptionModel dualMode = dualMode(delegate);

        dualMode.cancelSubscription("someId");

        assertThat(delegate.cancelSubscriptionCount("someId")).isEqualTo(1);
    }

    @Test
    void shutdown_invokes_the_shared_delegates_shutdown_exactly_once_in_dual_mode() {
        CountingCheckpointAwareSubscriptionModel delegate = new CountingCheckpointAwareSubscriptionModel();
        CatchupSubscriptionModel dualMode = dualMode(delegate);

        dualMode.shutdown();

        assertThat(delegate.shutdownCount()).isEqualTo(1);
    }

    @Test
    void stop_invokes_the_shared_delegates_stop_exactly_once_in_dual_mode() {
        CountingCheckpointAwareSubscriptionModel delegate = new CountingCheckpointAwareSubscriptionModel();
        CatchupSubscriptionModel dualMode = dualMode(delegate);

        dualMode.stop();

        assertThat(delegate.stopCount()).isEqualTo(1);
    }

    @Test
    void start_invokes_the_shared_delegates_start_exactly_once_in_dual_mode() {
        CountingCheckpointAwareSubscriptionModel delegate = new CountingCheckpointAwareSubscriptionModel();
        CatchupSubscriptionModel dualMode = dualMode(delegate);

        dualMode.start(true);

        assertThat(delegate.startCount()).isEqualTo(1);
    }

    /**
     * Copilot review on PR #839 (issue #827). The three children a dual-mode dispatcher constructs share a live
     * delegate and checkpoint storage, but each is its own {@code AbstractCatchupSubscriptionModel} instance, so
     * without deliberately sharing the handover lock registry between them, an id routed to the stream child on one
     * call and the DCB child on the next would serialize against neither. Reflection is the only way to observe
     * this without running a full race, since the registry is a private implementation detail.
     */
    @Test
    void the_three_children_of_a_dual_mode_dispatcher_share_one_handover_lock_registry() throws Exception {
        CatchupSubscriptionModel dualMode = dualMode(new CountingCheckpointAwareSubscriptionModel());

        Field streamField = CatchupSubscriptionModel.class.getDeclaredField("streamCatchupSubscriptionModel");
        streamField.setAccessible(true);
        Field dcbField = CatchupSubscriptionModel.class.getDeclaredField("dcbCatchupSubscriptionModel");
        dcbField.setAccessible(true);
        Field agnosticField = CatchupSubscriptionModel.class.getDeclaredField("agnosticCatchupSubscriptionModel");
        agnosticField.setAccessible(true);
        Field handoverLocksField = AbstractCatchupSubscriptionModel.class.getDeclaredField("handoverLocks");
        handoverLocksField.setAccessible(true);

        Object streamLocks = handoverLocksField.get(streamField.get(dualMode));
        Object dcbLocks = handoverLocksField.get(dcbField.get(dualMode));
        Object agnosticLocks = handoverLocksField.get(agnosticField.get(dualMode));

        assertThat(streamLocks)
                .as("the stream and DCB children must share the exact same handover lock registry instance, not "
                        + "merely an equal one, since only reference identity serializes a same-id attempt routed "
                        + "to different children on different calls")
                .isSameAs(dcbLocks);
        assertThat(streamLocks).isSameAs(agnosticLocks);
    }

    private static CatchupSubscriptionModel dualMode(CheckpointAwareSubscriptionModel delegate) {
        return new CatchupSubscriptionModel(delegate, new UnusedEventStoreQueries(), new UnusedDcbEventStore(), DcbCriteria.tags(Tag.parse("name:1")), new CatchupSubscriptionModelConfig(1));
    }

    /**
     * Counts calls reaching the shared delegate. {@code cancelSubscriptionCount} is keyed by subscription id so a
     * double-fire from the dual-mode fan-out (once via the stream inner model's path, once via the DCB path) would
     * surface as a count greater than one for that id, rather than being hidden by aggregating across ids.
     * {@code stop}/{@code start} have no id to key by (they are subscription-agnostic lifecycle calls), so a
     * double-fire there would show up directly as their aggregate count exceeding one.
     */
    private static final class CountingCheckpointAwareSubscriptionModel implements CheckpointAwareSubscriptionModel {
        private final AtomicInteger shutdownCount = new AtomicInteger();
        private final AtomicInteger stopCount = new AtomicInteger();
        private final AtomicInteger startCount = new AtomicInteger();
        private final ConcurrentMap<String, AtomicInteger> cancelSubscriptionCounts = new ConcurrentHashMap<>();

        int cancelSubscriptionCount(String subscriptionId) {
            AtomicInteger count = cancelSubscriptionCounts.get(subscriptionId);
            return count == null ? 0 : count.get();
        }

        int shutdownCount() {
            return shutdownCount.get();
        }

        int stopCount() {
            return stopCount.get();
        }

        int startCount() {
            return startCount.get();
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            throw new AssertionError("globalCheckpoint must not be called by cancelSubscription/shutdown");
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            throw new AssertionError("subscribe must not be called by this test");
        }

        @Override
        public void stop() {
            stopCount.incrementAndGet();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            startCount.incrementAndGet();
        }

        @Override
        public boolean isRunning() {
            throw new AssertionError("isRunning must not be called by this test");
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            throw new AssertionError("isRunning must not be called by this test");
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            throw new AssertionError("isPaused must not be called by this test");
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throw new AssertionError("resumeSubscription must not be called by this test");
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            throw new AssertionError("pauseSubscription must not be called by this test");
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            cancelSubscriptionCounts.computeIfAbsent(subscriptionId, __ -> new AtomicInteger()).incrementAndGet();
        }

        @Override
        public void shutdown() {
            shutdownCount.incrementAndGet();
        }
    }

    // Never actually read from since these tests only exercise cancelSubscription/shutdown, not subscribe.
    private static final class UnusedEventStoreQueries implements EventStoreQueries {
        @Override
        public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
            throw new AssertionError("query must not be called by this test");
        }

        @Override
        public long count(Filter filter) {
            throw new AssertionError("count must not be called by this test");
        }

        @Override
        public boolean exists(Filter filter) {
            throw new AssertionError("exists must not be called by this test");
        }
    }

    // Never actually read from since these tests only exercise cancelSubscription/shutdown, not subscribe.
    private static final class UnusedDcbEventStore implements DcbEventStore {
        @Override
        public DcbEventStream read(DcbCriteria criteria, DcbReadOptions options) {
            throw new AssertionError("read must not be called by this test");
        }

        @Override
        public DcbAppendResult append(List<CloudEvent> events) {
            throw new AssertionError("append must not be called by this test");
        }

        @Override
        public DcbAppendResult append(List<CloudEvent> events, DcbAppendCondition condition) {
            throw new AssertionError("append must not be called by this test");
        }
    }
}
