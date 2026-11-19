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
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.internal.CatchupReadScope;
import org.occurrent.eventstore.api.blocking.internal.CatchupReads;
import org.occurrent.eventstore.api.dcb.*;
import org.occurrent.eventstore.api.dcb.internal.DcbCatchupReadScope;
import org.occurrent.eventstore.api.dcb.internal.DcbCatchupReads;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.DcbSubscriptionFilter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.HistoryLossReportingSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A live subscription of a composite catch-up model that the live delegate calls the action of while the composite
 * shuts down, after the first child has forgotten the state all the children share, still gets the event to its action
 * and the delegate gets no {@code AttemptNotMade}. A delegate that is still delivering when its own shutdown begins
 * does exactly this, for a subscription that belongs to a child shut down after the first.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class CatchupSubscriptionModelShutdownLiveCallTest {

    private static final String ID = "live";

    private final KeepsTheLiveAction live = new KeepsTheLiveAction();
    private final List<String> received = new CopyOnWriteArrayList<>();
    private final CatchupSubscriptionModel catchup = new CatchupSubscriptionModel(live, new UnusedEventStoreQueries(), new UnusedDcbEventStore(), DcbCriteria.tags(Tag.parse("name:1")), new CatchupSubscriptionModelConfig(1));

    @Test
    void a_live_event_the_live_delegate_calls_from_its_own_shutdown_reaches_the_action_of_a_dcb_subscription() {
        // Given
        catchup.subscribe(ID, DcbSubscriptionFilter.filter(DcbCriteria.tags(Tag.parse("name:1"))), StartAt.now(), event -> received.add(event.getId()));
        live.whenItShutsDown(() -> live.callTheKeptAction(ID, aLiveEvent()));

        // When
        catchup.shutdown();

        // Then
        assertAll(
                () -> assertThat(live.thrownByTheAction).as("what the live call threw").isEmpty(),
                () -> assertThat(received).as("the events the action received").containsExactly("live1"));
    }

    @Test
    void a_live_event_the_live_delegate_calls_from_its_own_shutdown_reaches_the_action_of_an_agnostic_subscription() {
        // Given
        catchup.subscribe(ID, AgnosticSubscriptionFilter.filter(Filter.all()), StartAt.now(), event -> received.add(event.getId()));
        live.whenItShutsDown(() -> live.callTheKeptAction(ID, aLiveEvent()));

        // When
        catchup.shutdown();

        // Then
        assertAll(
                () -> assertThat(live.thrownByTheAction).as("what the live call threw").isEmpty(),
                () -> assertThat(received).as("the events the action received").containsExactly("live1"));
    }

    @Test
    void a_live_event_the_live_delegate_calls_once_the_first_child_has_forgotten_the_shared_state_reaches_the_action_of_a_dcb_subscription() {
        // Given the first child to shut down takes the history loss listener off the live delegate after it forgot the shared state
        catchup.subscribe(ID, DcbSubscriptionFilter.filter(DcbCriteria.tags(Tag.parse("name:1"))), StartAt.now(), event -> received.add(event.getId()));
        live.whenAHistoryLossListenerIsRemovedTheFirstTime(() -> live.callTheKeptAction(ID, aLiveEvent()));

        // When
        catchup.shutdown();

        // Then
        assertAll(
                () -> assertThat(live.thrownByTheAction).as("what the live call threw").isEmpty(),
                () -> assertThat(received).as("the events the action received").containsExactly("live1"));
    }

    // Carries the tag of the DCB subscription, which a DCB child checks the live events of
    private static CloudEvent aLiveEvent() {
        return DcbCloudEvents.withTags(CloudEventBuilder.v1().withId("live1").withSource(URI.create("urn:test")).withType("Tested").build(), List.of(Tag.parse("name:1")));
    }

    // Keeps the action it was given, as a live delegate that is mid-delivery does, and calls it when told to. Also a
    // model that reports history loss, since the catch-up model takes its listener off at the start of every child's
    // shutdown, which is a point in the shutdown after the first child but before the later ones.
    private static final class KeepsTheLiveAction implements CheckpointAwareSubscriptionModel, HistoryLossReportingSubscriptions {
        private final Map<String, Consumer<CloudEvent>> kept = new ConcurrentHashMap<>();
        private final List<Throwable> thrownByTheAction = new CopyOnWriteArrayList<>();
        private final AtomicBoolean firstRemoval = new AtomicBoolean(true);
        private final AtomicReference<Runnable> onShutdown = new AtomicReference<>(() -> {
        });
        private final AtomicReference<Runnable> onFirstRemoval = new AtomicReference<>(() -> {
        });

        void whenItShutsDown(Runnable action) {
            onShutdown.set(action);
        }

        void whenAHistoryLossListenerIsRemovedTheFirstTime(Runnable action) {
            onFirstRemoval.set(action);
        }

        void callTheKeptAction(String subscriptionId, CloudEvent event) {
            try {
                kept.get(subscriptionId).accept(event);
            } catch (Throwable e) {
                thrownByTheAction.add(e);
            }
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            kept.put(subscriptionId, action);
            return new Subscription() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public boolean waitUntilStarted(Duration timeout) {
                    return true;
                }
            };
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return new StringBasedCheckpoint("live start read now");
        }

        @Override
        public boolean canResumeFrom(Checkpoint checkpoint) {
            return true;
        }

        @Override
        public void addHistoryLossListener(HistoryLossListener listener) {
        }

        @Override
        public void removeHistoryLossListener(HistoryLossListener listener) {
            if (firstRemoval.compareAndSet(true, false)) {
                onFirstRemoval.get().run();
            }
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
            return kept.containsKey(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            kept.remove(subscriptionId);
        }

        @Override
        public void shutdown() {
            onShutdown.get().run();
        }
    }

    // Never read from, since a subscribe at now replays nothing
    private static final class UnusedEventStoreQueries implements EventStoreQueries, CatchupReads {
        @Override
        public CatchupReadScope openCatchupReadScope(@Nullable String notBefore) {
            return TimeOrderedCatchupReads.over(this).openCatchupReadScope(notBefore);
        }

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

    // Never read from, since a subscribe at now replays nothing
    private static final class UnusedDcbEventStore implements DcbEventStore, DcbCatchupReads {
        @Override
        public DcbCatchupReadScope openDcbCatchupReadScope(@Nullable String notBefore) {
            return DcbCatchupReads.unscoped(this).openDcbCatchupReadScope(notBefore);
        }

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
