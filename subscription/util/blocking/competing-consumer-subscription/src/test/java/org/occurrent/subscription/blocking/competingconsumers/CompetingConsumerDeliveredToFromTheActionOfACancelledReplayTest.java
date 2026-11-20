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

package org.occurrent.subscription.blocking.competingconsumers;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.internal.CatchupReadScope;
import org.occurrent.eventstore.api.blocking.internal.CatchupReads;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.RedeliveringSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.occurrent.subscription.blocking.durable.catchup.StartAtTime;

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * The action of a subscription whose catch-up replay a cancel has ended is still running while the cancel waits for it,
 * and the stage the catch-up model set for that call is complete by then. An event that this action delivers, on the
 * same thread, to a competing subscription of another model must reach that subscription's action, whether the other
 * subscription has the id of the cancelled one or another id.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class CompetingConsumerDeliveredToFromTheActionOfACancelledReplayTest {

    private static final String NODE = "node";
    private static final String OTHER_NODE = "other-node";

    private final List<String> reachedTheOtherSubscription = new CopyOnWriteArrayList<>();
    private final List<Throwable> thrownInTheAction = new CopyOnWriteArrayList<>();
    private final CountDownLatch theActionHasReturned = new CountDownLatch(1);
    private final AtomicReference<@Nullable Thread> cancel = new AtomicReference<>();
    private final DirectModel otherWrappedModel = new DirectModel();
    private final CompetingConsumerSubscriptionModel otherModel = new CompetingConsumerSubscriptionModel(otherWrappedModel, new AlwaysTheLeaseHolder());
    private @Nullable CatchupSubscriptionModel catchup;
    private @Nullable CompetingConsumerSubscriptionModel competing;

    @AfterEach
    void shutdown() throws InterruptedException {
        Thread cancelThread = cancel.get();
        if (cancelThread != null) {
            cancelThread.join(5000);
        }
        if (competing != null) {
            competing.shutdown();
        }
        if (catchup != null) {
            catchup.shutdown();
        }
        otherModel.shutdown();
    }

    @Test
    void an_event_the_action_of_a_cancelled_competing_replay_delivers_to_a_competing_subscription_with_the_same_id_reaches_that_subscriptions_action() throws InterruptedException {
        // Given a competing subscription "x" holding the lease, whose replay event is in its action, and a competing subscription "x" in another model
        catchup = new CatchupSubscriptionModel(new RedeliveringLiveModel(), new HistoryOfOneEvent(), new CatchupSubscriptionModelConfig(100));
        competing = new CompetingConsumerSubscriptionModel(catchup, new AlwaysTheLeaseHolder());
        otherModel.start();
        otherModel.subscribe(OTHER_NODE, "x", null, StartAt.now(), event -> reachedTheOtherSubscription.add(event.getId()));
        competing.start();

        // When the cancel of "x" has completed the stage of the replay event, which it does before it waits for the action, and the action then delivers on its own thread
        competing.subscribe(NODE, "x", null, StartAt.dynamic(context -> context.hasSubscriptionModelType(CatchupSubscriptionModel.class) ? StartAtTime.beginningOfTime() : StartAt.now()),
                event -> deliverOnceTheCancelWaitsForTheAction(() -> competing.cancelSubscription("x"), "x"));

        // Then
        assertThat(theActionHasReturned.await(10, TimeUnit.SECONDS)).as("the action of the cancelled subscription returned within 10 seconds").isTrue();
        assertAll(
                () -> assertThat(thrownInTheAction).as("what the delivery to the other subscription threw in the action of the cancelled one").isEmpty(),
                () -> assertThat(reachedTheOtherSubscription).as("the events that reached the action of the other subscription with the same id").containsExactly("delivered-from-the-action"));
    }

    @Test
    void an_event_the_action_of_a_cancelled_catchup_replay_delivers_to_a_competing_subscription_with_another_id_reaches_that_subscriptions_action() throws InterruptedException {
        // Given a catch-up subscription "a" with no competing consumer above it, whose replay event is in its action, and a competing subscription "b" in another model
        catchup = new CatchupSubscriptionModel(new RedeliveringLiveModel(), new HistoryOfOneEvent(), new CatchupSubscriptionModelConfig(100));
        otherModel.start();
        otherModel.subscribe(OTHER_NODE, "b", null, StartAt.now(), event -> reachedTheOtherSubscription.add(event.getId()));
        catchup.start();

        // When the cancel of "a" has completed the stage of the replay event, which it does before it waits for the action, and the action then delivers on its own thread
        catchup.subscribe("a", null, StartAt.dynamic(context -> StartAtTime.beginningOfTime()),
                event -> deliverOnceTheCancelWaitsForTheAction(() -> catchup.cancelSubscription("a"), "b"));

        // Then
        assertThat(theActionHasReturned.await(10, TimeUnit.SECONDS)).as("the action of the cancelled subscription returned within 10 seconds").isTrue();
        assertAll(
                () -> assertThat(thrownInTheAction).as("what the delivery to the other subscription threw in the action of the cancelled one").isEmpty(),
                () -> assertThat(reachedTheOtherSubscription).as("the events that reached the action of the competing subscription with another id").containsExactly("delivered-from-the-action"));
    }

    // Runs on the replay thread, inside the action. The cancel runs on a thread of its own and completes the stage of this
    // call before it waits, at most a second, for the action to return. The stack of that thread in its timed wait in
    // awaitTheAdmittedActions is how this sees that the stage is complete, so the delivery comes after it.
    private void deliverOnceTheCancelWaitsForTheAction(Runnable theCancel, String idOfTheOtherSubscription) {
        try {
            Thread cancelThread = new Thread(theCancel, "cancel");
            cancel.set(cancelThread);
            cancelThread.start();
            await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(1)).until(() -> cancelThread.getState() == Thread.State.TIMED_WAITING
                    && Arrays.stream(cancelThread.getStackTrace()).anyMatch(frame -> frame.getMethodName().equals("awaitTheAdmittedActions")));
            otherWrappedModel.deliver(idOfTheOtherSubscription, event("delivered-from-the-action"));
        } catch (Throwable e) {
            thrownInTheAction.add(e);
        } finally {
            theActionHasReturned.countDown();
        }
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("Tested").build();
    }

    private static final class AlwaysTheLeaseHolder implements CompetingConsumerStrategy {
        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            return true;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            return true;
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
        }
    }

    // A subscription model that calls the consumer of a subscription on the thread that calls deliver
    private static final class DirectModel implements SubscriptionModel {
        private final java.util.Map<String, Consumer<CloudEvent>> consumers = new java.util.concurrent.ConcurrentHashMap<>();

        void deliver(String subscriptionId, CloudEvent event) {
            Consumer<CloudEvent> consumer = consumers.get(subscriptionId);
            if (consumer == null) {
                throw new IllegalStateException("No consumer for " + subscriptionId);
            }
            consumer.accept(event);
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            consumers.put(subscriptionId, action);
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
            return consumers.containsKey(subscriptionId);
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
            consumers.remove(subscriptionId);
        }
    }

    // A live model that subscribes nothing, so only the replay delivers
    private static final class RedeliveringLiveModel implements CheckpointAwareSubscriptionModel, RedeliveringSubscriptions {
        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
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
        public boolean redeliversAnAttemptNotMade() {
            return true;
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
            return false;
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
        }
    }

    // The history a replay reads: one event, read by time, since this store writes no position
    private static final class HistoryOfOneEvent implements EventStoreQueries, CatchupReads {
        @Override
        public CatchupReadScope openCatchupReadScope(@Nullable String notBefore) {
            return new CatchupReadScope() {
                @Override
                public @Nullable String liveStart() {
                    return null;
                }

                @Override
                public long currentPosition() {
                    return 0;
                }

                @Override
                public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    return Stream.empty();
                }

                @Override
                public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
                    return Stream.of(event("replayed"));
                }

                @Override
                public void close() {
                }
            };
        }

        @Override
        public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
            return Stream.of(event("replayed"));
        }

        @Override
        public long count(Filter filter) {
            return 1;
        }

        @Override
        public boolean exists(Filter filter) {
            return true;
        }
    }
}
