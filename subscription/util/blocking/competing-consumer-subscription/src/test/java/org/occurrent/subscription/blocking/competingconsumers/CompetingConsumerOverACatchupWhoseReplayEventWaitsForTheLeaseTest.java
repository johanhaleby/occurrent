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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.occurrent.subscription.blocking.durable.catchup.StartAtTime;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A replay event of a competing subscription that waits in awaitTheLease, since this node is never given the lease,
 * holds the replay thread inside the action. A cancel or a stop of the competing consumer model has to end that wait
 * itself, instead of leaving the catch-up model underneath to wait a second for an action that cannot return.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class CompetingConsumerOverACatchupWhoseReplayEventWaitsForTheLeaseTest {

    private static final String NODE = "node";
    private static final String ID = "subscription";
    private static final Duration PROMPTLY = Duration.ofMillis(500);

    private final NeverTheLeaseHolder strategy = new NeverTheLeaseHolder();
    private final List<String> received = new CopyOnWriteArrayList<>();
    private final ListAppender<ILoggingEvent> logged = new ListAppender<>();
    private final Logger catchupLog = (Logger) LoggerFactory.getLogger("org.occurrent.subscription.blocking.durable.catchup.AbstractCatchupSubscriptionModel");
    // Counted down when the replay closes the stream of its event, which it does once the action call has returned
    private final CountDownLatch theActionHasExited = new CountDownLatch(1);
    private CatchupSubscriptionModel catchup;
    private CompetingConsumerSubscriptionModel competing;

    @BeforeEach
    void a_competing_consumer_model_whose_replay_event_waits_for_the_lease() throws InterruptedException {
        logged.start();
        catchupLog.addAppender(logged);
        catchup = new CatchupSubscriptionModel(new RedeliveringLiveModel(), new HistoryOfOneEvent(() -> strategy.fenced = true, theActionHasExited::countDown), new CatchupSubscriptionModelConfig(100));
        competing = new CompetingConsumerSubscriptionModel(catchup, strategy);
        competing.start();
        competing.subscribe(NODE, ID, null, StartAt.dynamic(context -> context.hasSubscriptionModelType(CatchupSubscriptionModel.class) ? StartAtTime.beginningOfTime() : StartAt.now()),
                cloudEvent -> received.add(cloudEvent.getId()));
        assertThat(strategy.askedWithoutTheLease.await(5, TimeUnit.SECONDS)).as("the replay event waits for the lease").isTrue();
    }

    @AfterEach
    void shutdown() {
        competing.shutdown();
        catchupLog.detachAppender(logged);
    }

    @Test
    void a_cancel_of_the_subscription_returns_promptly_and_logs_no_warning_about_an_action_that_still_ran() {
        // Given
        assertThat(theActionHasExited.getCount()).as("the action is still in its wait for the lease").isEqualTo(1);

        // When
        long before = System.nanoTime();
        competing.cancelSubscription(ID);
        Duration took = Duration.ofNanos(System.nanoTime() - before);

        // Then
        assertAll(
                () -> assertThat(took).as("how long the cancel took with the replay event waiting for the lease").isLessThan(PROMPTLY),
                () -> assertThat(theActionHasExited.getCount()).as("the action has left its wait for the lease when the cancel returns").isZero(),
                () -> assertThat(stillRanWarnings()).as("the warnings logged").isEmpty(),
                () -> assertThat(received).as("the events the action received").isEmpty());
    }

    @Test
    void a_stop_returns_promptly_and_logs_no_warning_about_an_action_that_still_ran() {
        // Given
        assertThat(theActionHasExited.getCount()).as("the action is still in its wait for the lease").isEqualTo(1);

        // When
        long before = System.nanoTime();
        competing.stop();
        Duration took = Duration.ofNanos(System.nanoTime() - before);

        // Then
        assertAll(
                () -> assertThat(took).as("how long the stop took with the replay event waiting for the lease").isLessThan(PROMPTLY),
                () -> assertThat(theActionHasExited.getCount()).as("the action has left its wait for the lease when the stop returns").isZero(),
                () -> assertThat(stillRanWarnings()).as("the warnings logged").isEmpty(),
                () -> assertThat(received).as("the events the action received").isEmpty());
    }

    @Test
    void a_cancel_that_withdraws_the_event_while_the_lease_is_looked_for_keeps_it_from_the_action_although_the_look_finds_the_lease_held() throws InterruptedException {
        // Given the look for the lease is under way
        strategy.blockTheNextLook.set(true);
        assertThat(strategy.lookInFlight.await(5, TimeUnit.SECONDS)).as("a look for the lease is under way for the replay event").isTrue();
        Thread cancel = new Thread(() -> competing.cancelSubscription(ID));

        try {
            // When the cancel has withdrawn the event and waits for the action, and the look then finds the lease held
            cancel.start();
            await().atMost(Duration.ofSeconds(5)).until(() -> cancel.getState() != Thread.State.RUNNABLE && Arrays.stream(cancel.getStackTrace())
                    .anyMatch(frame -> frame.getMethodName().equals("completesWithin") || frame.getMethodName().equals("awaitTheAdmittedActions")));
            strategy.releaseTheLook.countDown();
            cancel.join(5000);

            // Then
            assertThat(cancel.isAlive()).as("the cancel returned").isFalse();
            assertAll(
                    () -> assertThat(theActionHasExited.getCount()).as("the action has left its wait for the lease when the cancel returns").isZero(),
                    () -> assertThat(stillRanWarnings()).as("the warnings logged").isEmpty(),
                    () -> assertThat(received).as("the events the action received").isEmpty());
        } finally {
            strategy.releaseTheLook.countDown();
            cancel.join(5000);
        }
    }

    @Test
    void a_shutdown_of_the_catchup_model_itself_ends_the_wait_of_the_replay_event_for_the_lease() throws InterruptedException {
        // Given
        assertThat(theActionHasExited.getCount()).as("the action is still in its wait for the lease").isEqualTo(1);

        // When
        catchup.shutdown();

        // Then
        assertAll(
                () -> assertThat(theActionHasExited.await(2, TimeUnit.SECONDS)).as("the action has left its wait for the lease within 2 seconds of the shutdown").isTrue(),
                () -> assertThat(received).as("the events the action received").isEmpty());
    }

    private List<String> stillRanWarnings() {
        synchronized (logged) {
            return new ArrayList<>(logged.list).stream()
                    .filter(event -> event.getLevel() == Level.WARN)
                    .map(ILoggingEvent::getFormattedMessage)
                    .filter(message -> message.contains("still ran for an event its catch-up replay delivered"))
                    .toList();
        }
    }

    // Grants the lease when a consumer registers, so the replay runs, and holds it for nobody once fenced, which the
    // history sets as the replay reads it. An event of the replay is then asked for the lease and waits. Counts down
    // when a thread other than the test's asks without the lease.
    private static final class NeverTheLeaseHolder implements CompetingConsumerStrategy {
        final CountDownLatch askedWithoutTheLease = new CountDownLatch(1);
        final CountDownLatch lookInFlight = new CountDownLatch(1);
        final CountDownLatch releaseTheLook = new CountDownLatch(1);
        final AtomicBoolean blockTheNextLook = new AtomicBoolean();
        volatile boolean fenced;
        private final Thread testThread = Thread.currentThread();

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
            if (fenced && Thread.currentThread() != testThread) {
                askedWithoutTheLease.countDown();
                if (blockTheNextLook.compareAndSet(true, false)) {
                    lookInFlight.countDown();
                    awaitTheRelease();
                    return true;
                }
            }
            return !fenced;
        }

        private void awaitTheRelease() {
            try {
                releaseTheLook.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
        }
    }

    // A live model that redelivers an event its action withdrew, which the catch-up model reports as its own
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
        private final Runnable whenTheReplayReadsIt;
        private final Runnable whenTheReplayClosesTheStream;

        private HistoryOfOneEvent(Runnable whenTheReplayReadsIt, Runnable whenTheReplayClosesTheStream) {
            this.whenTheReplayReadsIt = whenTheReplayReadsIt;
            this.whenTheReplayClosesTheStream = whenTheReplayClosesTheStream;
        }

        private static CloudEvent event() {
            return CloudEventBuilder.v1().withId("history1").withSource(URI.create("urn:test")).withType("Tested").build();
        }

        private Stream<CloudEvent> theEvent() {
            return Stream.of(event()).onClose(whenTheReplayClosesTheStream);
        }

        @Override
        public CatchupReadScope openCatchupReadScope(@Nullable String notBefore) {
            whenTheReplayReadsIt.run();
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
                    return theEvent();
                }

                @Override
                public void close() {
                }
            };
        }

        @Override
        public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
            return Stream.of(event());
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
