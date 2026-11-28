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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
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
import org.occurrent.subscription.api.blocking.RedeliveringSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.occurrent.subscription.blocking.durable.catchup.StartAtTime;
import org.occurrent.subscription.blocking.durable.catchup.StreamCatchupSubscriptionModel;
import org.occurrent.subscription.internal.UnwantedDelivery;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The live delegate of a stopped catch-up model throws from its start, after a subscribe made while the start was
 * inside the delegate began a replay, and the action of that replay waits for the lease of a competing consumer. The
 * competing consumer model does not poll the catch-up model, so the only thing that ends that wait is the stage
 * {@link UnwantedDelivery} hands the action, which the catch-up model completes once it parks the replay. The failed
 * start stops the catch-up model again and parks that replay, which has to complete the stage as a stop does, and a
 * later start has to relaunch the replay without waiting for the action of the attempt that was parked.
 * <p>
 * The action here reads the stage as the competing consumer's wait for the lease does, instead of being made by a
 * {@link CompetingConsumerSubscriptionModel}, since that model's own subscribe resumes the subscription it made, and a
 * resume waits for a start under way, so no competing subscribe can begin a replay while the delegate starts.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class CatchupWhoseStartFailsParksAReplayWhoseActionWaitsForTheLeaseTest {

    private static final String ID = "subscription";
    private static final Duration BOUND = Duration.ofSeconds(5);

    enum Catchup {
        DISPATCHER, STREAM
    }

    private final List<String> received = new CopyOnWriteArrayList<>();
    private final List<String> withdrawn = new CopyOnWriteArrayList<>();
    private final AtomicInteger callsOfTheActionWaitingForTheLease = new AtomicInteger();
    private final CountDownLatch theActionHasExited = new CountDownLatch(1);
    // Streams closed by replays, the first being the one of the subscription made to give a dispatcher a catch-up model to stop
    private final AtomicInteger replaysClosed = new AtomicInteger();
    private final CompletableFuture<Void> theLeaseIsGranted = new CompletableFuture<>();
    private final FailingStartLiveModel live = new FailingStartLiveModel();
    private final ExecutorService callers = Executors.newCachedThreadPool();
    private SubscriptionModel catchup;

    @AfterEach
    void shutdown() {
        // Lets an action that still waits for the lease return, and the delegate's start throw, before shutting down
        theLeaseIsGranted.complete(null);
        live.releaseTheStart.countDown();
        if (catchup != null) {
            catchup.shutdown();
        }
        callers.shutdownNow();
    }

    @ParameterizedTest
    @EnumSource(Catchup.class)
    void a_start_that_fails_in_the_live_delegate_ends_the_wait_for_the_lease_of_a_replay_event_that_the_start_parked(Catchup kind) throws InterruptedException {
        // Given
        Future<?> start = aStartWhoseLiveDelegateBlocks(kind);
        aSubscribeWhoseReplayEventWaitsForTheLease(kind);

        // When
        live.releaseTheStart.countDown();

        // Then
        assertThat(failureOf(start)).as("what the start threw").isSameAs(live.startFailure);
        assertThat(catchup.isRunning()).as("the catch-up model runs after the start that failed").isFalse();
        assertThat(theActionHasExited.await(BOUND.toSeconds(), TimeUnit.SECONDS)).as("the action has left its wait for the lease within %s of the failed start", BOUND).isTrue();
        assertThat(received).as("the events the action received").isEmpty();
        assertThat(withdrawn).as("the events the action withdrew as unwanted").containsExactly("history1");
    }

    @ParameterizedTest
    @EnumSource(Catchup.class)
    void a_start_after_a_failed_start_relaunches_the_parked_replay_without_waiting_for_the_action_of_the_parked_attempt_and_delivers_its_event(Catchup kind) {
        // Given a start that failed while the replay event waited for the lease
        Future<?> failedStart = aStartWhoseLiveDelegateBlocks(kind);
        aSubscribeWhoseReplayEventWaitsForTheLease(kind);
        live.releaseTheStart.countDown();
        assertThat(failureOf(failedStart)).as("what the first start threw").isSameAs(live.startFailure);
        live.failTheStart = false;

        // When the live delegate starts normally
        Future<?> secondStart = callers.submit(() -> catchup.start(true));

        // Then
        assertThat(secondStart).as("the second start").succeedsWithin(BOUND);
        await().atMost(BOUND).untilAsserted(() -> assertThat(callsOfTheActionWaitingForTheLease).as("the calls of the action that wait for the lease, the relaunched replay being the second").hasValue(2));
        theLeaseIsGranted.complete(null);
        await().atMost(BOUND).untilAsserted(() -> assertThat(received).as("the events the action received once the lease is granted").containsExactly("history1"));
    }

    private Future<?> aStartWhoseLiveDelegateBlocks(Catchup kind) {
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100);
        HistoryOfOneEvent history = new HistoryOfOneEvent(() -> {
            if (replaysClosed.incrementAndGet() > 1) {
                theActionHasExited.countDown();
            }
        });
        catchup = kind == Catchup.DISPATCHER ? new CatchupSubscriptionModel(live, history, config) : new StreamCatchupSubscriptionModel(live, history, config);
        // A catch-up model that was started and stopped again, so its next start is one that a failure takes back. A dispatcher
        // has a catch-up model to stop only once something was subscribed, hence the subscription whose replay completes first
        catchup.start(true);
        catchup.subscribe("warmup", StartAtTime.beginningOfTime(), event -> { });
        await().atMost(BOUND).until(() -> replaysClosed.get() == 1);
        catchup.stop();
        live.failTheStart = true;
        Future<?> start = callers.submit(() -> catchup.start(true));
        await().atMost(BOUND).until(() -> live.startEntered.getCount() == 0);
        return start;
    }

    // The latch is counted down by the action itself once it has read its stage and is about to wait on it and on the
    // lease, so the replay is in the wait before the live delegate is released
    private void aSubscribeWhoseReplayEventWaitsForTheLease(Catchup kind) {
        Future<?> subscribe = callers.submit(() -> catchup.subscribe(ID, StartAtTime.beginningOfTime(), this::waitForTheLeaseUnlessUnwanted));
        assertThat(subscribe).as("the subscribe made while the start is inside the live delegate").succeedsWithin(BOUND);
        await().atMost(BOUND).until(() -> callsOfTheActionWaitingForTheLease.get() == 1);
    }

    private void waitForTheLeaseUnlessUnwanted(CloudEvent event) {
        CompletionStage<Void> unwanted = UnwantedDelivery.ofTheCallOnThisThread(ID, model -> model == live);
        if (unwanted == null) {
            throw new AssertionError("the replay called the action without a stage for an unwanted delivery");
        }
        callsOfTheActionWaitingForTheLease.incrementAndGet();
        try {
            CompletableFuture.anyOf(unwanted.toCompletableFuture(), theLeaseIsGranted).get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } catch (ExecutionException | TimeoutException e) {
            throw new AssertionError("the action waited for the lease for good", e);
        }
        if (unwanted.toCompletableFuture().isDone()) {
            withdrawn.add(event.getId());
        } else {
            received.add(event.getId());
        }
    }

    private static @Nullable Throwable failureOf(Future<?> call) {
        try {
            call.get(BOUND.toSeconds(), TimeUnit.SECONDS);
            return null;
        } catch (ExecutionException e) {
            return e.getCause();
        } catch (InterruptedException | TimeoutException e) {
            throw new AssertionError("the start did not return within " + BOUND, e);
        }
    }

    // A live model whose first start reports that it runs while it waits for the release, and then throws, after which
    // it reports that it does not run, and whose later starts return
    private static final class FailingStartLiveModel implements CheckpointAwareSubscriptionModel, RedeliveringSubscriptions {
        final CountDownLatch startEntered = new CountDownLatch(1);
        final CountDownLatch releaseTheStart = new CountDownLatch(1);
        final IllegalStateException startFailure = new IllegalStateException("the live delegate failed to start");
        volatile boolean failTheStart;
        private volatile boolean running;

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
            running = false;
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            if (!failTheStart) {
                running = true;
                return;
            }
            running = true;
            startEntered.countDown();
            try {
                if (!releaseTheStart.await(30, TimeUnit.SECONDS)) {
                    throw new AssertionError("the test never released the start of the live delegate");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                running = false;
            }
            throw startFailure;
        }

        @Override
        public boolean isRunning() {
            return running;
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
        private final Runnable whenTheReplayClosesTheStream;

        private HistoryOfOneEvent(Runnable whenTheReplayClosesTheStream) {
            this.whenTheReplayClosesTheStream = whenTheReplayClosesTheStream;
        }

        private Stream<CloudEvent> theEvent() {
            CloudEvent event = CloudEventBuilder.v1().withId("history1").withSource(URI.create("urn:test")).withType("Tested").build();
            return Stream.of(event).onClose(whenTheReplayClosesTheStream);
        }

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
                    throw new UnsupportedOperationException();
                }

                @Override
                public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
                    return skip == 0 ? theEvent() : Stream.empty();
                }

                @Override
                public void close() {
                }
            };
        }

        @Override
        public long count(Filter filter) {
            return 1;
        }

        @Override
        public boolean exists(Filter filter) {
            return true;
        }

        @Override
        public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
            return skip == 0 ? theEvent() : Stream.empty();
        }
    }
}
