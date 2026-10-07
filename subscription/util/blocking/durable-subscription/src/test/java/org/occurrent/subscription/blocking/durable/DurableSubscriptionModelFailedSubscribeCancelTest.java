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

package org.occurrent.subscription.blocking.durable;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * For every subscribe of an id that started before {@code cancelSubscription(id)} returned, no checkpoint write
 * through that subscribe lands after the cancel returned, whether or not the wrapped model's subscribe threw. A wrapped
 * model can throw from its subscribe and still hold a run that calls the action it was given.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelFailedSubscribeCancelTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";

    @Test
    void a_checkpoint_written_by_a_run_the_wrapped_model_kept_after_its_subscribe_failed_does_not_land_after_cancel_subscription_returned() throws InterruptedException {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        RetainsTheRunOfAFailedSubscribe wrapped = new RetainsTheRunOfAFailedSubscribe();
        wrapped.globalCheckpoint = new StringBasedCheckpoint("0");
        wrapped.outcomes.add(Outcome.FAILS_WITH_A_RETAINED_RUN);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
        BlockingAction action = new BlockingAction(wrapped.actionStarted);

        Throwable failure = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, action));

        assertThat(failure).isInstanceOf(IllegalStateException.class).hasMessage("the subscribe fails");
        assertThat(storage.read(SUBSCRIPTION_ID)).as("the first position the evaluation recorded before the cancel").isNotNull()
                .extracting(Checkpoint::asString).isEqualTo("0");

        durable.cancelSubscription(SUBSCRIPTION_ID);
        releaseAndJoinTheRuns(action, wrapped);

        assertThat(storage.read(SUBSCRIPTION_ID)).as("the checkpoint stored after cancelSubscription returned").isNull();
    }

    @Test
    void checkpoints_written_by_the_runs_of_two_failed_subscribes_of_the_same_id_do_not_land_after_cancel_subscription_returned() throws InterruptedException {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        RetainsTheRunOfAFailedSubscribe wrapped = new RetainsTheRunOfAFailedSubscribe();
        wrapped.globalCheckpoint = new StringBasedCheckpoint("0");
        wrapped.outcomes.add(Outcome.FAILS_WITH_A_RETAINED_RUN);
        wrapped.outcomes.add(Outcome.FAILS_WITH_A_RETAINED_RUN);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
        BlockingAction action = new BlockingAction(wrapped.actionStarted);

        Throwable firstFailure = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, action));
        Throwable secondFailure = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, action));

        assertThat(firstFailure).as("the first subscribe").isInstanceOf(IllegalStateException.class).hasMessage("the subscribe fails");
        assertThat(secondFailure).as("the second subscribe").isInstanceOf(IllegalStateException.class).hasMessage("the subscribe fails");
        assertThat(wrapped.runs).as("the runs the wrapped model kept").hasSize(2);

        durable.cancelSubscription(SUBSCRIPTION_ID);
        releaseAndJoinTheRuns(action, wrapped);

        assertThat(storage.read(SUBSCRIPTION_ID)).as("the checkpoint stored after cancelSubscription returned").isNull();
    }

    /**
     * A subscribe that threw leaves the id free, so nothing but a cancel of the id stops what a run it left behind
     * writes. The run's action returns here before anything cancels the id.
     */
    @Test
    void a_run_the_wrapped_model_kept_after_its_subscribe_failed_still_stores_its_checkpoint_while_nothing_has_cancelled_the_id() throws InterruptedException {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        RetainsTheRunOfAFailedSubscribe wrapped = new RetainsTheRunOfAFailedSubscribe();
        wrapped.globalCheckpoint = new StringBasedCheckpoint("0");
        wrapped.outcomes.add(Outcome.FAILS_WITH_A_RETAINED_RUN);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
        BlockingAction action = new BlockingAction(wrapped.actionStarted);
        assertThat(catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, action))).isInstanceOf(IllegalStateException.class);

        releaseAndJoinTheRuns(action, wrapped);

        assertThat(storage.read(SUBSCRIPTION_ID)).as("the checkpoint stored by the run").isNotNull()
                .extracting(Checkpoint::asString).isEqualTo("after-e1");
    }

    /**
     * The first run's action is still blocked while the id is subscribed again, and returns once the checks are done.
     * What that run writes at that point is not asserted, since only the later subscribe's own delivery is.
     */
    @Test
    void subscribing_an_id_again_after_a_failed_subscribe_whose_run_the_wrapped_model_kept_is_accepted_and_checkpoints_its_events() throws InterruptedException {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        RetainsTheRunOfAFailedSubscribe wrapped = new RetainsTheRunOfAFailedSubscribe();
        wrapped.globalCheckpoint = new StringBasedCheckpoint("0");
        wrapped.outcomes.add(Outcome.FAILS_WITH_A_RETAINED_RUN);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
        BlockingAction blockedAction = new BlockingAction(wrapped.actionStarted);
        assertThat(catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, blockedAction))).isInstanceOf(IllegalStateException.class);
        durable.getWrappedSubscriptionModel().cancelSubscription(SUBSCRIPTION_ID);
        List<String> delivered = new ArrayList<>();

        try {
            assertThatCode(() -> durable.subscribe(SUBSCRIPTION_ID, cloudEvent -> delivered.add(cloudEvent.getId())))
                    .as("subscribing the id again").doesNotThrowAnyException();
            wrapped.deliver(SUBSCRIPTION_ID, new CheckpointAwareCloudEvent(cloudEvent("event-2"), new StringBasedCheckpoint("after-event-2")));

            assertThat(delivered).as("the events the second subscribe delivered").containsExactly("event-2");
            assertThat(storage.read(SUBSCRIPTION_ID)).as("the checkpoint stored for the second subscribe's event").isNotNull()
                    .extracting(Checkpoint::asString).isEqualTo("after-event-2");
        } finally {
            releaseAndJoinTheRuns(blockedAction, wrapped);
        }
    }

    /**
     * The wrapped model here keeps the start position its subscribe got and throws before evaluating it.
     */
    @Test
    void an_evaluation_after_a_refused_subscribe_stores_nothing_and_a_later_subscribe_of_the_id_stores_only_its_own_position() {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        RetainsTheRunOfAFailedSubscribe wrapped = new RetainsTheRunOfAFailedSubscribe();
        wrapped.globalCheckpoint = new StringBasedCheckpoint("0");
        wrapped.outcomes.add(Outcome.FAILS_BEFORE_EVALUATING);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
        assertThat(catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }))).isInstanceOf(IllegalStateException.class).hasMessage("the subscribe fails");
        StartAt keptByTheRefusedSubscribe = wrapped.keptStartAt;
        assertThat(keptByTheRefusedSubscribe).as("the start position the refused subscribe got").isNotNull();

        Throwable lateEvaluation = catchThrowable(() -> wrapped.evaluate(keptByTheRefusedSubscribe));

        assertThat(lateEvaluation).as("what evaluating the start position of the refused subscribe does")
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("failed before this evaluation got its start position");
        assertThat(storage.read(SUBSCRIPTION_ID)).as("the checkpoint stored by the late evaluation").isNull();

        wrapped.globalCheckpoint = new StringBasedCheckpoint("5");
        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        assertThat(storage.read(SUBSCRIPTION_ID)).as("the checkpoint stored by the second subscribe").isNotNull()
                .extracting(Checkpoint::asString).isEqualTo("5");
    }

    private static void releaseAndJoinTheRuns(BlockingAction action, RetainsTheRunOfAFailedSubscribe wrapped) throws InterruptedException {
        action.mayReturn.countDown();
        for (Thread run : wrapped.runs) {
            assertThat(run.join(Duration.ofSeconds(10))).as("whether a run the wrapped model kept ended").isTrue();
        }
        assertThat(wrapped.runFailures).as("what the runs the wrapped model kept threw").isEmpty();
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("test.event").build();
    }

    /**
     * An action that reports it started and then waits until it may return.
     */
    private static final class BlockingAction implements Consumer<CloudEvent> {
        final CountDownLatch mayReturn = new CountDownLatch(1);
        private final Semaphore started;

        private BlockingAction(Semaphore started) {
            this.started = started;
        }

        @Override
        public void accept(CloudEvent cloudEvent) {
            started.release();
            try {
                if (!mayReturn.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("The action was never allowed to return");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }

    private enum Outcome {
        /**
         * Keeps the start position and throws without evaluating it.
         */
        FAILS_BEFORE_EVALUATING,
        /**
         * Starts a run that evaluates the start position and calls the action with the checkpoint {@code after-e<n>} for
         * the n:th such run, throws once the action has started, and keeps the run.
         */
        FAILS_WITH_A_RETAINED_RUN
    }

    /**
     * Each subscribe takes the next outcome from {@code outcomes}, and accepts once none is left, which evaluates the
     * start position and holds the action. A cancel on this model drops the held action only, so a run it kept goes on.
     */
    private static final class RetainsTheRunOfAFailedSubscribe implements CheckpointAwareSubscriptionModel {
        final Queue<Outcome> outcomes = new ConcurrentLinkedQueue<>();
        final Semaphore actionStarted = new Semaphore(0);
        final List<Thread> runs = new CopyOnWriteArrayList<>();
        final Queue<Throwable> runFailures = new ConcurrentLinkedQueue<>();
        volatile @Nullable StartAt keptStartAt;
        volatile @Nullable Checkpoint globalCheckpoint;
        private final Map<String, Consumer<CloudEvent>> actions = new ConcurrentHashMap<>();
        private final AtomicInteger retainedRuns = new AtomicInteger();

        void deliver(String subscriptionId, CloudEvent cloudEvent) {
            actions.get(subscriptionId).accept(cloudEvent);
        }

        StartAt evaluate(StartAt startAt) {
            return startAt.get(new SubscriptionModelContext(RetainsTheRunOfAFailedSubscribe.class));
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            Outcome outcome = outcomes.poll();
            if (outcome == Outcome.FAILS_BEFORE_EVALUATING) {
                keptStartAt = startAt;
                throw new IllegalStateException("the subscribe fails");
            }
            if (outcome == Outcome.FAILS_WITH_A_RETAINED_RUN) {
                int run = retainedRuns.incrementAndGet();
                runs.add(Thread.ofPlatform().start(() -> {
                    try {
                        evaluate(startAt);
                        action.accept(new CheckpointAwareCloudEvent(cloudEvent("e" + run), new StringBasedCheckpoint("after-e" + run)));
                    } catch (Throwable t) {
                        runFailures.add(t);
                    }
                }));
                try {
                    if (!actionStarted.tryAcquire(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("The action of the run never started");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
                throw new IllegalStateException("the subscribe fails");
            }
            evaluate(startAt);
            actions.put(subscriptionId, action);
            return dummySubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            actions.remove(subscriptionId);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return globalCheckpoint;
        }

        @Override
        public void shutdown() {
            actions.clear();
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
            return actions.containsKey(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return dummySubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }

        private static Subscription dummySubscription(String subscriptionId) {
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
    }
}
