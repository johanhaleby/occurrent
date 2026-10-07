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
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.lang.ref.Reference;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * For every subscribe of an id that returned or threw before {@code cancelSubscription(id)} was called, no write of
 * that subscribe reaches the checkpoint storage after the cancel returned, whether or not the wrapped model's
 * subscribe threw. That covers the checkpoint of an event and a first position an evaluation of its start position
 * would record. A wrapped model can throw from its subscribe and still hold a run that calls the action it was given.
 * A later subscribe of the id also stops those writes, before the wrapped model gets it.
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
     * A subscribe that threw doesn't hold the id, so only a cancel or a later subscribe of the id stops what a run it
     * left behind writes. The run's action returns here before either happens.
     */
    @Test
    void a_run_the_wrapped_model_kept_after_its_subscribe_failed_still_stores_its_checkpoint_while_nothing_has_cancelled_or_subscribed_the_id_again() throws InterruptedException {
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
     * The first run's action is still blocked while the id is subscribed again, and returns once the second subscribe's
     * event is checkpointed, and must not replace that checkpoint then.
     */
    @Test
    void subscribing_an_id_again_after_a_failed_subscribe_whose_run_the_wrapped_model_kept_is_accepted_and_the_kept_run_does_not_overwrite_the_checkpoint_of_its_events() throws InterruptedException {
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

            releaseAndJoinTheRuns(blockedAction, wrapped);

            assertThat(storage.read(SUBSCRIPTION_ID)).as("the checkpoint stored after the first run's action returned").isNotNull()
                    .extracting(Checkpoint::asString).isEqualTo("after-event-2");
        } finally {
            blockedAction.mayReturn.countDown();
        }
    }

    /**
     * Nothing cancels the id between the two subscribes, so only the later subscribe of the id can stop what the first
     * run writes.
     */
    @Test
    void a_run_the_wrapped_model_kept_after_its_subscribe_failed_does_not_overwrite_the_checkpoint_of_a_later_subscribe_of_the_id() throws InterruptedException {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        RetainsTheRunOfAFailedSubscribe wrapped = new RetainsTheRunOfAFailedSubscribe();
        wrapped.globalCheckpoint = new StringBasedCheckpoint("0");
        wrapped.outcomes.add(Outcome.FAILS_WITH_A_RETAINED_RUN);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
        BlockingAction blockedAction = new BlockingAction(wrapped.actionStarted);
        assertThat(catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, blockedAction))).isInstanceOf(IllegalStateException.class);
        List<String> delivered = new ArrayList<>();

        try {
            assertThatCode(() -> durable.subscribe(SUBSCRIPTION_ID, cloudEvent -> delivered.add(cloudEvent.getId())))
                    .as("subscribing the id again").doesNotThrowAnyException();
            wrapped.deliver(SUBSCRIPTION_ID, new CheckpointAwareCloudEvent(cloudEvent("event-2"), new StringBasedCheckpoint("after-event-2")));

            assertThat(delivered).as("the events the second subscribe delivered").containsExactly("event-2");
            assertThat(storage.read(SUBSCRIPTION_ID)).as("the checkpoint stored for the second subscribe's event").isNotNull()
                    .extracting(Checkpoint::asString).isEqualTo("after-event-2");

            releaseAndJoinTheRuns(blockedAction, wrapped);

            assertThat(storage.read(SUBSCRIPTION_ID)).as("the checkpoint stored after the first run's action returned").isNotNull()
                    .extracting(Checkpoint::asString).isEqualTo("after-event-2");
        } finally {
            blockedAction.mayReturn.countDown();
        }
    }

    /**
     * The first run writes while the second subscribe is still inside the wrapped model, after the evaluation there
     * settled the second subscribe's first position and before that subscribe returned. A write at that point moves the
     * stored position past events the second subscribe has not delivered, so a restart would skip them.
     */
    @Test
    void a_run_the_wrapped_model_kept_after_its_subscribe_failed_does_not_overwrite_the_start_position_a_later_subscribe_settled_inside_its_wrapped_subscribe() throws InterruptedException {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        RetainsTheRunOfAFailedSubscribe wrapped = new RetainsTheRunOfAFailedSubscribe();
        wrapped.globalCheckpoint = new StringBasedCheckpoint("0");
        wrapped.outcomes.add(Outcome.FAILS_WITH_A_RETAINED_RUN);
        wrapped.outcomes.add(Outcome.EVALUATES_THEN_RUNS_A_HOOK_AND_ACCEPTS);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
        BlockingAction blockedAction = new BlockingAction(wrapped.actionStarted);
        assertThat(catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, blockedAction))).isInstanceOf(IllegalStateException.class);
        assertThat(wrapped.runs).as("the runs the wrapped model kept").hasSize(1);
        Thread leftoverRun = wrapped.runs.getFirst();
        AtomicBoolean leftoverRunEnded = new AtomicBoolean();
        wrapped.whileSubscribing = () -> {
            blockedAction.mayReturn.countDown();
            try {
                leftoverRunEnded.set(leftoverRun.join(Duration.ofSeconds(10)));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        };

        try {
            assertThatCode(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
            })).as("subscribing the id again").doesNotThrowAnyException();

            assertThat(leftoverRunEnded).as("whether the first run ended while the second subscribe was inside the wrapped model").isTrue();
            assertThat(wrapped.lastEvaluated).as("the start position the second subscribe's evaluation produced")
                    .isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class,
                            startAtCheckpoint -> assertThat(startAtCheckpoint.checkpoint.asString()).as("the checkpoint the second subscribe starts from").isEqualTo("0"));
            assertThat(storage.read(SUBSCRIPTION_ID)).as("the checkpoint stored once the second subscribe returned").isNotNull()
                    .extracting(Checkpoint::asString).isEqualTo("0");
        } finally {
            releaseAndJoinTheRuns(blockedAction, wrapped);
        }
    }

    /**
     * A start position that resolves to null opts the id out of checkpointing and hands the wrapped model the action
     * as it is, so the only checkpoint write left is the first run's.
     */
    @Test
    void a_run_the_wrapped_model_kept_after_its_subscribe_failed_does_not_write_a_checkpoint_once_a_later_subscribe_of_the_id_opted_out_of_checkpointing() throws InterruptedException {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        RetainsTheRunOfAFailedSubscribe wrapped = new RetainsTheRunOfAFailedSubscribe();
        wrapped.globalCheckpoint = new StringBasedCheckpoint("0");
        wrapped.outcomes.add(Outcome.FAILS_WITH_A_RETAINED_RUN);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
        BlockingAction blockedAction = new BlockingAction(wrapped.actionStarted);
        assertThat(catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, blockedAction))).isInstanceOf(IllegalStateException.class);
        assertThat(storage.read(SUBSCRIPTION_ID)).as("the first position the evaluation recorded before the opt-out").isNotNull()
                .extracting(Checkpoint::asString).isEqualTo("0");

        try {
            assertThatCode(() -> durable.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(context -> null), __ -> {
            })).as("subscribing the id again without checkpointing").doesNotThrowAnyException();

            releaseAndJoinTheRuns(blockedAction, wrapped);

            assertThat(storage.read(SUBSCRIPTION_ID)).as("the checkpoint stored after the first run's action returned").isNotNull()
                    .extracting(Checkpoint::asString).isEqualTo("0");
        } finally {
            blockedAction.mayReturn.countDown();
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

    /**
     * A cancel of an id nothing subscribed is how the model gets to drop what it kept for ids whose registrations were
     * collected.
     */
    @Test
    void subscribes_the_wrapped_model_refuses_leave_nothing_kept_for_their_ids_once_their_registrations_are_collected() {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        RetainsTheRunOfAFailedSubscribe wrapped = new RetainsTheRunOfAFailedSubscribe();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
        int refusals = 1000;
        for (int i = 0; i < refusals; i++) {
            wrapped.outcomes.add(Outcome.REFUSES_AS_A_DUPLICATE);
        }

        for (int i = 0; i < refusals; i++) {
            String id = "refused-" + i;
            Throwable refused = catchThrowable(() -> durable.subscribe(id, __ -> {
            }));
            assertThat(refused).as("the subscribe of " + id).isInstanceOf(DuplicateSubscriptionIdException.class);
        }

        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            System.gc();
            durable.cancelSubscription("an-id-never-subscribed");
            assertThat(durable.idsWithUntrackedRegistrations()).as("the ids the model still keeps registrations for").isZero();
        });
    }

    /**
     * The cancel removes the stored checkpoint, and the kept run then evaluates its start position again. No subscribe
     * of the id is waiting for the position that evaluation would store, so it must not be stored.
     */
    @Test
    void a_run_the_wrapped_model_kept_after_its_subscribe_failed_stores_no_start_position_when_it_evaluates_it_again_after_cancel_subscription_returned() throws InterruptedException {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        RetainsTheRunOfAFailedSubscribe wrapped = new RetainsTheRunOfAFailedSubscribe();
        wrapped.globalCheckpoint = new StringBasedCheckpoint("0");
        wrapped.outcomes.add(Outcome.FAILS_WITH_A_RETAINED_RUN);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
        BlockingAction action = new BlockingAction(wrapped.actionStarted);
        assertThat(catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, action))).isInstanceOf(IllegalStateException.class);
        StartAt kept = wrapped.keptStartAt;
        assertThat(kept).as("the start position the kept run got").isNotNull();

        try {
            durable.cancelSubscription(SUBSCRIPTION_ID);
            assertThat(storage.read(SUBSCRIPTION_ID)).as("precondition: the checkpoint stored after cancelSubscription returned").isNull();
            wrapped.globalCheckpoint = new StringBasedCheckpoint("7");

            Throwable reEvaluation = catchThrowable(() -> wrapped.evaluate(kept));

            assertThat(storage.read(SUBSCRIPTION_ID)).as("the checkpoint stored after the kept run evaluated its start position again").isNull();
            assertThat(reEvaluation).isInstanceOf(IllegalStateException.class).hasMessageContaining("before this evaluation recorded a first position");
        } finally {
            releaseAndJoinTheRuns(action, wrapped);
        }
    }

    /**
     * The later subscribe starts from a checkpoint of its own, so it stores nothing, and the kept run's evaluation
     * must not store a position either.
     */
    @Test
    void a_run_the_wrapped_model_kept_after_its_subscribe_failed_stores_no_start_position_when_it_evaluates_it_again_after_a_later_subscribe_from_a_checkpoint() throws InterruptedException {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        RetainsTheRunOfAFailedSubscribe wrapped = new RetainsTheRunOfAFailedSubscribe();
        wrapped.globalCheckpoint = new StringBasedCheckpoint("0");
        wrapped.outcomes.add(Outcome.FAILS_WITH_A_RETAINED_RUN);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
        BlockingAction action = new BlockingAction(wrapped.actionStarted);
        assertThat(catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, action))).isInstanceOf(IllegalStateException.class);
        StartAt kept = wrapped.keptStartAt;
        assertThat(kept).as("the start position the kept run got").isNotNull();

        try {
            durable.cancelSubscription(SUBSCRIPTION_ID);
            assertThatCode(() -> durable.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(new StringBasedCheckpoint("3")), __ -> {
            })).as("subscribing the id again from a checkpoint").doesNotThrowAnyException();
            assertThat(storage.read(SUBSCRIPTION_ID)).as("precondition: the checkpoint stored by the later subscribe").isNull();
            wrapped.globalCheckpoint = new StringBasedCheckpoint("7");

            Throwable reEvaluation = catchThrowable(() -> wrapped.evaluate(kept));

            assertThat(storage.read(SUBSCRIPTION_ID)).as("the checkpoint stored after the kept run evaluated its start position again").isNull();
            assertThat(reEvaluation).isInstanceOf(IllegalStateException.class).hasMessageContaining("before this evaluation recorded a first position");
        } finally {
            releaseAndJoinTheRuns(action, wrapped);
        }
    }

    /**
     * The checkpoint is deleted directly here, which stands for a checkpoint removed outside this model.
     */
    @Test
    void a_run_the_wrapped_model_kept_after_its_subscribe_failed_stores_its_start_position_when_it_evaluates_it_again_while_nothing_stopped_its_writes() throws InterruptedException {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        RetainsTheRunOfAFailedSubscribe wrapped = new RetainsTheRunOfAFailedSubscribe();
        wrapped.globalCheckpoint = new StringBasedCheckpoint("0");
        wrapped.outcomes.add(Outcome.FAILS_WITH_A_RETAINED_RUN);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
        BlockingAction action = new BlockingAction(wrapped.actionStarted);
        assertThat(catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, action))).isInstanceOf(IllegalStateException.class);
        StartAt kept = wrapped.keptStartAt;
        assertThat(kept).as("the start position the kept run got").isNotNull();

        try {
            storage.delete(SUBSCRIPTION_ID);
            wrapped.globalCheckpoint = new StringBasedCheckpoint("7");

            StartAt reEvaluation = wrapped.evaluate(kept);

            assertThat(storage.read(SUBSCRIPTION_ID)).as("the checkpoint stored after the kept run evaluated its start position again").isNotNull()
                    .extracting(Checkpoint::asString).isEqualTo("7");
            assertThat(reEvaluation).as("the start position the second evaluation produced")
                    .isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class,
                            startAtCheckpoint -> assertThat(startAtCheckpoint.checkpoint.asString()).as("the checkpoint the kept run starts from").isEqualTo("7"));
        } finally {
            releaseAndJoinTheRuns(action, wrapped);
        }
    }

    /**
     * A wrapped model may evaluate the start position and hold the action before it refuses the id as a duplicate, so
     * the checkpoint that action writes for an event is not stored after a cancel or a later subscribe of the id, as
     * for any other subscribe that failed.
     */
    @Test
    void a_checkpointing_action_the_wrapped_model_held_while_refusing_the_id_as_a_duplicate_stores_nothing_after_cancel_subscription_returned() {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        RetainsTheRunOfAFailedSubscribe wrapped = new RetainsTheRunOfAFailedSubscribe();
        wrapped.globalCheckpoint = new StringBasedCheckpoint("0");
        wrapped.outcomes.add(Outcome.HOLDS_THE_ACTION_AND_REFUSES_AS_A_DUPLICATE);
        wrapped.outcomes.add(Outcome.HOLDS_THE_ACTION_AND_REFUSES_AS_A_DUPLICATE);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
        assertThat(catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }))).as("the first subscribe").isInstanceOf(DuplicateSubscriptionIdException.class);
        assertThat(wrapped.heldByRefusedSubscribes).as("the actions the wrapped model held").hasSize(1);
        Consumer<CloudEvent> held = wrapped.heldByRefusedSubscribes.getFirst();

        held.accept(new CheckpointAwareCloudEvent(cloudEvent("event-1"), new StringBasedCheckpoint("5")));

        assertThat(storage.read(SUBSCRIPTION_ID)).as("the checkpoint stored before cancelSubscription").isNotNull()
                .extracting(Checkpoint::asString).isEqualTo("5");

        durable.cancelSubscription(SUBSCRIPTION_ID);
        held.accept(new CheckpointAwareCloudEvent(cloudEvent("event-2"), new StringBasedCheckpoint("6")));

        assertThat(storage.read(SUBSCRIPTION_ID)).as("the checkpoint stored after cancelSubscription returned").isNull();

        assertThat(catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }))).as("the second subscribe").isInstanceOf(DuplicateSubscriptionIdException.class);
        Checkpoint storedBeforeDelivery = storage.read(SUBSCRIPTION_ID);
        held.accept(new CheckpointAwareCloudEvent(cloudEvent("event-3"), new StringBasedCheckpoint("7")));

        assertThat(storage.read(SUBSCRIPTION_ID)).as("the checkpoint stored after the second subscribe").isEqualTo(storedBeforeDelivery);

        for (int i = 0; i < 3; i++) {
            System.gc();
            durable.cancelSubscription("an-id-never-subscribed");
            assertThat(durable.idsWithUntrackedRegistrations()).as("the ids the model keeps registrations for, after collection " + (i + 1)).isEqualTo(1);
        }
        Reference.reachabilityFence(held);
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
         * the n:th such run, throws once the action has started, and keeps the run and the start position it got in
         * {@code keptStartAt}.
         */
        FAILS_WITH_A_RETAINED_RUN,
        /**
         * Evaluates the start position and records it in {@code lastEvaluated}, then runs {@code whileSubscribing}, then
         * holds the action and returns.
         */
        EVALUATES_THEN_RUNS_A_HOOK_AND_ACCEPTS,
        /**
         * Throws a {@link DuplicateSubscriptionIdException} and keeps nothing, no start position, no action and no
         * thread.
         */
        REFUSES_AS_A_DUPLICATE,
        /**
         * Evaluates the start position, keeps the action in {@code heldByRefusedSubscribes}, and throws a
         * {@link DuplicateSubscriptionIdException}.
         */
        HOLDS_THE_ACTION_AND_REFUSES_AS_A_DUPLICATE
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
        final List<Consumer<CloudEvent>> heldByRefusedSubscribes = new CopyOnWriteArrayList<>();
        volatile @Nullable StartAt keptStartAt;
        volatile @Nullable StartAt lastEvaluated;
        volatile @Nullable Runnable whileSubscribing;
        volatile @Nullable Checkpoint globalCheckpoint;
        private final Map<String, Consumer<CloudEvent>> actions = new ConcurrentHashMap<>();
        private final AtomicInteger retainedRuns = new AtomicInteger();

        void deliver(String subscriptionId, CloudEvent cloudEvent) {
            actions.get(subscriptionId).accept(cloudEvent);
        }

        @Nullable StartAt evaluate(StartAt startAt) {
            return startAt.get(new SubscriptionModelContext(RetainsTheRunOfAFailedSubscribe.class));
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            Outcome outcome = outcomes.poll();
            if (outcome == Outcome.FAILS_BEFORE_EVALUATING) {
                keptStartAt = startAt;
                throw new IllegalStateException("the subscribe fails");
            }
            if (outcome == Outcome.REFUSES_AS_A_DUPLICATE) {
                throw new DuplicateSubscriptionIdException(subscriptionId);
            }
            if (outcome == Outcome.HOLDS_THE_ACTION_AND_REFUSES_AS_A_DUPLICATE) {
                evaluate(startAt);
                heldByRefusedSubscribes.add(action);
                throw new DuplicateSubscriptionIdException(subscriptionId);
            }
            if (outcome == Outcome.FAILS_WITH_A_RETAINED_RUN) {
                keptStartAt = startAt;
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
            if (outcome == Outcome.EVALUATES_THEN_RUNS_A_HOOK_AND_ACCEPTS) {
                lastEvaluated = evaluate(startAt);
                Runnable hook = whileSubscribing;
                if (hook != null) {
                    hook.run();
                }
            } else {
                evaluate(startAt);
            }
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
