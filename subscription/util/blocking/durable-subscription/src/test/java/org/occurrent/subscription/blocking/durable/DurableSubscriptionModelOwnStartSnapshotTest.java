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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.HistoryLossReportingSubscriptions;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * What a plain {@link DurableSubscriptionModel#resumeSubscription(String)} of a subscription subscribed at a
 * {@code StartAt.checkpoint(..)} of its own does, decided by what is stored for the id when it was subscribed, and
 * what {@link DurableSubscriptionModel#shutdown()} leaves a subscription that is still running able to store. The
 * wrapped model is a stub, so the order of what is stored, read and resumed is exact.
 */
@DisplayName("DurableSubscriptionModelOwnStartSnapshot")
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelOwnStartSnapshotTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";
    private static final StartAt X = StartAt.checkpoint(new StringBasedCheckpoint("x"));
    private static final DurableSubscriptionModelConfig EVERY_EVENT = new DurableSubscriptionModelConfig(1);

    @Test
    void an_Error_from_the_storage_read_of_what_is_stored_escapes_from_a_subscribe_at_a_checkpoint() {
        // Given a storage whose read throws an Error
        Error crash = new Error("the storage crashed");
        ProbingStorage storage = new ProbingStorage();
        storage.readFailure = crash;
        DurableSubscriptionModel durable = new DurableSubscriptionModel(new RepositionableStub(), storage, EVERY_EVENT);

        // When
        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, null, X, cloudEvent -> {
        }));

        // Then
        assertThat(thrown).isSameAs(crash);
    }

    @Test
    void an_Error_from_the_storage_write_version_of_what_is_stored_escapes_from_a_subscribe_at_a_checkpoint() {
        // Given a storage whose write version throws an Error
        Error crash = new Error("the storage crashed");
        ProbingStorage storage = new ProbingStorage();
        storage.writeVersionFailure = crash;
        DurableSubscriptionModel durable = new DurableSubscriptionModel(new RepositionableStub(), storage, EVERY_EVENT);

        // When
        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, null, X, cloudEvent -> {
        }));

        // Then
        assertThat(thrown).isSameAs(crash);
    }

    @Test
    void a_subscribe_at_a_checkpoint_whose_snapshot_read_threw_an_Error_leaves_no_delivery_running_and_no_registration_behind() {
        // Given a subscribe at a checkpoint whose snapshot read throws an Error after the wrapped model accepted the subscription
        ProbingStorage storage = new ProbingStorage();
        Error crash = new Error("the storage crashed");
        storage.readFailure = crash;

        // When, then
        assertNothingIsLeftBehindBy(storage, crash);
    }

    @Test
    void a_subscribe_at_a_checkpoint_whose_snapshot_write_version_read_threw_an_Error_leaves_no_delivery_running_and_no_registration_behind() {
        // Given a subscribe at a checkpoint whose snapshot write version read throws an Error after the wrapped model accepted the subscription
        ProbingStorage storage = new ProbingStorage();
        Error crash = new Error("the storage crashed");
        storage.writeVersionFailure = crash;

        // When, then
        assertNothingIsLeftBehindBy(storage, crash);
    }

    @Test
    void a_plain_resume_goes_on_from_the_wrapped_models_own_position_when_the_wrapped_cancel_failed_after_an_Error_escaped_from_the_snapshot_read_of_a_subscribe_at_a_checkpoint() {
        // Given a checkpoint an earlier run stored, and a wrapped model whose cancel fails
        ProbingStorage storage = new ProbingStorage();
        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stored-by-an-earlier-run"));
        Error crash = new Error("the storage crashed");
        storage.readFailure = crash;
        RepositionableStub wrapped = new RepositionableStub();
        IllegalStateException cancelFailure = new IllegalStateException("cannot cancel");
        wrapped.cancelFailure = cancelFailure;
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage, EVERY_EVENT);

        // When a subscribe at a checkpoint has its snapshot read throw an Error, and the wrapped model still holds the subscription
        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, null, X, cloudEvent -> {
        }));
        storage.readFailure = null;
        wrapped.cancelFailure = null;

        // Then the Error escapes with the cancel failure as the cause of a suppressed IllegalStateException
        assertThat(thrown).isSameAs(crash);
        assertThat(thrown.getSuppressed()).as("suppressed by the Error").singleElement().isInstanceOfSatisfying(IllegalStateException.class,
                suppressed -> assertThat(suppressed).hasCause(cancelFailure));

        // And a pause and a plain resume of the subscription the wrapped model holds go on from the wrapped model's own position
        durable.pauseSubscription(SUBSCRIPTION_ID);
        durable.resumeSubscription(SUBSCRIPTION_ID);
        assertThat(wrapped.plainResumes.get()).as("plain resumes of the wrapped model").isEqualTo(1);
        assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was repositioned to").isEmpty();
    }

    @Test
    void a_RuntimeException_from_the_storage_write_version_of_what_is_stored_does_not_fail_a_subscribe_at_a_checkpoint() {
        // Given a storage whose write version throws a RuntimeException
        ProbingStorage storage = new ProbingStorage();
        storage.writeVersionFailure = new IllegalStateException("cannot read the write version");
        DurableSubscriptionModel durable = new DurableSubscriptionModel(new RepositionableStub(), storage, EVERY_EVENT);

        // When
        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, null, X, cloudEvent -> {
        }));

        // Then
        assertThat(thrown).isNull();
    }

    @Test
    void a_plain_resume_goes_to_a_checkpoint_stored_by_another_node_after_the_snapshot_failed_and_the_subscription_stored_one_of_its_own() {
        // Given a subscribe at a checkpoint whose snapshot read failed, so it can't tell what was stored before
        ProbingStorage storage = new ProbingStorage();
        storage.readFailure = new IllegalStateException("cannot read the checkpoint");
        RepositionableStub wrapped = new RepositionableStub();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage, EVERY_EVENT);
        durable.subscribe(SUBSCRIPTION_ID, null, X, cloudEvent -> {
        });
        storage.readFailure = null;

        // And it stores the checkpoint of an event, and then another node stores a later one
        wrapped.deliver(0, event("e1", "after-e1"));
        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stored-by-another-node"));

        // When it is paused and resumed with a plain resume
        durable.pauseSubscription(SUBSCRIPTION_ID);
        durable.resumeSubscription(SUBSCRIPTION_ID);

        // Then it resumes at the checkpoint the other node stored
        assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was repositioned to").containsExactly("stored-by-another-node");
        assertThat(wrapped.plainResumes.get()).as("plain resumes of the wrapped model").isZero();
    }

    @Test
    void a_plain_resume_goes_on_from_the_wrapped_models_own_position_when_the_snapshot_failed_and_the_subscription_stored_nothing() {
        // Given a subscribe at a checkpoint whose snapshot read failed, and a checkpoint that is stored for the id by an earlier run
        ProbingStorage storage = new ProbingStorage();
        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stored-by-an-earlier-run"));
        storage.readFailure = new IllegalStateException("cannot read the checkpoint");
        RepositionableStub wrapped = new RepositionableStub();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage, EVERY_EVENT);
        durable.subscribe(SUBSCRIPTION_ID, null, X, cloudEvent -> {
        });
        storage.readFailure = null;

        // When it is paused and resumed with a plain resume
        durable.pauseSubscription(SUBSCRIPTION_ID);
        durable.resumeSubscription(SUBSCRIPTION_ID);

        // Then it goes on from the wrapped model's own position, which can deliver an event again but never skips one
        assertThat(wrapped.plainResumes.get()).as("plain resumes of the wrapped model").isEqualTo(1);
        assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was repositioned to").isEmpty();
    }

    @Test
    void a_plain_resume_goes_to_the_stored_checkpoint_when_the_same_checkpoint_is_stored_again_with_a_newer_write_version_after_the_subscribe() {
        // Given a checkpoint s0 an earlier run stored, with write version 1, and a subscribe at a checkpoint before it
        ProbingStorage storage = new ProbingStorage();
        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("s0"), CheckpointWriteCondition.notOlderThan(1));
        RepositionableStub wrapped = new RepositionableStub();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage, EVERY_EVENT);
        durable.subscribe(SUBSCRIPTION_ID, null, X, cloudEvent -> {
        });

        // And s0 is stored again from outside, with write version 2
        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("s0"), CheckpointWriteCondition.notOlderThan(2));

        // When it is paused and resumed with a plain resume
        durable.pauseSubscription(SUBSCRIPTION_ID);
        durable.resumeSubscription(SUBSCRIPTION_ID);

        // Then it resumes at s0
        assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was repositioned to").containsExactly("s0");
        assertThat(wrapped.plainResumes.get()).as("plain resumes of the wrapped model").isZero();
    }

    @Test
    void a_plain_resume_goes_on_from_the_wrapped_models_own_position_when_the_checkpoint_and_the_write_version_stored_are_the_ones_read_at_subscribe() {
        // Given a checkpoint s0 an earlier run stored, with write version 1, and a subscribe at a checkpoint before it
        ProbingStorage storage = new ProbingStorage();
        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("s0"), CheckpointWriteCondition.notOlderThan(1));
        RepositionableStub wrapped = new RepositionableStub();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage, EVERY_EVENT);
        durable.subscribe(SUBSCRIPTION_ID, null, X, cloudEvent -> {
        });

        // When it is paused and resumed with a plain resume
        durable.pauseSubscription(SUBSCRIPTION_ID);
        durable.resumeSubscription(SUBSCRIPTION_ID);

        // Then it goes on from the wrapped model's own position, since s0 can lie after events it has not delivered
        assertThat(wrapped.plainResumes.get()).as("plain resumes of the wrapped model").isEqualTo(1);
        assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was repositioned to").isEmpty();
    }

    @Test
    void a_checkpoint_stored_by_a_registration_a_failed_cancel_kept_while_the_next_subscribe_is_inside_the_wrapped_model_is_not_taken_for_one_stored_by_another_node() {
        // Given a subscribe of the id whose cancel failed, so it is still tracked and its action still stores checkpoints
        ProbingStorage storage = new ProbingStorage();
        RepositionableStub wrapped = new RepositionableStub();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage, EVERY_EVENT);
        durable.subscribe(SUBSCRIPTION_ID, null, X, cloudEvent -> {
        });
        wrapped.cancelFailure = new IllegalStateException("cannot cancel");
        assertThat(catchThrowable(() -> durable.cancelSubscription(SUBSCRIPTION_ID))).isSameAs(wrapped.cancelFailure);
        wrapped.cancelFailure = null;

        // And that action stores the checkpoint z while the next subscribe of the id, at a checkpoint before z, is inside the wrapped model
        wrapped.whileSubscribing = () -> wrapped.deliver(0, event("e1", "z"));

        // When the next subscribe returns and the subscription is paused and resumed with a plain resume
        durable.subscribe(SUBSCRIPTION_ID, null, X, cloudEvent -> {
        });
        assertThat(storage.read(SUBSCRIPTION_ID)).as("stored by the kept registration").isNotNull().extracting(Checkpoint::asString).isEqualTo("z");
        durable.pauseSubscription(SUBSCRIPTION_ID);
        durable.resumeSubscription(SUBSCRIPTION_ID);

        // Then it goes on from its own position, since z was stored by the run that ended before this subscribe returned
        assertThat(wrapped.plainResumes.get()).as("plain resumes of the wrapped model").isEqualTo(1);
        assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was repositioned to").isEmpty();
    }

    @Test
    void over_a_wrapped_model_that_cannot_reposition_a_subscribe_at_a_checkpoint_reads_nothing_from_the_storage() {
        // Given a wrapped model that is not repositionable
        ProbingStorage storage = new ProbingStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(new NotRepositionableStub(), storage, EVERY_EVENT);

        // When
        durable.subscribe(SUBSCRIPTION_ID, null, X, cloudEvent -> {
        });

        // Then
        assertThat(storage.reads.get()).as("reads of the checkpoint").isZero();
        assertThat(storage.writeVersionReads.get()).as("reads of the write version").isZero();
    }

    @Test
    void an_action_of_a_subscription_that_is_still_running_when_shutdown_returns_stores_no_checkpoint_after_it() throws Exception {
        // Given a subscription whose action is running, and is not waited for by the wrapped model's shutdown
        ProbingStorage storage = new ProbingStorage();
        RepositionableStub wrapped = new RepositionableStub();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage, EVERY_EVENT);
        RunningAction running = new RunningAction();
        durable.subscribe(SUBSCRIPTION_ID, null, X, running);
        Thread run = running.startDelivering(() -> wrapped.deliver(0, event("e1", "after-e1")));

        // When shutdown returns and the action then returns
        durable.shutdown();
        running.mayReturn.countDown();
        run.join(Duration.ofSeconds(10));

        // Then it stored no checkpoint
        assertThat(run.isAlive()).as("whether the run is still alive").isFalse();
        assertThat(running.failure.get()).as("what the run threw").isNull();
        assertThat(storage.exists(SUBSCRIPTION_ID)).as("whether a checkpoint is stored").isFalse();
    }

    @Test
    void an_action_of_a_subscribe_the_wrapped_model_kept_after_it_failed_stores_no_checkpoint_after_shutdown_returns() throws Exception {
        // Given a subscribe the wrapped model failed after it kept the action, whose run is inside the action
        ProbingStorage storage = new ProbingStorage();
        RepositionableStub wrapped = new RepositionableStub();
        wrapped.failNextSubscribe = true;
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage, EVERY_EVENT);
        RunningAction running = new RunningAction();
        assertThatCode(() -> durable.subscribe(SUBSCRIPTION_ID, null, X, running)).isInstanceOf(IllegalStateException.class);
        Thread run = running.startDelivering(() -> wrapped.deliver(0, event("e1", "after-e1")));

        // When shutdown returns and the action then returns
        durable.shutdown();
        running.mayReturn.countDown();
        run.join(Duration.ofSeconds(10));

        // Then it stored no checkpoint
        assertThat(run.isAlive()).as("whether the run is still alive").isFalse();
        assertThat(running.failure.get()).as("what the run threw").isNull();
        assertThat(storage.exists(SUBSCRIPTION_ID)).as("whether a checkpoint is stored").isFalse();
    }

    @Test
    void an_action_of_a_subscribe_whose_wrapped_subscribe_called_shutdown_stores_no_checkpoint() {
        // Given a wrapped model whose subscribe calls shutdown on the durable model, on the subscribing thread
        ProbingStorage storage = new ProbingStorage();
        RepositionableStub wrapped = new RepositionableStub();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage, EVERY_EVENT);
        wrapped.whileSubscribing = durable::shutdown;

        // When the subscribe returns and the action is called with an event
        durable.subscribe(SUBSCRIPTION_ID, null, X, cloudEvent -> {
        });
        wrapped.deliver(0, event("e1", "after-e1"));

        // Then no checkpoint is stored
        assertThat(storage.exists(SUBSCRIPTION_ID)).as("whether a checkpoint is stored").isFalse();
    }

    private static void assertNothingIsLeftBehindBy(ProbingStorage storage, Throwable crash) {
        HistoryLossReportingStub wrapped = new HistoryLossReportingStub();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage, EVERY_EVENT);

        // When the subscribe fails with the Error, and the storage then works
        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, null, X, cloudEvent -> {
        }));
        storage.readFailure = null;
        storage.writeVersionFailure = null;

        // Then the Error escapes, and the wrapped model's subscription was cancelled
        assertThat(thrown).isSameAs(crash);
        assertThat(wrapped.cancelAttempts).as("ids cancelled in the wrapped model").containsExactly(SUBSCRIPTION_ID);

        // And the id is not tracked
        assertThat(wrapped.listeners).hasSize(1);
        assertThat(wrapped.listeners.getFirst().storesRestartPositionOf(SUBSCRIPTION_ID)).as("whether a restart position is stored for the id").isFalse();
    }

    private static CloudEvent event(String id, String checkpoint) {
        CloudEvent cloudEvent = CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("test.event").build();
        return new CheckpointAwareCloudEvent(cloudEvent, new StringBasedCheckpoint(checkpoint));
    }

    /**
     * An action that reports that it started and then waits until it may return.
     */
    private static final class RunningAction implements Consumer<CloudEvent> {
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch mayReturn = new CountDownLatch(1);
        final AtomicReference<@Nullable Throwable> failure = new AtomicReference<>();

        @Override
        public void accept(CloudEvent cloudEvent) {
            started.countDown();
            try {
                if (!mayReturn.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("The action was never allowed to return");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }

        Thread startDelivering(Runnable delivery) throws InterruptedException {
            Thread run = Thread.ofPlatform().start(() -> {
                try {
                    delivery.run();
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            assertThat(started.await(10, TimeUnit.SECONDS)).as("whether the action started").isTrue();
            return run;
        }
    }

    /**
     * An in-memory storage that counts what is read and fails what it is told to fail.
     */
    private static final class ProbingStorage extends InMemoryCheckpointStorage {
        final AtomicInteger reads = new AtomicInteger();
        final AtomicInteger writeVersionReads = new AtomicInteger();
        volatile @Nullable Throwable readFailure;
        volatile @Nullable Throwable writeVersionFailure;

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            reads.incrementAndGet();
            throwIfSet(readFailure);
            return super.read(subscriptionId);
        }

        @Override
        public OptionalLong writeVersion(String subscriptionId) {
            writeVersionReads.incrementAndGet();
            throwIfSet(writeVersionFailure);
            return super.writeVersion(subscriptionId);
        }

        private static void throwIfSet(@Nullable Throwable failure) {
            if (failure instanceof Error error) {
                throw error;
            }
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
        }
    }

    /**
     * A wrapped model that is not repositionable, keeps the action of every subscribe and counts its plain resumes.
     */
    private static class NotRepositionableStub implements CheckpointAwareSubscriptionModel {
        final List<Consumer<CloudEvent>> actions = new CopyOnWriteArrayList<>();
        final List<String> resumedAt = new ArrayList<>();
        final AtomicInteger plainResumes = new AtomicInteger();
        volatile boolean failNextSubscribe;
        volatile @Nullable Runnable whileSubscribing;
        final List<String> cancelAttempts = new CopyOnWriteArrayList<>();
        volatile @Nullable RuntimeException cancelFailure;

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            actions.add(action);
            Runnable hook = whileSubscribing;
            if (hook != null) {
                hook.run();
            }
            if (failNextSubscribe) {
                failNextSubscribe = false;
                throw new IllegalStateException("the subscribe fails");
            }
            return dummySubscription(subscriptionId);
        }

        void deliver(int subscribe, CloudEvent cloudEvent) {
            actions.get(subscribe).accept(cloudEvent);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return new StringBasedCheckpoint("global");
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
            return true;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            plainResumes.incrementAndGet();
            return dummySubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            cancelAttempts.add(subscriptionId);
            RuntimeException failure = cancelFailure;
            if (failure != null) {
                throw failure;
            }
        }

        static Subscription dummySubscription(String subscriptionId) {
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

    private static class RepositionableStub extends NotRepositionableStub implements RepositionableSubscriptions {
        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            resumedAt.add(((StartAt.StartAtCheckpoint) startAt).checkpoint.asString());
            return dummySubscription(subscriptionId);
        }
    }

    private static final class HistoryLossReportingStub extends RepositionableStub implements HistoryLossReportingSubscriptions {
        final List<HistoryLossListener> listeners = new CopyOnWriteArrayList<>();

        @Override
        public void addHistoryLossListener(HistoryLossListener listener) {
            listeners.add(listener);
        }

        @Override
        public void removeHistoryLossListener(HistoryLossListener listener) {
            listeners.remove(listener);
        }
    }
}
