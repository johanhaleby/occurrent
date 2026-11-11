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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.InstanceOfAssertFactories.THROWABLE;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A wrapped model or a storage can throw a checked exception without declaring it, which a Kotlin implementation does
 * with a plain {@code throw}. The clean-up after a failed first position record must run for it as for any other
 * failure.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelUndeclaredCheckedExceptionTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";

    @Test
    void cancels_the_subscription_in_the_wrapped_model_when_recording_the_first_position_throws_a_checked_exception_without_declaring_it() {
        IOException unreachable = new IOException("the position source is unreachable");
        HoldsTheStartPositionUnevaluated wrapped = new HoldsTheStartPositionUnevaluated();
        wrapped.failGlobalCheckpointWith = unreachable;
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage());

        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));

        assertAll(
                () -> assertThat(thrown).as("thrown").isSameAs(unreachable),
                () -> assertThat(wrapped.cancelled).as("subscriptions cancelled in the wrapped model").containsExactly(SUBSCRIPTION_ID),
                () -> assertThat(wrapped.isRunning(SUBSCRIPTION_ID)).as("held by the wrapped model").isFalse()
        );
    }

    @Test
    void keeps_the_checkpoint_on_a_later_cancel_when_the_cancel_after_a_failed_first_position_record_throws_a_checked_exception_without_declaring_it() {
        IllegalStateException recordFailure = new IllegalStateException("the storage refuses the first position");
        IOException cancelFailure = new IOException("the wrapped model lost its connection");
        FailsTheFirstSave storage = new FailsTheFirstSave(recordFailure);
        HoldsTheStartPositionUnevaluated wrapped = new HoldsTheStartPositionUnevaluated();
        wrapped.globalCheckpoint = new StringBasedCheckpoint("global");
        wrapped.failCancelWith = cancelFailure;
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);

        Throwable refusal = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));
        StringBasedCheckpoint writtenByTheHeldRun = new StringBasedCheckpoint("written-by-the-held-run");
        storage.save(SUBSCRIPTION_ID, writtenByTheHeldRun);
        wrapped.failCancelWith = null;
        durable.cancelSubscription(SUBSCRIPTION_ID);

        assertAll(
                () -> assertThat(refusal).as("refusal").isSameAs(recordFailure),
                () -> assertThat(refusal.getSuppressed()).as("suppressed").singleElement(THROWABLE)
                        .isInstanceOf(IllegalStateException.class).hasCause(cancelFailure),
                () -> assertThat(storage.deletes.get()).as("deletes of the stored checkpoint").isZero(),
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("stored checkpoint").isEqualTo(writtenByTheHeldRun)
        );
    }

    @Test
    void keeps_the_subscription_when_recording_the_first_position_throws_a_checked_exception_without_declaring_it_after_an_evaluation_stored_the_position() {
        IOException unreachable = new IOException("the position source is unreachable");
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        HoldsTheStartPositionUnevaluated wrapped = new HoldsTheStartPositionUnevaluated();
        wrapped.globalCheckpoint = new StringBasedCheckpoint("global");
        wrapped.failGlobalCheckpointWith = unreachable;
        wrapped.evaluateBeforeFailing = true;
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);

        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));

        assertAll(
                () -> assertThat(thrown).as("thrown").isNull(),
                () -> assertThat(wrapped.cancelled).as("subscriptions cancelled in the wrapped model").isEmpty(),
                () -> assertThat(wrapped.isRunning(SUBSCRIPTION_ID)).as("held by the wrapped model").isTrue(),
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("stored checkpoint").isEqualTo(new StringBasedCheckpoint("global"))
        );
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> RuntimeException sneaky(Throwable throwable) throws E {
        throw (E) throwable;
    }

    private static final class FailsTheFirstSave extends InMemoryCheckpointStorage {
        final AtomicInteger deletes = new AtomicInteger();
        private final AtomicInteger savesLeftToFail = new AtomicInteger(1);
        private final RuntimeException failure;

        FailsTheFirstSave(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            if (savesLeftToFail.getAndDecrement() > 0) {
                throw failure;
            }
            return super.save(subscriptionId, checkpoint, condition);
        }

        @Override
        public void delete(String subscriptionId) {
            deletes.incrementAndGet();
            super.delete(subscriptionId);
        }
    }

    /**
     * Holds the subscription without evaluating its start position, so the first position is recorded by the
     * subscribing thread once the subscribe has returned. Throws the failures it is given without declaring them, and
     * fails only the subscribing thread's {@code globalCheckpoint()}, so an evaluation on another thread gets the position.
     */
    private static final class HoldsTheStartPositionUnevaluated implements CheckpointAwareSubscriptionModel {
        final List<String> cancelled = new CopyOnWriteArrayList<>();
        private final List<String> held = new CopyOnWriteArrayList<>();
        volatile @Nullable Checkpoint globalCheckpoint;
        volatile @Nullable Throwable failGlobalCheckpointWith;
        volatile @Nullable Throwable failCancelWith;
        // Evaluates the start position on another thread, which stores it, before the subscribing thread's
        // globalCheckpoint() fails
        volatile boolean evaluateBeforeFailing;
        private volatile @Nullable StartAt startAt;
        private volatile @Nullable Thread subscribingThread;

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            this.startAt = startAt;
            subscribingThread = Thread.currentThread();
            held.add(subscriptionId);
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
        public void cancelSubscription(String subscriptionId) {
            cancelled.add(subscriptionId);
            Throwable failure = failCancelWith;
            if (failure != null) {
                throw sneaky(failure);
            }
            held.remove(subscriptionId);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            Throwable failure = failGlobalCheckpointWith;
            if (failure != null && Thread.currentThread() == subscribingThread) {
                if (evaluateBeforeFailing) {
                    evaluateOnAnotherThread();
                }
                throw sneaky(failure);
            }
            return globalCheckpoint;
        }

        private void evaluateOnAnotherThread() {
            StartAt toEvaluate = startAt;
            Thread evaluation = new Thread(() -> toEvaluate.get(new SubscriptionModelContext(HoldsTheStartPositionUnevaluated.class)));
            evaluation.start();
            try {
                evaluation.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void shutdown() {
            held.clear();
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
            return held.contains(subscriptionId);
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
    }
}
