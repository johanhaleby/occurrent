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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A wrapped model's {@code start(..)} can return only once its own {@code shutdown()} is called, as the MongoDB
 * change-stream model's does while it waits for its stream to start. A {@code shutdown()} of the catch-up model that
 * comes meanwhile ends that start instead of waiting for it.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupShutdownEndsAStartStuckInTheWrappedModelTest {

    private InMemorySubscriptionModel inMemory;
    private StuckUntilShutdownSubscriptionModel wrapped;
    private StreamCatchupSubscriptionModel model;
    private final List<CallOnAThread> calls = new ArrayList<>();

    @BeforeEach
    void create_a_model_over_a_wrapped_model_whose_start_returns_only_once_it_is_shut_down() {
        inMemory = new InMemorySubscriptionModel();
        wrapped = new StuckUntilShutdownSubscriptionModel(inMemory);
        model = new StreamCatchupSubscriptionModel(wrapped, new InMemoryEventStore(inMemory).withoutStreamPosition(), new CatchupSubscriptionModelConfig(100));
    }

    @AfterEach
    void release_everything_and_shut_down() {
        try {
            wrapped.release();
            calls.forEach(CallOnAThread::awaitEnded);
        } finally {
            model.shutdown();
            inMemory.shutdown();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @Timeout(40)
    void a_shutdown_that_comes_while_a_start_is_stuck_in_the_wrapped_start_returns_and_ends_the_start(boolean resumeSubscriptionsAutomatically) throws InterruptedException {
        // Given
        model.stop();
        CountDownLatch startReturned = new CountDownLatch(1);
        calls.add(new CallOnAThread(() -> {
            model.start(resumeSubscriptionsAutomatically);
            startReturned.countDown();
        }));
        assertThat(wrapped.startEntered.await(5, TimeUnit.SECONDS)).as("the start is inside the wrapped model's start").isTrue();

        // When
        CountDownLatch shutdownReturned = new CountDownLatch(1);
        calls.add(new CallOnAThread(() -> {
            model.shutdown();
            shutdownReturned.countDown();
        }));

        // Then
        assertThat(shutdownReturned.await(5, TimeUnit.SECONDS)).as("whether the shutdown returned within 5 seconds while the start was stuck in the wrapped start").isTrue();
        assertThat(startReturned.await(5, TimeUnit.SECONDS)).as("whether the start returned once the shutdown had ended it").isTrue();
    }

    // Its start(..) returns once shutdown() is called, as a model does that waits for its stream to start
    private static final class StuckUntilShutdownSubscriptionModel implements CheckpointAwareSubscriptionModel {
        private final InMemorySubscriptionModel delegate;
        final CountDownLatch startEntered = new CountDownLatch(1);
        private final CountDownLatch shutdownCalled = new CountDownLatch(1);

        private StuckUntilShutdownSubscriptionModel(InMemorySubscriptionModel delegate) {
            this.delegate = delegate;
        }

        // Ends a start that nothing else ended, which a test does in a finally
        void release() {
            shutdownCalled.countDown();
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribe(subscriptionId, filter, StartAt.now(), action);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return new StringBasedCheckpoint("in-memory-global-position");
        }

        @Override
        public void stop() {
            delegate.stop();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            startEntered.countDown();
            try {
                if (!shutdownCalled.await(30, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("shutdown not called");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            delegate.start(resumeSubscriptionsAutomatically);
        }

        @Override
        public boolean isRunning() {
            return delegate.isRunning();
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return delegate.isRunning(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return delegate.isPaused(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return delegate.resumeSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            delegate.pauseSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            delegate.cancelSubscription(subscriptionId);
        }

        @Override
        public void shutdown() {
            shutdownCalled.countDown();
            delegate.shutdown();
        }
    }
}
