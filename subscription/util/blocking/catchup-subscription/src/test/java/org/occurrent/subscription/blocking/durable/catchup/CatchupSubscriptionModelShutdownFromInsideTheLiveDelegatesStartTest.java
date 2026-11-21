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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A {@code shutdown()} made on the thread of a {@code start(..)}, with no action or claim held, either from a
 * {@code StartAt.dynamic} function the start asks before the wrapped model's start, or from inside the wrapped model's
 * own {@code start(..)}. A shutdown that returned, or whose start returned, must leave the wrapped model shut down. So
 * the wrapped start must not be called after a shutdown that came before it, the wrapped start that goes on after a
 * shutdown made inside it must not leave the model running, and a later shutdown must still shut it down. The wrapped
 * model here is observed by the order of the calls it got, since the in-memory model it wraps reports itself running
 * after any start, whatever shut it down before.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupSubscriptionModelShutdownFromInsideTheLiveDelegatesStartTest {

    private InMemorySubscriptionModel inMemory;
    private ShutsDownInsideItsStartSubscriptionModel wrapped;
    private CatchupSubscriptionModel model;
    private ExecutorService executor;

    @BeforeEach
    void create_a_model_over_a_wrapped_model() {
        inMemory = new InMemorySubscriptionModel();
        wrapped = new ShutsDownInsideItsStartSubscriptionModel(inMemory);
        model = new CatchupSubscriptionModel(wrapped, new InMemoryEventStore(inMemory).withoutStreamPosition(), new CatchupSubscriptionModelConfig(100));
        executor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "call-on-a-thread");
            thread.setDaemon(true);
            return thread;
        });
    }

    @AfterEach
    void shut_down() {
        try {
            executor.shutdownNow();
        } finally {
            model.shutdown();
            inMemory.shutdown();
        }
    }

    @Test
    @Timeout(40)
    void a_shutdown_called_from_inside_the_wrapped_start_leaves_the_wrapped_models_last_call_a_shutdown_once_the_start_returns_and_a_later_shutdown_keeps_it_so() throws Exception {
        // Given
        model.stop();
        wrapped.beforeStart = () -> {
            try {
                model.shutdown();
            } catch (Throwable e) {
                wrapped.shutdownThrew.set(e);
            }
            wrapped.calls.add("start-proceeded");
        };

        // When
        Future<?> start = executor.submit(() -> model.start(false));
        start.get(5, TimeUnit.SECONDS);

        // Then
        assertThat(wrapped.calledShutdown).as("whether the wrapped start called the catch-up model's shutdown").isTrue();
        assertThat(wrapped.shutdownThrew.get()).as("what the shutdown called from inside the wrapped start threw").isNull();
        assertThat(wrapped.calls).as("the calls the wrapped model got, the last of which must be a shutdown after its start went on").contains("start-proceeded").last().isEqualTo("shutdown");
        int callsAfterTheStart = wrapped.calls.size();
        model.shutdown();
        assertThat(wrapped.calls).as("the calls the wrapped model got once a later shutdown had returned").hasSizeGreaterThanOrEqualTo(callsAfterTheStart).last().isEqualTo("shutdown");
        assertThat(wrapped.calls.lastIndexOf("start-proceeded")).as("the position of the wrapped start's going on, which no later call must follow without a shutdown").isLessThan(wrapped.calls.lastIndexOf("shutdown"));
    }

    @Test
    @Timeout(40)
    void a_shutdown_called_from_a_start_at_function_the_start_asks_before_the_wrapped_start_keeps_the_wrapped_start_from_being_called() throws Exception {
        // Given
        AtomicBoolean armed = new AtomicBoolean();
        AtomicBoolean calledShutdown = new AtomicBoolean();
        AtomicReference<@Nullable Throwable> shutdownThrew = new AtomicReference<>();
        model.subscribe("subscription", null, StartAt.dynamic(() -> {
            if (armed.get() && calledShutdown.compareAndSet(false, true)) {
                try {
                    model.shutdown();
                } catch (Throwable e) {
                    shutdownThrew.set(e);
                }
            }
            return null;
        }), event -> {
        });
        model.pauseSubscription("subscription");
        assertThat(model.isPaused("subscription")).as("whether the subscription is paused before the start").isTrue();
        model.stop();
        wrapped.calls.clear();
        wrapped.beforeStart = () -> wrapped.calls.add("start-proceeded");
        armed.set(true);

        // When
        Future<?> start = executor.submit(() -> model.start(true));
        start.get(5, TimeUnit.SECONDS);

        // Then
        assertThat(calledShutdown).as("whether the start asked the function, which called the catch-up model's shutdown").isTrue();
        assertThat(shutdownThrew.get()).as("what the shutdown called from the function threw").isNull();
        assertThat(wrapped.calls).as("the calls the wrapped model got, none of which may be a start once the function's shutdown came").doesNotContain("start-proceeded");
        assertThat(wrapped.isRunning()).as("whether the wrapped model runs once the start returned").isFalse();
    }

    // Runs its hook on the first start(..) only, before its own start, and logs the calls a test looks at
    private static final class ShutsDownInsideItsStartSubscriptionModel implements CheckpointAwareSubscriptionModel {
        private final InMemorySubscriptionModel delegate;
        final List<String> calls = new CopyOnWriteArrayList<>();
        final AtomicBoolean calledShutdown = new AtomicBoolean();
        final AtomicReference<@Nullable Throwable> shutdownThrew = new AtomicReference<>();
        volatile Runnable beforeStart = () -> {
        };

        private ShutsDownInsideItsStartSubscriptionModel(InMemorySubscriptionModel delegate) {
            this.delegate = delegate;
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
            if (calledShutdown.compareAndSet(false, true)) {
                beforeStart.run();
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
            calls.add("shutdown");
            delegate.shutdown();
        }
    }
}
