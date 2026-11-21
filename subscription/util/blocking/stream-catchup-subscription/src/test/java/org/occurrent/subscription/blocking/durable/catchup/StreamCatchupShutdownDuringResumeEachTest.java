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
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A {@code start(true)} that resumes each subscription the wrapped model reports as paused, one at a time, resumes no
 * further one once a {@code shutdown()} from another thread has set the flag in the meantime. The start is held at the
 * first of them by a cancel under way, which every resume of that id waits for.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupShutdownDuringResumeEachTest {

    private static final String CANCELLED = "cancelled";
    private static final String FIRST = "first";
    private static final String SECOND = "second";

    private InMemorySubscriptionModel inMemory;
    private ReportsTwoAsPaused wrapped;
    private StreamCatchupSubscriptionModel model;
    private final List<CallOnAThread> calls = new ArrayList<>();

    @BeforeEach
    void create_a_model_over_a_wrapped_model_that_reports_two_subscriptions_as_paused_and_holds_a_cancel() {
        inMemory = new InMemorySubscriptionModel();
        wrapped = new ReportsTwoAsPaused(inMemory);
        model = new StreamCatchupSubscriptionModel(wrapped, new InMemoryEventStore(inMemory).withoutStreamPosition(), new CatchupSubscriptionModelConfig(100));
    }

    @AfterEach
    void release_everything_and_shut_down() {
        try {
            wrapped.releaseTheCancel();
            calls.forEach(CallOnAThread::awaitEnded);
        } finally {
            model.shutdown();
            inMemory.shutdown();
        }
    }

    @Test
    @Timeout(40)
    void a_start_that_waits_for_a_cancel_resumes_the_paused_subscriptions_once_the_cancel_has_returned() throws InterruptedException {
        // Given
        CallOnAThread start = aStartWaitingForACancel();

        // When
        wrapped.releaseTheCancel();

        // Then
        start.awaitEnded();
        assertThat(wrapped.resumed).as("the subscriptions resumed on the wrapped model").contains(FIRST, SECOND);
    }

    @Test
    @Timeout(40)
    void a_shutdown_from_another_thread_while_a_start_waits_for_a_cancel_stops_the_start_from_resuming_the_paused_subscriptions_after_it() throws InterruptedException {
        // Given
        aStartWaitingForACancel();

        // When
        CallOnAThread shutdown = new CallOnAThread(model::shutdown);
        calls.add(shutdown);
        shutdown.awaitReturnedOrWaiting();
        wrapped.releaseTheCancel();

        // Then
        calls.forEach(CallOnAThread::awaitEnded);
        assertThat(wrapped.resumed).as("the subscriptions resumed on the wrapped model, of which a start that a shutdown overtook while it waited for a cancel resumes none after it").doesNotContain(FIRST, SECOND);
    }

    private CallOnAThread aStartWaitingForACancel() throws InterruptedException {
        model.subscribe(CANCELLED, null, StartAt.now(), event -> {
        });
        model.subscribe(FIRST, null, StartAt.now(), event -> {
        });
        model.subscribe(SECOND, null, StartAt.now(), event -> {
        });
        model.pauseSubscription(FIRST);
        model.pauseSubscription(SECOND);
        model.stop();
        calls.add(new CallOnAThread(() -> model.cancelSubscription(CANCELLED)));
        assertThat(wrapped.cancelEntered.await(5, TimeUnit.SECONDS)).as("the cancel is inside the wrapped model's cancel").isTrue();
        CallOnAThread start = new CallOnAThread(() -> model.start(true));
        calls.add(start);
        start.awaitReturnedOrWaiting();
        return start;
    }

    // It reports FIRST and SECOND as paused, in that order, and holds the cancel of CANCELLED until it is released
    private static final class ReportsTwoAsPaused implements CheckpointAwareSubscriptionModel, IntrospectableSubscriptions {
        private final InMemorySubscriptionModel delegate;
        final CountDownLatch cancelEntered = new CountDownLatch(1);
        final List<String> resumed = new CopyOnWriteArrayList<>();
        private final CountDownLatch cancelReleased = new CountDownLatch(1);
        private final Set<String> paused = ConcurrentHashMap.newKeySet();

        private ReportsTwoAsPaused(InMemorySubscriptionModel delegate) {
            this.delegate = delegate;
        }

        void releaseTheCancel() {
            cancelReleased.countDown();
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
        public Set<String> subscriptionIds() {
            return new LinkedHashSet<>(List.of(FIRST, SECOND));
        }

        @Override
        public void stop() {
            delegate.stop();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            delegate.start(resumeSubscriptionsAutomatically);
        }

        @Override
        public boolean isRunning() {
            return delegate.isRunning();
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return !isPaused(subscriptionId) && delegate.isRunning(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return paused.contains(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            resumed.add(subscriptionId);
            paused.remove(subscriptionId);
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
        public void pauseSubscription(String subscriptionId) {
            paused.add(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            if (CANCELLED.equals(subscriptionId)) {
                cancelEntered.countDown();
                try {
                    if (!cancelReleased.await(30, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("cancel not released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
            delegate.cancelSubscription(subscriptionId);
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }
    }
}
