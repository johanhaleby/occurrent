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
import io.cloudevents.core.builder.CloudEventBuilder;
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

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A {@code stop()} from inside a live action returns at once and runs once the action has returned. A
 * {@code resumeSubscription(..)} that another thread calls after it, while the action is still running, takes effect
 * after that stop.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupSubscriptionModelResumeAfterAHandedStopTest {

    private InMemorySubscriptionModel inMemory;
    private StopTellingSubscriptionModel wrapped;
    private InMemoryEventStore eventStore;
    private CatchupSubscriptionModel model;
    private final CountDownLatch handedStopRan = new CountDownLatch(1);
    private final List<CallOnAThread> calls = new ArrayList<>();

    @BeforeEach
    void create_a_started_model_over_an_in_memory_model_that_tells_when_it_was_stopped() {
        inMemory = new InMemorySubscriptionModel();
        wrapped = new StopTellingSubscriptionModel(inMemory, handedStopRan);
        eventStore = new InMemoryEventStore(inMemory).withoutStreamPosition();
        model = new CatchupSubscriptionModel(wrapped, eventStore, new CatchupSubscriptionModelConfig(100));
    }

    @AfterEach
    void release_everything_and_shut_down() {
        try {
            calls.forEach(CallOnAThread::awaitEnded);
        } finally {
            model.shutdown();
            inMemory.shutdown();
        }
    }

    @Test
    @Timeout(40)
    void a_resume_called_after_a_stop_that_a_live_action_handed_over_takes_effect_after_that_stop() throws InterruptedException {
        // Given
        model.subscribe("x", StartAt.now(), event -> {
        });
        model.pauseSubscription("x");
        CountDownLatch inTheAction = new CountDownLatch(1);
        CountDownLatch resumeCallStarted = new CountDownLatch(1);
        AtomicReference<CallOnAThread> resume = new AtomicReference<>();
        model.subscribe("y", StartAt.now(), event -> {
            model.stop();
            inTheAction.countDown();
            awaitLatch(resumeCallStarted);
            // Returns once the resume returned or waits, as it does for the handed stop's turn
            resume.get().awaitReturnedOrWaiting();
        });
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        assertThat(inTheAction.await(5, TimeUnit.SECONDS)).as("the live action of y called stop() and is still in its action").isTrue();

        // When
        CallOnAThread resumeCall = new CallOnAThread(() -> model.resumeSubscription("x"));
        calls.add(resumeCall);
        resume.set(resumeCall);
        resumeCallStarted.countDown();

        // Then
        assertThat(handedStopRan.await(10, TimeUnit.SECONDS)).as("whether the stop the action handed over ran").isTrue();
        resumeCall.awaitEnded();
        assertThat(model.isPaused("x")).as("whether x is paused after a stop that was handed over and a resume called after it").isFalse();
        assertThat(model.isRunning()).as("whether the model runs after a stop that was handed over and a resume called after it").isTrue();
    }

    private static CloudEvent cloudEvent() {
        return CloudEventBuilder.v1().withId(UUID.randomUUID().toString()).withSource(URI.create("urn:test")).withType("test.event").withTime(OffsetDateTime.now()).build();
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("latch not released");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    // One call on a thread of its own, which fails awaitEnded() when it threw
    private static final class CallOnAThread {
        private final Thread thread;
        private final AtomicBoolean returned = new AtomicBoolean();
        private final AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();

        private CallOnAThread(Runnable body) {
            thread = new Thread(() -> {
                try {
                    body.run();
                } catch (Throwable e) {
                    thrown.set(e);
                } finally {
                    returned.set(true);
                }
            }, "call-on-a-thread");
            thread.setDaemon(true);
            thread.start();
        }

        // Returns once the call has returned or its thread waits or is blocked, as a call that waits for a lock or a latch does
        void awaitReturnedOrWaiting() {
            EnumSet<Thread.State> waiting = EnumSet.of(Thread.State.WAITING, Thread.State.TIMED_WAITING, Thread.State.BLOCKED);
            await().atMost(Duration.ofSeconds(10)).until(() -> returned.get() || waiting.contains(thread.getState()));
        }

        void awaitEnded() {
            try {
                thread.join(15_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            assertThat(thread.isAlive()).as("whether the call is still running 15 seconds after the test released what held it").isFalse();
            assertThat(thrown.get()).as("what the call threw").isNull();
        }
    }

    // Wraps the in-memory model and tells when its stop() has run
    private static final class StopTellingSubscriptionModel implements CheckpointAwareSubscriptionModel {
        private final InMemorySubscriptionModel delegate;
        private final CountDownLatch stopRan;

        private StopTellingSubscriptionModel(InMemorySubscriptionModel delegate, CountDownLatch stopRan) {
            this.delegate = delegate;
            this.stopRan = stopRan;
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
            stopRan.countDown();
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
            delegate.shutdown();
        }
    }
}
