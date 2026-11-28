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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A replay whose handover to the live delegate is under way has left the attempts a stop parks, since it read the model
 * as not stopped before the stop began. A {@code stop()} of a {@link CatchupSubscriptionModel} must still wait for that
 * handover, so that the live delegate holds the subscription when the stop stops the delegate. Right after the stop
 * returned the subscription then reads as paused, never as neither paused nor running, and the next start runs it. This
 * holds for the replay of a plain subscribe and for the replay a resume relaunched from the action of another replay.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class CatchupSubscriptionModelStopWaitsForAHandoverUnderWayTest {

    private static final String ID = "subscription";

    private final CountDownLatch inTheLiveSubscribe = new CountDownLatch(1);
    private final CountDownLatch releaseTheLiveSubscribe = new CountDownLatch(1);
    private InMemorySubscriptionModel inMemory;
    private DelegatingSubscriptionModel wrapped;
    private InMemoryEventStore eventStore;
    private CatchupSubscriptionModel model;

    @BeforeEach
    void create_a_model_over_a_live_delegate_whose_subscribe_can_be_held() {
        inMemory = new InMemorySubscriptionModel();
        wrapped = new DelegatingSubscriptionModel(inMemory);
        eventStore = new InMemoryEventStore(inMemory).withoutStreamPosition();
        model = new CatchupSubscriptionModel(wrapped, eventStore, new CatchupSubscriptionModelConfig(100));
    }

    @AfterEach
    void release_everything_and_shut_down() {
        releaseTheLiveSubscribe.countDown();
        CompletableFuture<Void> shutdown = CompletableFuture.runAsync(() -> {
            model.shutdown();
            inMemory.shutdown();
        }, daemonExecutor("shutdown-of-the-model"));
        try {
            shutdown.get(10, TimeUnit.SECONDS);
        } catch (Exception ignored) {
            // A shutdown that hangs is left on its daemon thread, the assertions of the test have already reported it
        }
    }

    @Test
    void a_stop_made_while_the_handover_of_a_replay_is_held_in_the_live_delegates_subscribe_leaves_the_subscription_paused_once_it_returns_and_the_next_start_runs_it() throws Exception {
        // Given a replay that has read the model as not stopped and is held inside the live delegate's subscribe
        write();
        holdTheLiveSubscribeOf(ID);
        model.subscribe(ID, StartAtTime.beginningOfTime(), event -> {
        });
        assertThat(inTheLiveSubscribe.await(10, TimeUnit.SECONDS)).as("whether the replay reached the live delegate's subscribe").isTrue();

        // When a stop is made from another thread, which records what it reads right after it returned
        StopRecord afterTheStop = stopFromAnotherThreadWhileTheLiveSubscribeIsHeld(ID);

        // Then
        assertThat(afterTheStop.runs).as("whether the model runs right after the stop returned").isFalse();
        assertThat(afterTheStop.paused).as("whether the subscription reads as paused right after the stop returned").isTrue();

        // And the next start runs the subscription
        model.start();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(model.isRunning()).as("whether the model runs within 10 s of the next start()").isTrue();
            assertThat(model.isRunning(ID)).as("whether the subscription runs within 10 s of the next start()").isTrue();
            assertThat(model.isPaused(ID)).as("whether the subscription is still paused within 10 s of the next start()").isFalse();
        });
    }

    @Test
    void a_stop_made_while_the_replay_a_resume_from_the_action_of_another_replay_relaunched_is_held_in_the_live_delegates_subscribe_leaves_the_subscription_paused_once_it_returns_and_the_next_start_runs_it() throws Exception {
        // Given a replay of x and a replay of y whose actions are held, and a stop that gave up waiting for them and parked both
        String x = "x";
        String y = "y";
        write();
        holdTheLiveSubscribeOf(y);
        CountDownLatch inTheActionOfX = new CountDownLatch(1);
        CountDownLatch inTheActionOfY = new CountDownLatch(1);
        CountDownLatch resumeNow = new CountDownLatch(1);
        CountDownLatch releaseY = new CountDownLatch(1);
        AtomicBoolean firstActionOfX = new AtomicBoolean(true);
        CompletableFuture<Subscription> resumed = new CompletableFuture<>();
        model.subscribe(x, StartAtTime.beginningOfTime(), event -> {
            if (firstActionOfX.compareAndSet(true, false)) {
                inTheActionOfX.countDown();
                awaitLatch(resumeNow);
                try {
                    resumed.complete(model.resumeSubscription(y));
                } catch (Throwable e) {
                    resumed.completeExceptionally(e);
                }
            }
        });
        model.subscribe(y, StartAtTime.beginningOfTime(), event -> {
            inTheActionOfY.countDown();
            awaitLatch(releaseY);
        });
        assertThat(inTheActionOfX.await(10, TimeUnit.SECONDS)).as("whether the replay of x reached its action").isTrue();
        assertThat(inTheActionOfY.await(10, TimeUnit.SECONDS)).as("whether the replay of y reached its action").isTrue();
        CompletableFuture.runAsync(model::stop, daemonExecutor("first-stop-of-the-model")).get(10, TimeUnit.SECONDS);
        assertThat(model.isPaused(y)).as("whether y reads as paused after the first stop parked its replay").isTrue();
        releaseY.countDown();

        // And the action of x resumed y, which the model hands over and runs once the action has returned, held in the live delegate's subscribe
        resumeNow.countDown();
        assertThat(inTheLiveSubscribe.await(10, TimeUnit.SECONDS)).as("whether the relaunched replay of y reached the live delegate's subscribe").isTrue();

        // When a stop is made from another thread, which records what it reads right after it returned
        StopRecord afterTheStop = stopFromAnotherThreadWhileTheLiveSubscribeIsHeld(y);

        // Then
        assertThat(afterTheStop.runs).as("whether the model runs right after the stop returned").isFalse();
        assertThat(afterTheStop.paused).as("whether y reads as paused right after the stop returned").isTrue();

        // And the next start runs y and the handle the resume returned starts
        model.start();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(model.isRunning()).as("whether the model runs within 10 s of the next start()").isTrue();
            assertThat(model.isRunning(y)).as("whether y runs within 10 s of the next start()").isTrue();
            assertThat(model.isPaused(y)).as("whether y is still paused within 10 s of the next start()").isFalse();
        });
        Subscription handle = resumed.get(10, TimeUnit.SECONDS);
        assertThat(handle.waitUntilStarted(Duration.ofSeconds(10))).as("whether the handle resumeSubscription(y) returned started within 10 s of the next start()").isTrue();
    }

    private void holdTheLiveSubscribeOf(String subscriptionId) {
        wrapped.beforeSubscribe = id -> {
            if (subscriptionId.equals(id)) {
                inTheLiveSubscribe.countDown();
                awaitLatch(releaseTheLiveSubscribe);
            }
        };
    }

    // A stop that waits for the handover has not returned within a second, one that does not has. The handover is held
    // until the first, so the test ends either way and never depends on the fix to end. What the stop read right after
    // it returned is read on its own thread
    private StopRecord stopFromAnotherThreadWhileTheLiveSubscribeIsHeld(String subscriptionId) throws Exception {
        StopRecord record = new StopRecord();
        CompletableFuture<Void> stop = CompletableFuture.runAsync(() -> {
            model.stop();
            record.runs = model.isRunning();
            record.paused = model.isPaused(subscriptionId);
        }, daemonExecutor("stop-of-the-model"));
        try {
            stop.get(1, TimeUnit.SECONDS);
        } catch (TimeoutException ignored) {
            // The stop is waiting for the handover
        }
        releaseTheLiveSubscribe.countDown();
        stop.get(10, TimeUnit.SECONDS);
        return record;
    }

    private static final class StopRecord {
        volatile boolean runs = true;
        volatile boolean paused;
    }

    private void write() {
        CloudEvent event = CloudEventBuilder.v1().withId(UUID.randomUUID().toString()).withSource(URI.create("urn:test")).withType("test.event").withTime(OffsetDateTime.now()).build();
        eventStore.write(UUID.randomUUID().toString(), List.of(event));
    }

    private static Executor daemonExecutor(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            thread.start();
        };
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

    // Wraps the in-memory model, which a catch-up model needs to ask for a global checkpoint, and runs a hook of the test before each subscribe
    private static final class DelegatingSubscriptionModel implements CheckpointAwareSubscriptionModel {
        private final InMemorySubscriptionModel delegate;
        volatile Consumer<String> beforeSubscribe = id -> {
        };

        private DelegatingSubscriptionModel(InMemorySubscriptionModel delegate) {
            this.delegate = delegate;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            beforeSubscribe.accept(subscriptionId);
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
