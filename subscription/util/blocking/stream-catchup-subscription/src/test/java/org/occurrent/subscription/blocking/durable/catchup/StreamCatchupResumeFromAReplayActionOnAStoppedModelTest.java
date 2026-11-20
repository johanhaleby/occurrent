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
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A {@code stop()} that gives up waiting for the action of a replay parks every replay in flight, so the subscription of
 * another replay it parked reads as paused. A {@code resumeSubscription(..)} that the still running action then calls
 * for that subscription only hands the start over to be run once the action has returned, since the model is stopped
 * until then. The resume must still take effect once the action has returned: the model runs, the subscription runs and
 * the handle the resume returned starts.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class StreamCatchupResumeFromAReplayActionOnAStoppedModelTest {

    private static final String X = "x";
    private static final String Y = "y";

    private final CountDownLatch inTheActionOfX = new CountDownLatch(1);
    private final CountDownLatch inTheActionOfY = new CountDownLatch(1);
    private final CountDownLatch resumeNow = new CountDownLatch(1);
    private final CountDownLatch releaseY = new CountDownLatch(1);
    private final CountDownLatch actionOfXReturned = new CountDownLatch(1);
    private final AtomicBoolean resumeCalled = new AtomicBoolean();
    private final CompletableFuture<Subscription> resumed = new CompletableFuture<>();
    private InMemorySubscriptionModel inMemory;
    private InMemoryEventStore eventStore;
    private StreamCatchupSubscriptionModel model;

    @BeforeEach
    void create_a_started_model_over_an_in_memory_model() {
        inMemory = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(inMemory).withoutStreamPosition();
        model = new StreamCatchupSubscriptionModel(new HookedSubscriptionModel(inMemory), eventStore, new CatchupSubscriptionModelConfig(100));
    }

    @AfterEach
    void release_everything_and_shut_down() throws InterruptedException {
        resumeNow.countDown();
        releaseY.countDown();
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
    void a_resume_that_the_action_of_a_replay_calls_for_a_subscription_a_stop_parked_takes_effect_once_the_action_has_returned() throws Exception {
        // Given a replay of x whose action is held, and a replay of y whose action is held, both of which a stop parks
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        model.subscribe(X, StartAtTime.beginningOfTime(), event -> {
            if (resumeCalled.compareAndSet(false, true)) {
                inTheActionOfX.countDown();
                awaitLatch(resumeNow);
                try {
                    resumed.complete(model.resumeSubscription(Y));
                } catch (Throwable e) {
                    resumed.completeExceptionally(e);
                } finally {
                    actionOfXReturned.countDown();
                }
            }
        });
        model.subscribe(Y, StartAtTime.beginningOfTime(), event -> {
            inTheActionOfY.countDown();
            awaitLatch(releaseY);
        });
        assertThat(inTheActionOfX.await(10, TimeUnit.SECONDS)).as("whether the replay of x reached its action").isTrue();
        assertThat(inTheActionOfY.await(10, TimeUnit.SECONDS)).as("whether the replay of y reached its action").isTrue();

        // And a stop that gives up waiting for the two actions and returns
        CompletableFuture<Void> stop = CompletableFuture.runAsync(model::stop, daemonExecutor("stop-of-the-model"));
        Throwable stopFailed = null;
        boolean stopReturned = true;
        try {
            stop.get(10, TimeUnit.SECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            stopReturned = false;
        } catch (Exception e) {
            stopFailed = e;
        }
        assertThat(stopReturned).as("whether stop() returned within 10 s although the actions of x and y still ran").isTrue();
        assertThat(stopFailed).as("what stop() threw").isNull();
        assertThat(model.isRunning()).as("whether the model runs right after the stop returned").isFalse();
        assertThat(model.isPaused(Y)).as("whether y reads as paused right after the stop parked its replay").isTrue();
        releaseY.countDown();

        // When the action of x resumes y and then returns
        resumeNow.countDown();
        assertThat(actionOfXReturned.await(10, TimeUnit.SECONDS)).as("whether the action of x returned within 10 s of calling resumeSubscription(y)").isTrue();
        Throwable resumeThrew = null;
        Subscription handle = null;
        try {
            handle = resumed.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            resumeThrew = e;
        }

        // Then
        assertThat(resumeThrew).as("what resumeSubscription(y) threw when the action of x called it on a stopped model").isNull();
        try {
            await().atMost(Duration.ofSeconds(10)).until(() -> model.isRunning() && model.isRunning(Y) && !model.isPaused(Y));
        } catch (ConditionTimeoutException ignored) {
            // The assertions below say which of the three did not hold
        }
        assertThat(model.isRunning()).as("whether the model runs within 10 s of the action of x returning").isTrue();
        assertThat(model.isRunning(Y)).as("whether y runs within 10 s of the action of x returning").isTrue();
        assertThat(model.isPaused(Y)).as("whether y is still paused within 10 s of the action of x returning").isFalse();
        assertThat(handle).as("the handle resumeSubscription(y) returned").isNotNull();
        assertThat(handle.waitUntilStarted(Duration.ofSeconds(10))).as("whether the handle resumeSubscription(y) returned started within 10 s").isTrue();
    }

    @Test
    void a_stop_that_completes_after_the_action_of_a_replay_decided_to_resume_leaves_the_model_stopped_and_the_subscription_paused_until_the_next_start() throws Exception {
        // Given a replay of x whose action is held, and a replay of y whose action is held, on a model that runs
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        AtomicReference<Thread> threadOfX = new AtomicReference<>();
        AtomicBoolean stopRun = new AtomicBoolean();
        model.subscribe(X, StartAtTime.beginningOfTime(), event -> {
            if (resumeCalled.compareAndSet(false, true)) {
                threadOfX.set(Thread.currentThread());
                inTheActionOfX.countDown();
                awaitLatch(resumeNow);
                try {
                    resumed.complete(model.resumeSubscription(Y));
                } catch (Throwable e) {
                    resumed.completeExceptionally(e);
                } finally {
                    actionOfXReturned.countDown();
                }
            }
        });
        model.subscribe(Y, StartAtTime.beginningOfTime(), event -> {
            inTheActionOfY.countDown();
            awaitLatch(releaseY);
        });
        assertThat(inTheActionOfX.await(10, TimeUnit.SECONDS)).as("whether the replay of x reached its action").isTrue();
        assertThat(inTheActionOfY.await(10, TimeUnit.SECONDS)).as("whether the replay of y reached its action").isTrue();

        // And a stop, on another thread, that completes right after the resume decided the model runs, and parks the replay of y
        model.runAfterTheResumeDecision(() -> {
            if (Thread.currentThread() == threadOfX.get() && stopRun.compareAndSet(false, true)) {
                try {
                    CompletableFuture.runAsync(model::stop, daemonExecutor("stop-of-the-model")).get(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new IllegalStateException("stop() did not return within 10 s", e);
                }
                releaseY.countDown();
            }
        });

        // When the action of x resumes y, the stop completes in between, and the action returns
        resumeNow.countDown();
        assertThat(actionOfXReturned.await(30, TimeUnit.SECONDS)).as("whether the action of x returned within 30 s of calling resumeSubscription(y)").isTrue();
        Throwable resumeThrew = null;
        Subscription handle = null;
        try {
            handle = resumed.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            resumeThrew = e;
        }
        assertThat(resumeThrew).as("what resumeSubscription(y) threw when a stop completed after the action of x decided to resume").isNull();
        assertThat(stopRun.get()).as("whether the stop ran between the decision to resume and the rest of the resume").isTrue();
        model.awaitTheHandedCalls();

        // Then the outcome is that of resuming and then stopping
        assertThat(model.isRunning()).as("whether the model runs after the action of x returned and every call handed to the lifecycle thread ran").isFalse();
        assertThat(model.isPaused(Y)).as("whether y is paused after the action of x returned and every call handed to the lifecycle thread ran").isTrue();

        // And the next start runs the replay of y again
        model.start();
        try {
            await().atMost(Duration.ofSeconds(10)).until(() -> model.isRunning(Y) && !model.isPaused(Y));
        } catch (ConditionTimeoutException ignored) {
            // The assertions below say which of the two did not hold
        }
        assertThat(model.isRunning(Y)).as("whether y runs within 10 s of the next start()").isTrue();
        assertThat(model.isPaused(Y)).as("whether y is still paused within 10 s of the next start()").isFalse();
        assertThat(handle).as("the handle resumeSubscription(y) returned").isNotNull();
        assertThat(handle.waitUntilStarted(Duration.ofSeconds(10))).as("whether the handle resumeSubscription(y) returned started within 10 s of the next start()").isTrue();
    }

    private static Executor daemonExecutor(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            thread.start();
        };
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
}
