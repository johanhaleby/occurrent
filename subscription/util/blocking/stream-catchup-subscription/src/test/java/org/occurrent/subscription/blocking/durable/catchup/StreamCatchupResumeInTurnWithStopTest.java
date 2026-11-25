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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.StartAt;
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
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A {@code stop()}, from another thread, that completes while a {@code resumeSubscription(x)} is under way must not
 * leave the model reading as stopped while the live delegate runs x. The end state is that of the two calls made one
 * after the other, in either order, so either the model is stopped and x paused, or the model and x both run.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class StreamCatchupResumeInTurnWithStopTest {

    private static final String X = "x";
    private static final String Y = "y";
    private static final Duration WAIT = Duration.ofSeconds(10);

    private final CountDownLatch inTheActionOfX = new CountDownLatch(1);
    private final CountDownLatch goOn = new CountDownLatch(1);
    private final CountDownLatch resumeDecided = new CountDownLatch(1);
    private final CountDownLatch stopReturned = new CountDownLatch(1);
    private final CountDownLatch resumeReleased = new CountDownLatch(1);
    private final CountDownLatch actionReturned = new CountDownLatch(1);
    private final AtomicReference<@Nullable Throwable> thrownByTheAction = new AtomicReference<>();
    private final AtomicReference<@Nullable Subscription> handleOfTheResume = new AtomicReference<>();
    private InMemorySubscriptionModel inMemory;
    private HookedSubscriptionModel live;
    private InMemoryEventStore eventStore;
    private StreamCatchupSubscriptionModel model;

    @BeforeEach
    void create_a_started_model_over_an_in_memory_model() {
        inMemory = new InMemorySubscriptionModel();
        live = new HookedSubscriptionModel(inMemory);
        eventStore = new InMemoryEventStore(inMemory).withoutStreamPosition();
        model = new StreamCatchupSubscriptionModel(live, eventStore, new CatchupSubscriptionModelConfig(100));
    }

    @AfterEach
    void release_everything_and_shut_down() throws Exception {
        goOn.countDown();
        resumeDecided.countDown();
        stopReturned.countDown();
        resumeReleased.countDown();
        CompletableFuture.runAsync(() -> {
            model.shutdown();
            inMemory.shutdown();
        }, daemonExecutor("shutdown-of-the-model")).get(10, TimeUnit.SECONDS);
    }

    @Test
    void a_stop_from_another_thread_made_after_the_action_of_a_replay_called_resume_for_a_held_subscription_leaves_the_model_and_the_subscription_as_one_order_of_the_two_calls_would() throws Exception {
        // Given y that the live delegate holds paused, and a replay of x whose action resumes y and then waits for a stop from another thread to return
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        model.subscribe(Y, null, StartAt.now(), event -> {
        });
        model.pauseSubscription(Y);
        assertThat(model.isPaused(Y)).as("whether y is held paused before the test starts").isTrue();
        // The live delegate's resume waits for the stop, so a resume that reached it before the stop ran after the stop.
        // A stop made after the resume was handed over runs after it and cannot return first, so the wait ends
        live.beforeResume = () -> {
            resumeDecided.countDown();
            awaitUpTo(stopReturned, Duration.ofMillis(1500));
        };
        model.subscribe(X, StartAtTime.beginningOfTime(), event -> {
            if (inTheActionOfX.getCount() == 0) {
                return;
            }
            inTheActionOfX.countDown();
            awaitLatch(goOn);
            try {
                handleOfTheResume.set(model.resumeSubscription(Y));
            } catch (Throwable e) {
                thrownByTheAction.set(e);
            }
            resumeDecided.countDown();
            awaitUpTo(stopReturned, Duration.ofMillis(1500));
            actionReturned.countDown();
        });
        assertThat(inTheActionOfX.await(10, TimeUnit.SECONDS)).as("whether the replay of x reached its action").isTrue();
        CompletableFuture<Void> stop = CompletableFuture.runAsync(() -> {
            awaitLatch(resumeDecided);
            model.stop();
            stopReturned.countDown();
        }, daemonExecutor("stop-of-the-model"));

        // When the action calls resume for y, a stop from another thread is made, and the action returns
        goOn.countDown();
        stop.get(30, TimeUnit.SECONDS);
        assertThat(actionReturned.await(10, TimeUnit.SECONDS)).as("whether the action of the replay of x returned").isTrue();
        model.awaitTheHandedCalls(null, "test");
        assertThat(thrownByTheAction.get()).as("what resumeSubscription(y) threw in the action").isNull();
        assertThat(handleOfTheResume.get()).as("the handle resumeSubscription(y) returned to the action").isNotNull();

        // Then y and a later historical subscription read as the resume followed by the stop, or the stop followed by the resume
        assertThat(readAndALaterHistoricalSubscription(Y))
                .as("whether y runs, y is paused and a later historical subscription replays: resume then stop is [false, true, false], stop then resume is [true, false, true]")
                .isIn(List.of(false, true, false), List.of(true, false, true));
    }

    @Test
    void a_stop_from_another_thread_made_while_a_resume_from_another_thread_waits_in_the_live_delegate_leaves_the_model_and_the_subscription_as_one_order_of_the_two_calls_would() throws Exception {
        // Given a running model where x is paused, and a live delegate whose resume of x waits for a latch
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        model.subscribe(X, null, StartAt.now(), event -> {
        });
        model.pauseSubscription(X);
        live.beforeResume = () -> {
            resumeDecided.countDown();
            awaitLatch(resumeReleased);
        };
        CompletableFuture<Subscription> resume = CompletableFuture.supplyAsync(() -> model.resumeSubscription(X), daemonExecutor("resume-of-x"));
        assertThat(resumeDecided.await(10, TimeUnit.SECONDS)).as("whether the live delegate was asked to resume x").isTrue();

        // When a stop from another thread is made, given 500 ms to complete, and the live delegate then lets the resume go on
        CompletableFuture<Void> stop = CompletableFuture.runAsync(model::stop, daemonExecutor("stop-of-the-model"));
        try {
            stop.get(500, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ignored) {
            // The stop waits for the lock the resume holds, and runs once it has been released
        }
        resumeReleased.countDown();
        stop.get(30, TimeUnit.SECONDS);
        resume.get(30, TimeUnit.SECONDS);

        // Then x and a later historical subscription read as the resume followed by the stop, or the stop followed by the resume
        assertThat(readAndALaterHistoricalSubscription(X))
                .as("whether x runs, x is paused and a later historical subscription replays: resume then stop is [false, true, false], stop then resume is [true, false, true]")
                .isIn(List.of(false, true, false), List.of(true, false, true));
    }

    // Reads whether the subscription runs and is paused, and then subscribes z from the beginning of time and reads whether
    // its replay delivered the history. A stopped model parks the replay of z, so z delivering nothing and reading paused
    // tells a stopped model from a started one, whatever the live delegate runs
    private List<Boolean> readAndALaterHistoricalSubscription(String subscriptionId) {
        boolean runs = model.isRunning(subscriptionId);
        boolean paused = model.isPaused(subscriptionId);
        AtomicBoolean zReceivedTheHistory = new AtomicBoolean();
        model.subscribe("z", null, StartAtTime.beginningOfTime(), event -> zReceivedTheHistory.set(true));
        try {
            await().atMost(WAIT).until(() -> zReceivedTheHistory.get() || model.isPaused("z"));
        } catch (ConditionTimeoutException ignored) {
            // The assertion on the state says that z neither delivered nor read as paused
        }
        return List.of(runs, paused, zReceivedTheHistory.get());
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

    private static void awaitUpTo(CountDownLatch latch, Duration timeout) {
        try {
            latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
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
