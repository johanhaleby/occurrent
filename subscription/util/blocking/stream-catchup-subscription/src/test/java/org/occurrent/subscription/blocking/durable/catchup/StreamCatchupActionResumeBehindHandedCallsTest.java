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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * A call the action of a live call or a replay makes for a subscription of the model takes effect once the action has
 * returned, in the order the action made it, together with the {@code stop()} and {@code start(..)} calls the same
 * action handed over. So {@code stop()} followed by {@code resumeSubscription(x)} from one action ends with the model
 * and x running, and {@code resumeSubscription(x)} followed by {@code pauseSubscription(x)} ends with x paused and the
 * pause not refused.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class StreamCatchupActionResumeBehindHandedCallsTest {

    private static final String X = "x";
    private static final String Y = "y";
    private static final Duration WAIT = Duration.ofSeconds(10);

    private final CountDownLatch actionReturned = new CountDownLatch(1);
    private final AtomicReference<@Nullable Throwable> thrownByTheAction = new AtomicReference<>();
    private final AtomicReference<@Nullable Subscription> handleOfTheResume = new AtomicReference<>();
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
    void release_everything_and_shut_down() throws Exception {
        CompletableFuture.runAsync(() -> {
            model.shutdown();
            inMemory.shutdown();
        }).get(10, TimeUnit.SECONDS);
    }

    @Test
    void a_stop_and_then_a_resume_from_a_live_action_leave_the_model_running_and_the_resumed_subscription_running_and_the_handle_started() throws Exception {
        // Given a running model where x is paused and the action of y is a live action that stops the model and resumes x
        givenXPaused();
        model.subscribe(Y, null, StartAt.now(), actionOf(() -> {
            model.stop();
            handleOfTheResume.set(model.resumeSubscription(X));
        }));

        // When y receives an event, and the calls the action handed over have run
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        assertThat(actionReturned.await(10, TimeUnit.SECONDS)).as("whether the action of y returned").isTrue();
        model.awaitTheHandedCalls("test");

        // Then the outcome is that of the stop followed by the resume, which finds the model stopped and x paused
        assertThat(thrownByTheAction.get()).as("what the action threw").isNull();
        assertThat(model.isRunning()).as("whether the model runs after the stop and the resume the action handed over ran").isTrue();
        assertThat(model.isRunning(X)).as("whether x runs after the stop and the resume the action handed over ran").isTrue();
        assertThat(model.isPaused(X)).as("whether x is paused after the stop and the resume the action handed over ran").isFalse();
        assertThat(handleOfTheResume.get().waitUntilStarted(WAIT)).as("whether the handle of resumeSubscription(x) answers started").isTrue();
    }

    @Test
    void a_stop_and_then_a_resume_from_the_action_of_a_replay_leave_the_model_running_and_the_resumed_subscription_running_and_the_handle_started() throws Exception {
        // Given a running model where x is paused and the action of the replay of y stops the model and resumes x
        givenXPaused();
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        model.subscribe(Y, StartAtTime.beginningOfTime(), actionOf(() -> {
            model.stop();
            handleOfTheResume.set(model.resumeSubscription(X));
        }));

        // When the replay of y reaches its action, and the calls the action handed over have run
        assertThat(actionReturned.await(10, TimeUnit.SECONDS)).as("whether the action of the replay of y returned").isTrue();
        model.awaitTheHandedCalls("test");

        // Then the outcome is that of the stop followed by the resume, which finds the model stopped and x paused
        assertThat(thrownByTheAction.get()).as("what the action threw").isNull();
        assertThat(model.isRunning()).as("whether the model runs after the stop and the resume the action handed over ran").isTrue();
        assertThat(model.isRunning(X)).as("whether x runs after the stop and the resume the action handed over ran").isTrue();
        assertThat(model.isPaused(X)).as("whether x is paused after the stop and the resume the action handed over ran").isFalse();
        assertThat(handleOfTheResume.get().waitUntilStarted(WAIT)).as("whether the handle of resumeSubscription(x) answers started").isTrue();
    }

    @Test
    void a_resume_and_then_a_pause_of_the_same_subscription_from_a_live_action_leave_it_paused_and_the_pause_is_not_refused() throws Exception {
        // Given a running model where x is paused and the action of y is a live action that resumes x and then pauses it
        givenXPaused();
        model.subscribe(Y, null, StartAt.now(), actionOf(() -> {
            handleOfTheResume.set(model.resumeSubscription(X));
            model.pauseSubscription(X);
        }));

        // When y receives an event, and the calls the action handed over have run
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        assertThat(actionReturned.await(10, TimeUnit.SECONDS)).as("whether the action of y returned").isTrue();
        model.awaitTheHandedCalls("test");

        // Then the outcome is that of the resume followed by the pause, which finds x running
        assertThat(thrownByTheAction.get()).as("what the action threw, as the pause finds x still paused when it runs at once").isNull();
        assertThat(model.isPaused(X)).as("whether x is paused after the resume and the pause the action handed over ran").isTrue();
        assertThat(model.isRunning(X)).as("whether x runs after the resume and the pause the action handed over ran").isFalse();
    }

    @Test
    void a_resume_and_then_a_cancel_of_the_same_subscription_from_a_live_action_run_in_that_order_so_x_is_started_and_then_unknown_to_the_model() throws Exception {
        // Given a running model where x is paused and the action of y is a live action that resumes x and then cancels it
        givenXPaused();
        model.subscribe(Y, null, StartAt.now(), actionOf(() -> {
            handleOfTheResume.set(model.resumeSubscription(X));
            model.cancelSubscription(X);
        }));

        // When y receives an event, and the calls the action handed over have run
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        assertThat(actionReturned.await(10, TimeUnit.SECONDS)).as("whether the action of y returned").isTrue();
        model.awaitTheHandedCalls("test");

        // Then the outcome is that of the resume followed by the cancel, so x is unknown to the model, and the resume had started it before the cancel took it away
        assertThat(thrownByTheAction.get()).as("what the action threw").isNull();
        assertThat(model.isRunning(X)).as("whether x runs after the resume and the cancel the action made ran").isFalse();
        assertThat(model.isPaused(X)).as("whether x is paused after the resume and the cancel the action made ran").isFalse();
        AtomicReference<Boolean> started = new AtomicReference<>();
        assertThat(catchThrowable(() -> started.set(handleOfTheResume.get().waitUntilStarted(WAIT)))).as("what the handle of resumeSubscription(x) threw").isNull();
        assertThat(started.get()).as("whether the handle of resumeSubscription(x) reports the subscription started, as the resume ran before the cancel").isTrue();
    }

    @Test
    void a_start_and_then_a_subscribe_from_the_action_of_a_replay_on_a_stopped_model_start_the_model_and_replay_the_new_subscription() throws Exception {
        // Given a replay of x whose action is held, and a model that a stop from the test thread parked it on
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        CountDownLatch inTheActionOfX = new CountDownLatch(1);
        CountDownLatch goOn = new CountDownLatch(1);
        AtomicBoolean zReceivedTheHistory = new AtomicBoolean();
        model.subscribe("x-replay", StartAtTime.beginningOfTime(), actionOf(() -> {
            inTheActionOfX.countDown();
            try {
                goOn.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            model.start(false);
            model.subscribe("z", null, StartAtTime.beginningOfTime(), event -> zReceivedTheHistory.set(true));
        }));
        assertThat(inTheActionOfX.await(10, TimeUnit.SECONDS)).as("whether the replay of x reached its action").isTrue();
        CompletableFuture.runAsync(model::stop).get(10, TimeUnit.SECONDS);
        assertThat(model.isRunning()).as("whether the model reads as stopped before the action goes on").isFalse();

        // When the action starts the model and subscribes z from the beginning of time, and the calls it handed over have run
        goOn.countDown();
        assertThat(actionReturned.await(10, TimeUnit.SECONDS)).as("whether the action of the replay of x returned").isTrue();
        model.awaitTheHandedCalls("test");

        // Then the outcome is that of the start followed by the subscribe, so the model runs, and z replays the history and is not paused
        assertThat(thrownByTheAction.get()).as("what the action threw").isNull();
        assertThat(model.isRunning()).as("whether the model runs after the start the action handed over ran").isTrue();
        try {
            await().atMost(WAIT).until(() -> zReceivedTheHistory.get() || model.isPaused("z"));
        } catch (ConditionTimeoutException ignored) {
            // The assertions below say which of the two did not hold
        }
        assertThat(zReceivedTheHistory.get()).as("whether z replayed the history after the start and the subscribe the action made").isTrue();
        assertThat(model.isPaused("z")).as("whether z is paused after the start and the subscribe the action made").isFalse();
    }

    private void givenXPaused() {
        model.subscribe(X, null, StartAt.now(), event -> {
        });
        model.pauseSubscription(X);
        assertThat(model.isPaused(X)).as("whether x is paused before the test starts").isTrue();
    }

    // Runs calls on the thread of the action once, and records what it threw
    private Consumer<CloudEvent> actionOf(Runnable calls) {
        return event -> {
            try {
                calls.run();
            } catch (Throwable e) {
                thrownByTheAction.set(e);
            } finally {
                actionReturned.countDown();
            }
        };
    }

    private static CloudEvent cloudEvent() {
        return CloudEventBuilder.v1().withId(UUID.randomUUID().toString()).withSource(URI.create("urn:test")).withType("test.event").withTime(OffsetDateTime.now()).build();
    }
}
