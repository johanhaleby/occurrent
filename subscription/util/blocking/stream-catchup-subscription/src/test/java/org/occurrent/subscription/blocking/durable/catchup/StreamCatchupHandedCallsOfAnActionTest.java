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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A lifecycle call an action hands over reaches the lifecycle thread once that action has returned, so the thread never
 * waits for an action. A call made from another thread while an action that handed one over still runs of the model gives up
 * after the wait for that action and changes nothing, while a call for another subscription runs, and a call another action
 * handed over runs although the first action has not returned.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupHandedCallsOfAnActionTest {

    private static final Duration A_CALL_THAT_GIVES_UP = Duration.ofSeconds(15);
    private static final Duration A_CALL_THAT_IS_NOT_HELD_UP = Duration.ofSeconds(5);

    private final CountDownLatch aHandedACallOver = new CountDownLatch(1);
    private final CountDownLatch releaseA = new CountDownLatch(1);
    private final AtomicBoolean aIsInItsAction = new AtomicBoolean();
    private InMemorySubscriptionModel inMemory;
    private HookedSubscriptionModel wrapped;
    private InMemoryEventStore eventStore;
    private StreamCatchupSubscriptionModel model;

    @BeforeEach
    void create_a_started_model_over_an_in_memory_model_and_subscribe_a_whose_action_hands_a_call_over_and_blocks() {
        inMemory = new InMemorySubscriptionModel();
        wrapped = new HookedSubscriptionModel(inMemory);
        eventStore = new InMemoryEventStore(inMemory).withoutStreamPosition();
        model = new StreamCatchupSubscriptionModel(wrapped, eventStore, new CatchupSubscriptionModelConfig(100));
        model.subscribe("A", StartAt.now(), event -> {
            if (aIsInItsAction.compareAndSet(false, true)) {
                model.resumeSubscription("A");
                aHandedACallOver.countDown();
                HookedSubscriptionModel.awaitLatch(releaseA);
            }
        });
    }

    @AfterEach
    void release_everything_and_shut_down() throws Exception {
        releaseA.countDown();
        CompletableFuture.runAsync(() -> {
            model.shutdown();
            inMemory.shutdown();
        }).get(20, TimeUnit.SECONDS);
    }

    @Test
    @Timeout(60)
    void a_resume_of_another_subscription_while_an_action_that_handed_a_call_over_blocks_resumes_it_and_it_receives_the_next_event() throws Exception {
        // Given b is paused, and the action of a has handed a resume over and blocks
        List<CloudEvent> receivedByB = new CopyOnWriteArrayList<>();
        model.subscribe("B", StartAt.now(), receivedByB::add);
        model.pauseSubscription("B");
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        assertThat(aHandedACallOver.await(5, TimeUnit.SECONDS)).as("whether the action of a handed its resume over").isTrue();

        // When the test thread resumes b while that action still runs
        CompletableFuture<Subscription> resumeWhileABlocks = CompletableFuture.supplyAsync(() -> model.resumeSubscription("B"));

        // Then the resume runs, since the call the action of a handed over concerns a and not b
        assertThat(resumeWhileABlocks).as("the resume of b made while the action of a that handed a call over blocks").succeedsWithin(A_CALL_THAT_IS_NOT_HELD_UP);
        assertThat(releaseA.getCount()).as("whether the action of a still blocks").isEqualTo(1);
        assertThat(model.isPaused("B")).as("whether b is paused after the resume").isFalse();

        // And b receives what is written after the resume
        CloudEvent written = cloudEvent();
        eventStore.write(UUID.randomUUID().toString(), List.of(written));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(receivedByB).as("what b received after it was resumed").extracting(CloudEvent::getId).containsExactly(written.getId()));
        assertThat(wrapped.isRunning()).as("whether the wrapped model runs as the model does").isEqualTo(model.isRunning()).isTrue();
    }

    @Test
    @Timeout(90)
    void a_stop_from_another_thread_while_an_action_that_handed_a_call_over_blocks_gives_up_and_changes_nothing_and_a_stop_after_the_action_returned_runs() throws Exception {
        // Given the action of a has handed a resume over and blocks
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        assertThat(aHandedACallOver.await(5, TimeUnit.SECONDS)).as("whether the action of a handed its resume over").isTrue();

        // When the test thread stops the model while that action still runs
        CompletableFuture<Void> stopWhileABlocks = CompletableFuture.runAsync(() -> model.stop());

        // Then the stop, a call of the model, waits for that action and gives up with nothing changed
        assertThat(stopWhileABlocks).as("the stop made while the action of a that handed a call over blocks")
                .failsWithin(A_CALL_THAT_GIVES_UP)
                .withThrowableOfType(ExecutionException.class)
                .havingCause().isInstanceOf(IllegalStateException.class).withMessageContaining("Nothing was changed.");
        assertThat(model.isRunning()).as("whether the model runs after the stop that gave up").isTrue();
        assertThat(wrapped.isRunning()).as("whether the wrapped model runs after the stop that gave up").isTrue();

        // When the action of a returns, and the test thread stops the model again
        releaseA.countDown();
        CompletableFuture<Void> stopAfterAReturned = CompletableFuture.runAsync(() -> model.stop());

        // Then the stop runs
        assertThat(stopAfterAReturned).as("the stop made after the action of a returned").succeedsWithin(A_CALL_THAT_GIVES_UP);
        assertThat(model.isRunning()).as("whether the model runs after the stop that ran").isFalse();
        assertThat(wrapped.isRunning()).as("whether the wrapped model runs after the stop that ran").isFalse();

        // And a start leaves the model and the wrapped model running
        model.start(true);
        assertThat(model.isRunning()).as("whether the model runs after start(true)").isTrue();
        assertThat(wrapped.isRunning()).as("whether the wrapped model runs after start(true)").isTrue();
    }

    @Test
    @Timeout(60)
    void the_calls_an_action_handed_over_run_in_order_once_it_returns_although_another_subscription_was_resumed_meanwhile() throws Exception {
        // Given the action of d hands a resume and then a pause of d over and blocks, while the action of a blocks too
        CountDownLatch dHandedItsCallsOver = new CountDownLatch(1);
        CountDownLatch releaseD = new CountDownLatch(1);
        AtomicBoolean dIsInItsAction = new AtomicBoolean();
        List<CloudEvent> receivedByD = new CopyOnWriteArrayList<>();
        model.subscribe("D", StartAt.now(), event -> {
            if (dIsInItsAction.compareAndSet(false, true)) {
                model.resumeSubscription("D");
                model.pauseSubscription("D");
                dHandedItsCallsOver.countDown();
                HookedSubscriptionModel.awaitLatch(releaseD);
            } else {
                receivedByD.add(event);
            }
        });
        List<CloudEvent> receivedByB = new CopyOnWriteArrayList<>();
        model.subscribe("B", StartAt.now(), receivedByB::add);
        model.pauseSubscription("B");
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        assertThat(dHandedItsCallsOver.await(5, TimeUnit.SECONDS)).as("whether the action of d handed its calls over").isTrue();
        assertThat(aHandedACallOver.await(5, TimeUnit.SECONDS)).as("whether the action of a handed its call over").isTrue();

        // When b is resumed on another thread while both actions block
        assertThat(CompletableFuture.supplyAsync(() -> model.resumeSubscription("B"))).as("the resume of b made while the actions of a and d block").succeedsWithin(A_CALL_THAT_IS_NOT_HELD_UP);

        // Then the pause d handed over has not run
        assertThat(model.isPaused("D")).as("whether d is paused while its action blocks, before the pause it handed over has run").isFalse();

        // When the actions return
        releaseA.countDown();
        releaseD.countDown();

        // Then d ends up paused, as it does when its resume runs before its pause, and receives nothing written after
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(model.isPaused("D")).as("whether d is paused after the resume and the pause it handed over ran in that order").isTrue());
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        assertThat(receivedByD).as("what d received while paused").isEmpty();
    }

    @Test
    @Timeout(60)
    void a_stop_handed_over_by_an_action_that_returned_runs_while_the_action_of_another_subscription_that_handed_a_call_over_still_blocks() throws Exception {
        // Given the action of c stops the model once a has handed its resume over, and returns
        model.subscribe("C", StartAt.now(), event -> {
            HookedSubscriptionModel.awaitLatch(aHandedACallOver);
            model.stop();
        });

        // When an event reaches a and c
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));

        // Then the stop runs although the action of a has not returned
        await().atMost(A_CALL_THAT_IS_NOT_HELD_UP).untilAsserted(() ->
                assertThat(model.isRunning()).as("whether the model runs after the action of c handed a stop over and returned while the action of a blocks").isFalse());
        assertThat(wrapped.isRunning()).as("whether the wrapped model runs after the stop").isFalse();
        assertThat(releaseA.getCount()).as("whether the action of a still blocks").isEqualTo(1);
    }

    private static CloudEvent cloudEvent() {
        return CloudEventBuilder.v1().withId(UUID.randomUUID().toString()).withSource(URI.create("urn:test")).withType("test.event").withTime(OffsetDateTime.now()).build();
    }
}
