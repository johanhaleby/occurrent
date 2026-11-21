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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.StartAt;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A subscribe of an id makes each live call of the new subscription wait for the calls of the subscription that the
 * record it replaces, or that it put and removed again, still has in the subscriber's action, so the two actions never
 * run at once. The record a cancel whose live delegate threw kept, and the record of a subscribe whose live delegate
 * threw, are two such records.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class StreamCatchupLiveCallsOfAForgottenRecordTest {

    private static final String ID = "live";

    private final KeepsTheLiveActionSubscriptionModel live = new KeepsTheLiveActionSubscriptionModel();
    private final List<String> calls = new CopyOnWriteArrayList<>();
    private final CountDownLatch firstEntered = new CountDownLatch(1);
    private final CountDownLatch releaseTheFirst = new CountDownLatch(1);
    private final List<Thread> threads = new CopyOnWriteArrayList<>();
    private final StreamCatchupSubscriptionModel catchup = new StreamCatchupSubscriptionModel(live, new InMemoryEventStore(), new CatchupSubscriptionModelConfig(100));

    @AfterEach
    void release_the_first_call_and_shut_down() throws InterruptedException {
        releaseTheFirst.countDown();
        for (Thread thread : threads) {
            thread.join(5000);
        }
        catchup.shutdown();
    }

    @Test
    void a_subscribe_after_a_cancel_whose_live_delegate_threw_waits_for_the_live_call_the_kept_subscription_has_in_its_action() throws InterruptedException {
        // Given a live call in the action of the subscription, and a cancel that throws, so the subscription is kept
        catchup.subscribe(ID, null, StartAt.now(), theFirstAction());
        startALiveCall();
        assertThat(firstEntered.await(5, TimeUnit.SECONDS)).as("the live call is in the action of the first subscription").isTrue();
        live.failTheNextCancel();
        assertThatThrownBy(() -> catchup.cancelSubscription(ID)).as("the cancel").isInstanceOf(IllegalStateException.class);

        // When the id is subscribed again and the new subscription gets a live call while the first call is still in its action
        catchup.subscribe(ID, null, StartAt.now(), theSecondAction());
        Thread second = startALiveCall();
        awaitUntilTheSecondCallEitherReachedItsActionOrWaits(second);
        boolean secondInTheActionBesideTheFirst = calls.contains("second enters");
        releaseTheFirst.countDown();
        second.join(5000);

        // Then
        assertAll(
                () -> assertThat(secondInTheActionBesideTheFirst).as("the second subscription's action was called while the first call was still in the action of the first").isFalse(),
                () -> assertThat(calls).as("the calls of the actions in order").containsExactly("first enters", "first returns", "second enters"));
    }

    @Test
    void a_subscribe_after_a_subscribe_whose_live_delegate_threw_waits_for_the_live_call_the_failed_subscription_has_in_its_action() throws InterruptedException {
        // Given a live delegate that makes a live call into the action while it subscribes, and then throws
        live.whenSubscribing(action -> {
            startALiveCall();
            awaitTheFirstCall();
            throw new IllegalStateException("The live delegate failed to subscribe");
        });
        assertThatThrownBy(() -> catchup.subscribe(ID, null, StartAt.now(), theFirstAction())).as("the subscribe").isInstanceOf(IllegalStateException.class);

        // When the id is subscribed again and the new subscription gets a live call while the first call is still in its action
        catchup.subscribe(ID, null, StartAt.now(), theSecondAction());
        Thread second = startALiveCall();
        awaitUntilTheSecondCallEitherReachedItsActionOrWaits(second);
        boolean secondInTheActionBesideTheFirst = calls.contains("second enters");
        releaseTheFirst.countDown();
        second.join(5000);

        // Then
        assertAll(
                () -> assertThat(secondInTheActionBesideTheFirst).as("the second subscription's action was called while the first call was still in the action of the first").isFalse(),
                () -> assertThat(calls).as("the calls of the actions in order").containsExactly("first enters", "first returns", "second enters"));
    }

    // Enters, and stays in until the test releases it
    private Consumer<CloudEvent> theFirstAction() {
        return event -> {
            calls.add("first enters");
            firstEntered.countDown();
            try {
                releaseTheFirst.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            calls.add("first returns");
        };
    }

    private Consumer<CloudEvent> theSecondAction() {
        return event -> calls.add("second enters");
    }

    private Thread startALiveCall() {
        Thread thread = new Thread(() -> live.callTheKeptAction(ID));
        threads.add(thread);
        thread.start();
        return thread;
    }

    private void awaitTheFirstCall() {
        try {
            assertThat(firstEntered.await(5, TimeUnit.SECONDS)).as("the live call is in the action of the first subscription").isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // A call that waits for the first one polls, so it is timed waiting. One that is let through reaches its action at once
    private void awaitUntilTheSecondCallEitherReachedItsActionOrWaits(Thread second) {
        await().atMost(Duration.ofSeconds(5)).until(() -> calls.contains("second enters") || !second.isAlive() || second.getState() == Thread.State.TIMED_WAITING);
    }
}
