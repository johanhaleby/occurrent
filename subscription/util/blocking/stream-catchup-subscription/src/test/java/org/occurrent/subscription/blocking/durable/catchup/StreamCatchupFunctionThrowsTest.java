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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.subscription.blocking.durable.catchup.AbstractCatchupSubscriptionModel.LIFECYCLE_WAIT;

/**
 * A subscribe whose start-position function throws doesn't hold up a later call for the id on another thread.
 */
@Timeout(60)
@DisplayName("StreamCatchupFunctionThrows")
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupFunctionThrowsTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";
    private static final Duration BOUND = Duration.ofSeconds(30);
    // A new thread for each call, so no call runs on the thread whose subscribe asked the function
    private static final Executor NEW_THREAD = runnable -> new Thread(runnable).start();

    private StreamCatchupSubscriptionModel catchup;

    @AfterEach
    void shut_down() {
        if (catchup != null) {
            catchup.shutdown();
        }
    }

    @Nested
    @DisplayName("when the function of a subscribe on another thread throws")
    class When_the_function_of_a_subscribe_on_another_thread_throws {

        @Test
        @DisplayName("a pause of the id once it runs returns without waiting for that subscribe")
        void a_pause_of_the_id_once_it_runs_returns_without_waiting_for_that_subscribe() {
            // Given
            InMemorySubscriptionModel live = new InMemorySubscriptionModel();
            catchup = new StreamCatchupSubscriptionModel(new LiveStartLosingSubscriptionModel(live, false, false, () -> {
            }), new InMemoryEventStore(live), new CatchupSubscriptionModelConfig(100));
            CompletableFuture<Void> throwing = CompletableFuture.runAsync(() -> catchup.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(() -> {
                throw new IllegalStateException("the function failed");
            }), event -> {
            }), NEW_THREAD);
            assertThat(throwing).as("the subscribe whose function throws").failsWithin(BOUND)
                    .withThrowableThat().withStackTraceContaining("the function failed");
            assertThat(catchup.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), event -> {
            }).waitUntilStarted(Duration.ofSeconds(5))).as("the subscription that runs started").isTrue();

            // When
            CompletableFuture<Void> pause = CompletableFuture.runAsync(() -> catchup.pauseSubscription(SUBSCRIPTION_ID), NEW_THREAD);

            // Then a pause that waits for the subscribe whose function threw throws IllegalStateException after LIFECYCLE_WAIT
            assertThat(pause).as("the pause").succeedsWithin(LIFECYCLE_WAIT.dividedBy(2));
            assertThat(catchup.isPaused(SUBSCRIPTION_ID)).as("the id is paused").isTrue();
        }
    }
}
