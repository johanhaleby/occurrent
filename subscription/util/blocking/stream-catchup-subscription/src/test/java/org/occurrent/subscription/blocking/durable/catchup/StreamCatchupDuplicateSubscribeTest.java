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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.subscription.blocking.durable.catchup.AbstractCatchupSubscriptionModel.LIFECYCLE_WAIT;

/**
 * A subscribe of an id the catch-up model already runs throws before it asks its start-position function, as in 0.34.0,
 * so a function that blocks holds up no call for the subscription that runs.
 */
@Timeout(60)
@DisplayName("StreamCatchupDuplicateSubscribe")
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupDuplicateSubscribeTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";
    private static final Duration BOUND = Duration.ofSeconds(30);

    private final CompletableFuture<Void> functionMayReturn = new CompletableFuture<>();
    private StreamCatchupSubscriptionModel catchup;

    @BeforeEach
    void run_the_id() {
        InMemorySubscriptionModel live = new InMemorySubscriptionModel();
        catchup = new StreamCatchupSubscriptionModel(new LiveStartLosingSubscriptionModel(live, false, false, () -> {
        }), new InMemoryEventStore(live), new CatchupSubscriptionModelConfig(100));
        assertThat(catchup.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), event -> {
        }).waitUntilStarted(Duration.ofSeconds(5))).as("the subscription that runs started").isTrue();
    }

    @AfterEach
    void let_the_function_return_and_shut_down() {
        functionMayReturn.complete(null);
        catchup.shutdown();
    }

    @Nested
    @DisplayName("when the id runs and another subscribe of it has a StartAt.dynamic function")
    class When_the_id_runs_and_another_subscribe_of_it_has_a_StartAt_dynamic_function {

        @Test
        @DisplayName("the subscribe throws DuplicateSubscriptionIdException without asking the function")
        void the_subscribe_throws_DuplicateSubscriptionIdException_without_asking_the_function() {
            // Given
            AtomicBoolean asked = new AtomicBoolean();

            // When
            Throwable thrown = catchThrowable(() -> catchup.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(() -> {
                asked.set(true);
                return StartAt.now();
            }), event -> {
            }));

            // Then
            assertAll(
                    () -> assertThat(thrown).as("what the subscribe threw").isInstanceOf(DuplicateSubscriptionIdException.class),
                    () -> assertThat(asked).as("the function was asked").isFalse()
            );
        }

        @Test
        @DisplayName("a pause of the id on another thread returns without waiting for the function")
        void a_pause_of_the_id_on_another_thread_returns_without_waiting_for_the_function() throws Exception {
            // Given
            CompletableFuture<Void> asked = new CompletableFuture<>();
            CompletableFuture<Void> duplicate = CompletableFuture.runAsync(() -> catchup.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(() -> {
                asked.complete(null);
                functionMayReturn.orTimeout(BOUND.toSeconds(), TimeUnit.SECONDS).join();
                return StartAt.now();
            }), event -> {
            }));
            // The function blocks once asked, so the pause comes either while it blocks or after the subscribe has thrown
            CompletableFuture.anyOf(asked, duplicate).handle((__, error) -> null).get(BOUND.toSeconds(), TimeUnit.SECONDS);

            // When
            CompletableFuture<Void> pause = CompletableFuture.runAsync(() -> catchup.pauseSubscription(SUBSCRIPTION_ID));

            // Then a pause that waits for the function throws IllegalStateException after LIFECYCLE_WAIT
            assertThat(pause).as("the pause").succeedsWithin(LIFECYCLE_WAIT.dividedBy(2));
            functionMayReturn.complete(null);
            assertAll(
                    () -> assertThat(duplicate).as("the other subscribe").failsWithin(BOUND)
                            .withThrowableThat().withCauseInstanceOf(DuplicateSubscriptionIdException.class),
                    () -> assertThat(catchup.isPaused(SUBSCRIPTION_ID)).as("the id is paused").isTrue()
            );
        }
    }
}
