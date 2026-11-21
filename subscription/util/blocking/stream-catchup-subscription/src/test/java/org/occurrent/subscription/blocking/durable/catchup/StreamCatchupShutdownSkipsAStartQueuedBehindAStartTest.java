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

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.HookedSubscriptionModel.awaitLatch;

/**
 * A wrapped model whose {@code start(..)} returns only once a {@code shutdown()} that came after that start began
 * ends it. A start that waits for the model lock behind a stuck start doesn't start the wrapped model once a
 * {@code shutdown()} is under way, so the shutdown can take the lock.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupShutdownSkipsAStartQueuedBehindAStartTest {

    private InMemorySubscriptionModel inMemory;
    private HookedSubscriptionModel wrapped;
    private StreamCatchupSubscriptionModel model;
    private final CountDownLatch firstStartEntered = new CountDownLatch(1);
    private final AtomicReference<@Nullable CountDownLatch> endsTheLatestStart = new AtomicReference<>();
    private final List<CallOnAThread> calls = new ArrayList<>();

    @BeforeEach
    void create_a_model_over_a_wrapped_model_whose_start_returns_only_once_a_later_shutdown_ends_it() {
        inMemory = new InMemorySubscriptionModel();
        wrapped = new HookedSubscriptionModel(inMemory);
        wrapped.beforeStart = () -> {
            CountDownLatch shutdownCameAfterThisStartBegan = new CountDownLatch(1);
            endsTheLatestStart.set(shutdownCameAfterThisStartBegan);
            firstStartEntered.countDown();
            awaitLatch(shutdownCameAfterThisStartBegan);
        };
        wrapped.beforeShutdown = () -> {
            CountDownLatch endsIt = endsTheLatestStart.get();
            if (endsIt != null) {
                endsIt.countDown();
            }
        };
        model = new StreamCatchupSubscriptionModel(wrapped, new InMemoryEventStore(inMemory).withoutStreamPosition(), new CatchupSubscriptionModelConfig(100));
    }

    @AfterEach
    void release_everything_and_shut_down() {
        try {
            CountDownLatch endsIt = endsTheLatestStart.get();
            if (endsIt != null) {
                endsIt.countDown();
            }
            calls.forEach(CallOnAThread::awaitEnded);
        } finally {
            model.shutdown();
            inMemory.shutdown();
        }
    }

    @Test
    @Timeout(40)
    void a_shutdown_returns_while_a_start_is_queued_behind_a_start_stuck_in_the_wrapped_start_and_the_queued_start_never_enters_the_wrapped_start() throws InterruptedException {
        // Given
        model.stop();
        calls.add(new CallOnAThread(() -> model.start(false)));
        assertThat(firstStartEntered.await(5, TimeUnit.SECONDS)).as("the first start is inside the wrapped model's start").isTrue();
        CallOnAThread queuedStart = new CallOnAThread(() -> model.start(false));
        calls.add(queuedStart);
        queuedStart.awaitReturnedOrWaiting();

        // When
        CallOnAThread shutdown = new CallOnAThread(model::shutdown);
        calls.add(shutdown);

        // Then
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(shutdown.hasReturned()).as("whether the shutdown returned within 5 seconds while the first start was stuck and a second start was queued behind it").isTrue());
        calls.forEach(CallOnAThread::awaitEnded);
        assertThat(wrapped.starts.get()).as("the number of calls to the wrapped model's start").isEqualTo(1);
    }
}
