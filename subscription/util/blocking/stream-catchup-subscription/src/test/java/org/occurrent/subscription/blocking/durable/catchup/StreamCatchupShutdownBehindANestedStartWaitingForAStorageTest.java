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
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A {@code start(true)} nested inside another start reads the checkpoint of each subscription the live delegate holds
 * paused on the thread that holds the model lock, and a checkpoint storage that doesn't answer for one subscription holds
 * that thread for good. A {@code shutdown()} from another thread waits for that lock, shuts the live delegate down while it
 * waits, and gives up after {@link AbstractCatchupSubscriptionModel#LIFECYCLE_WAIT} by throwing, as it does behind code a
 * caller supplied, instead of waiting for a storage nobody bounds.
 * <p>
 * Each test asserts with a bounded wait on a shutdown run off the test thread, so a shutdown that never returns fails on
 * its assertion rather than on a timeout.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupShutdownBehindANestedStartWaitingForAStorageTest {

    private static final List<String> IDS = List.of("first", "second");
    private static final String NOT_ANSWERED_FOR = "second";

    private InMemorySubscriptionModel inMemory;
    private HookedSubscriptionModel wrapped;
    private AnswersNothingForOneSubscription storage;
    private StreamCatchupSubscriptionModel model;
    private final AtomicBoolean acted = new AtomicBoolean();
    private final AtomicReference<@Nullable Throwable> shutdownThrew = new AtomicReference<>();
    private final CountDownLatch shutdownReturned = new CountDownLatch(1);
    private volatile long shutdownTookNanos;

    @BeforeEach
    void create_a_model_over_a_wrapped_model_with_a_checkpoint_storage_that_answers_nothing_for_one_subscription() {
        inMemory = new InMemorySubscriptionModel();
        wrapped = new HookedSubscriptionModel(inMemory);
        storage = new AnswersNothingForOneSubscription();
        model = new StreamCatchupSubscriptionModel(wrapped, new InMemoryEventStore(inMemory).withoutStreamPosition(),
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
    }

    @AfterEach
    void let_the_storage_answer_and_shut_down_without_waiting_for_a_start_that_never_returned() {
        storage.answer();
        shutDownOnADaemonThread(model::shutdown);
        shutDownOnADaemonThread(inMemory::shutdown);
    }

    @Test
    @Timeout(60)
    void a_shutdown_gives_up_behind_a_start_nested_in_a_start_at_function_whose_checkpoint_read_does_not_return_and_has_shut_down_the_live_delegate() throws Exception {
        // Given two paused subscriptions in a stopped model, the first of which has a function that stops the model and starts it
        givenTwoPausedSubscriptionsInAStoppedModel(() -> {
            if (acted.compareAndSet(false, true)) {
                model.stop();
                model.start(true);
            }
        });
        startOffTheTestThread();
        assertThat(storage.readEntered.await(15, TimeUnit.SECONDS)).as("whether the nested start waits for the checkpoint of " + NOT_ANSWERED_FOR).isTrue();

        // When
        shutDownOffTheTestThread();

        // Then
        assertTheShutdownGaveUpAndShutTheLiveDelegateDown();
    }

    @Test
    @Timeout(60)
    void a_shutdown_gives_up_behind_a_start_nested_in_the_start_of_the_live_delegate_whose_checkpoint_read_does_not_return_and_has_shut_down_the_live_delegate() throws Exception {
        // Given two paused subscriptions in a stopped model, and a live delegate whose start starts the model
        givenTwoPausedSubscriptionsInAStoppedModel(() -> {
        });
        wrapped.beforeStart = () -> {
            if (acted.compareAndSet(false, true)) {
                model.start(true);
            }
        };
        startOffTheTestThread();
        assertThat(storage.readEntered.await(15, TimeUnit.SECONDS)).as("whether the nested start waits for the checkpoint of " + NOT_ANSWERED_FOR).isTrue();

        // When
        shutDownOffTheTestThread();

        // Then
        assertTheShutdownGaveUpAndShutTheLiveDelegateDown();
    }

    private void givenTwoPausedSubscriptionsInAStoppedModel(Runnable onAskedByTheStart) {
        AtomicBoolean armed = new AtomicBoolean();
        for (String id : IDS) {
            model.subscribe(id, null, StartAt.dynamic(() -> {
                if (armed.get() && id.equals("first")) {
                    onAskedByTheStart.run();
                }
                return null;
            }), event -> {
            });
            model.pauseSubscription(id);
            assertThat(model.isPaused(id)).as("whether " + id + " is paused before the start").isTrue();
        }
        model.stop();
        storage.doesNotAnswerForTheSecondSubscription();
        armed.set(true);
    }

    private void startOffTheTestThread() {
        CompletableFuture.runAsync(() -> {
            try {
                model.start(true);
            } catch (Throwable e) {
                // A start whose nested start the shutdown gave up on may fail once the storage answers
            }
        }, daemonExecutor("outer-start"));
    }

    private void shutDownOffTheTestThread() {
        CompletableFuture.runAsync(() -> {
            long before = System.nanoTime();
            try {
                model.shutdown();
            } catch (Throwable e) {
                shutdownThrew.set(e);
            } finally {
                shutdownTookNanos = System.nanoTime() - before;
                shutdownReturned.countDown();
            }
        }, daemonExecutor("shutdown-under-test"));
    }

    private void assertTheShutdownGaveUpAndShutTheLiveDelegateDown() throws InterruptedException {
        // The wait is 10 seconds, so a shutdown that has not returned after twice that waits for the storage for good
        assertThat(shutdownReturned.await(25, TimeUnit.SECONDS)).as("whether shutdown() returned while the nested start waited for a checkpoint storage that doesn't answer").isTrue();
        assertThat(shutdownThrew.get()).as("what shutdown() threw").isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(AbstractCatchupSubscriptionModel.SHUT_DOWN_PAST_A_WAIT);
        assertThat(Duration.ofNanos(shutdownTookNanos)).as("how long shutdown() took").isLessThan(Duration.ofSeconds(20));
        assertThat(wrapped.shutdowns.get()).as("the shutdowns of the live delegate").isPositive();
    }

    // Reads of the checkpoint of one subscription wait until the test lets them answer, but not those a start makes ahead of its turns on threads of their own, so the read that waits is the nested start's, made on the thread that holds the model lock
    private static final class AnswersNothingForOneSubscription extends InMemoryCheckpointStorage {
        private final CountDownLatch readEntered = new CountDownLatch(1);
        private final CountDownLatch answers = new CountDownLatch(1);
        private volatile boolean waits;

        void doesNotAnswerForTheSecondSubscription() {
            waits = true;
        }

        void answer() {
            answers.countDown();
        }

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            if (waits && NOT_ANSWERED_FOR.equals(subscriptionId) && !Thread.currentThread().isVirtual()) {
                readEntered.countDown();
                try {
                    answers.await(60, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return super.read(subscriptionId);
        }
    }

    private static Executor daemonExecutor(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            thread.start();
        };
    }

    private static void shutDownOnADaemonThread(Runnable shutdown) {
        try {
            CompletableFuture.runAsync(shutdown, daemonExecutor("shutdown")).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            // A start that never returned holds the lock, and the daemon threads end with the test JVM
        }
    }
}
