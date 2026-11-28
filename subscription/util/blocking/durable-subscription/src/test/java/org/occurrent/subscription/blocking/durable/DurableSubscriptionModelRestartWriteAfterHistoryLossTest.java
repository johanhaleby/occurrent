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

package org.occurrent.subscription.blocking.durable;

import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.time.Duration;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.subscription.util.predicate.EveryN.everyEvent;

/**
 * The write of the position a subscription restarts from after its history was lost, made through the listener the
 * wrapped model calls. When another node has written with a newer lease, the storage refuses the write and the listener
 * returns, but the model no longer knows what is stored, so the next quiet position reads it before it writes.
 * When the write throws, it may have written, so it reaches the caller of the listener, the delivery of an event read
 * before the restart stores nothing over it, and a quiet position is saved again once a read finds the restart position
 * stored.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class DurableSubscriptionModelRestartWriteAfterHistoryLossTest {

    private static final String SUBSCRIPTION_ID = "subscription";
    private static final Checkpoint START = new StringBasedCheckpoint("start");
    private static final Checkpoint EVENT = new StringBasedCheckpoint("event");
    private static final Checkpoint RESTARTED_FROM = new StringBasedCheckpoint("restarted-from");
    private static final Checkpoint STORED_BY_ANOTHER_NODE = new StringBasedCheckpoint("stored-by-another-node");
    private static final Checkpoint QUIET = new StringBasedCheckpoint("quiet");

    private final QuietPositionReportingModel wrapped = new QuietPositionReportingModel();
    private final RestartWriteStorage storage = new RestartWriteStorage();
    private final CountDownLatch handlerStarted = new CountDownLatch(1);
    private final CountDownLatch handlerMayReturn = new CountDownLatch(1);
    private final ExecutorService runThreads = Executors.newCachedThreadPool();

    @AfterEach
    void releaseTheHandlerAndStopItsThreads() {
        handlerMayReturn.countDown();
        runThreads.shutdownNow();
    }

    @Test
    void a_restart_position_write_the_storage_refuses_is_not_thrown_to_the_listener_and_the_next_quiet_position_is_not_saved_over_what_the_other_node_stored() {
        // Given
        subscribe(new AtomicBoolean(false));
        wrapped.deliver(SUBSCRIPTION_ID, EVENT);
        storage.refuseTheRestartWriteAfterAnotherNodeStored = true;

        // When
        Throwable thrownToTheListener = catchThrowable(this::restartingAfterHistoryLoss);
        Checkpoint storedAfterTheRestart = storage.read(SUBSCRIPTION_ID);
        boolean quietPositionWanted = saveAQuietPositionWhenWanted(QUIET);

        // Then
        assertAll(
                () -> assertThat(thrownToTheListener).as("thrown to the listener").isNull(),
                () -> assertThat(quietPositionWanted).as("quiet position wanted after the restart write was refused").isTrue(),
                () -> assertThat(storedAfterTheRestart).as("checkpoint stored after the restart write was refused").isEqualTo(STORED_BY_ANOTHER_NODE),
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored after a quiet position").isEqualTo(STORED_BY_ANOTHER_NODE)
        );
    }

    @Test
    void a_restart_position_write_that_throws_reaches_the_listener_refuses_the_delivery_of_an_earlier_read_and_has_a_quiet_position_saved_once_a_read_finds_the_position_stored() throws Exception {
        // Given
        subscribe(new AtomicBoolean(true));
        CompletableFuture<Void> delivery = CompletableFuture.runAsync(() -> {
            wrapped.beforeReading(SUBSCRIPTION_ID);
            wrapped.deliver(SUBSCRIPTION_ID, EVENT);
        }, runThreads);
        assertThat(awaitQuietly(handlerStarted)).as("handler started").isTrue();
        storage.throwAfterTheRestartWrote = true;

        // When
        Throwable thrownToTheListener = catchThrowable(this::restartingAfterHistoryLoss);
        handlerMayReturn.countDown();
        delivery.get(10, SECONDS);
        Checkpoint storedAfterTheDelivery = storage.read(SUBSCRIPTION_ID);
        boolean quietPositionWanted = saveAQuietPositionWhenWanted(QUIET);

        // Then
        assertAll(
                () -> assertThat(thrownToTheListener).as("thrown to the listener").isNotNull().isSameAs(storage.thrown.get()),
                () -> assertThat(storedAfterTheDelivery).as("checkpoint stored after the delivery of an event read before the restart").isEqualTo(RESTARTED_FROM),
                () -> assertThat(quietPositionWanted).as("quiet position wanted once a read found the restart position stored").isTrue(),
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored after a quiet position once a read found the restart position stored").isEqualTo(QUIET)
        );
    }

    private void subscribe(AtomicBoolean holdTheHandler) {
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(everyEvent()).saveQuietPositionEvery(Duration.ofMillis(1)));
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(START), cloudEvent -> {
            if (holdTheHandler.get()) {
                handlerStarted.countDown();
                awaitQuietly(handlerMayReturn);
            }
        });
    }

    private void restartingAfterHistoryLoss() {
        wrapped.historyLossListeners.getFirst().restartingAfterHistoryLoss(SUBSCRIPTION_ID, RESTARTED_FROM, () -> true);
    }

    // Reads until a listener wants the quiet position, which the 1 ms interval allows after a moment. False when none
    // does within 2 seconds
    private boolean saveAQuietPositionWhenWanted(Checkpoint quietPosition) {
        AtomicReference<Consumer<Checkpoint>> saver = new AtomicReference<>();
        try {
            await().atMost(2, SECONDS).until(() -> {
                saver.set(wrapped.beforeReading(SUBSCRIPTION_ID));
                return saver.get() != null;
            });
        } catch (ConditionTimeoutException e) {
            return false;
        }
        saver.get().accept(quietPosition);
        return true;
    }

    private static boolean awaitQuietly(CountDownLatch latch) {
        try {
            return latch.await(10, SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static final class RestartWriteStorage extends InMemoryCheckpointStorage {
        volatile boolean refuseTheRestartWriteAfterAnotherNodeStored;
        volatile boolean throwAfterTheRestartWrote;
        final AtomicReference<RuntimeException> thrown = new AtomicReference<>();

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            if (checkpoint.equals(RESTARTED_FROM)) {
                if (refuseTheRestartWriteAfterAnotherNodeStored) {
                    super.save(subscriptionId, STORED_BY_ANOTHER_NODE, CheckpointWriteCondition.any());
                    throw new CheckpointWriteConditionNotFulfilledException(subscriptionId, OptionalLong.empty(), condition);
                }
                if (throwAfterTheRestartWrote) {
                    super.save(subscriptionId, checkpoint, condition);
                    RuntimeException failure = new IllegalStateException("the write of the restart position failed");
                    thrown.set(failure);
                    throw failure;
                }
            }
            return super.save(subscriptionId, checkpoint, condition);
        }
    }
}
