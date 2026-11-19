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
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A cancel that comes while the replay stores a position waits for that store for as long as it takes, since the delete
 * of the stored position that follows would wait for the same storage, and an interrupt doesn't end the wait. Here the
 * store takes longer than the half second the cancel waits for an action of the replay.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class StreamCatchupCancelWaitsForAStoreUnderWayTest {

    private static final String ID = "subscription";
    // Longer than the 500 ms a bounded wait would give up after
    private static final long WELL_PAST_HALF_A_SECOND = 1200;

    private final KeepsTheLiveActionSubscriptionModel live = new KeepsTheLiveActionSubscriptionModel();
    private final CountDownLatch saveEntered = new CountDownLatch(1);
    private final CountDownLatch releaseTheSave = new CountDownLatch(1);
    private final AtomicBoolean blocked = new AtomicBoolean();
    private final AtomicLong saveReturnedAt = new AtomicLong();
    private final AtomicLong cancelReturnedAt = new AtomicLong();
    private final AtomicBoolean interruptStatusWhenTheCancelReturned = new AtomicBoolean();
    private final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage() {
        @Override
        public Checkpoint save(String id, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            if (blocked.compareAndSet(false, true)) {
                saveEntered.countDown();
                awaitTheRelease();
            }
            Checkpoint saved = super.save(id, checkpoint, condition);
            saveReturnedAt.set(System.nanoTime());
            return saved;
        }
    };
    private StreamCatchupSubscriptionModel catchup;

    @BeforeEach
    void a_replay_that_stores_a_position_after_its_first_event_with_a_store_that_is_held() throws InterruptedException {
        InMemoryEventStore eventStore = new InMemoryEventStore().withoutStreamPosition();
        eventStore.write("stream", List.of(event("event1"), event("event2")));
        catchup = new StreamCatchupSubscriptionModel(live, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        catchup.subscribe(ID, StartAtTime.beginningOfTime(), event -> {
        });
        assertThat(saveEntered.await(5, TimeUnit.SECONDS)).as("the replay is storing a position").isTrue();
    }

    @AfterEach
    void release_the_save_and_shut_down() {
        releaseTheSave.countDown();
        catchup.shutdown();
    }

    @Test
    void a_cancel_made_while_a_store_takes_longer_than_half_a_second_returns_only_after_the_store_has_returned_and_leaves_no_position_stored() throws InterruptedException {
        // Given
        Thread cancel = new Thread(this::cancelAndNoteWhenItReturned);

        // When the store is still under way well past half a second after the cancel began
        cancel.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> !cancel.isAlive() || cancel.getState() == Thread.State.WAITING);
        Thread.sleep(WELL_PAST_HALF_A_SECOND);
        boolean cancelStillWaiting = cancel.isAlive();
        releaseTheSave.countDown();
        cancel.join(5000);

        // Then
        assertAll(
                () -> assertThat(cancelStillWaiting).as("the cancel is still waiting while the store has not returned, well past half a second").isTrue(),
                () -> assertThat(cancel.isAlive()).as("the cancel returned once the store was released").isFalse(),
                () -> assertThat(saveReturnedAt.get()).as("when the store returned").isPositive().isLessThan(cancelReturnedAt.get()),
                () -> assertThat(storage.read(ID)).as("the position stored for the cancelled subscription").isNull());
    }

    @Test
    void an_interrupt_of_the_thread_that_cancels_does_not_end_the_wait_for_the_store_and_is_set_again_when_the_cancel_returns() throws InterruptedException {
        // Given
        Thread cancel = new Thread(this::cancelAndNoteWhenItReturned);
        cancel.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> !cancel.isAlive() || cancel.getState() == Thread.State.WAITING);

        // When the cancelling thread is interrupted during the wait, which then goes on for well past half a second
        cancel.interrupt();
        Thread.sleep(WELL_PAST_HALF_A_SECOND);
        boolean cancelStillWaiting = cancel.isAlive();
        releaseTheSave.countDown();
        cancel.join(5000);

        // Then
        assertAll(
                () -> assertThat(cancelStillWaiting).as("the cancel is still waiting for the store after the interrupt, well past half a second").isTrue(),
                () -> assertThat(cancel.isAlive()).as("the cancel returned once the store was released").isFalse(),
                () -> assertThat(saveReturnedAt.get()).as("when the store returned").isPositive().isLessThan(cancelReturnedAt.get()),
                () -> assertThat(interruptStatusWhenTheCancelReturned.get()).as("the interrupt status of the thread when the cancel returned").isTrue());
    }

    private void cancelAndNoteWhenItReturned() {
        catchup.cancelSubscription(ID);
        cancelReturnedAt.set(System.nanoTime());
        interruptStatusWhenTheCancelReturned.set(Thread.currentThread().isInterrupted());
    }

    private void awaitTheRelease() {
        try {
            releaseTheSave.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("Tested").withTime(OffsetDateTime.now()).build();
    }
}
