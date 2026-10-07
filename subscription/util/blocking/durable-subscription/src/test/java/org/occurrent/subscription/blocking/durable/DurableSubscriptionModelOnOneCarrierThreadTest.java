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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.subscription.util.predicate.EveryN.everyEvent;

/**
 * Runs in a JVM of its own whose virtual threads share one carrier thread, so a virtual thread that waits for the
 * checkpoint store while it holds a monitor stops every other virtual thread. On a JDK where a monitor no longer keeps
 * the carrier thread, these tests pass whether or not a monitor is held.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelOnOneCarrierThreadTest {

    private static final Duration INTERVAL = Duration.ofMillis(200);
    private static final Checkpoint START = new StringBasedCheckpoint("start");
    private static final long SLOW_MILLIS = 1500;
    private static final long OTHER_STARTS_AFTER_MILLIS = 200;

    private final QuietPositionReportingModel wrapped = new QuietPositionReportingModel();
    private final SlowStorage storage = new SlowStorage();

    @BeforeAll
    static void virtual_threads_share_one_carrier_thread() {
        assertThat(System.getProperty("jdk.virtualThreadScheduler.parallelism")).as("jdk.virtualThreadScheduler.parallelism").isEqualTo("1");
    }

    @Test
    void a_slow_checkpoint_save_for_an_event_does_not_hold_up_the_delivery_to_another_subscription() throws InterruptedException {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(everyEvent()));
        model.subscribe("a", null, StartAt.checkpoint(START), __ -> {
        });
        model.subscribe("b", null, StartAt.checkpoint(START), __ -> {
        });
        storage.slow.set(true);

        // When
        long millisForTheOther = millisForTheOtherWhile(() -> wrapped.deliver("a", new StringBasedCheckpoint("a1")));

        // Then
        assertThat(millisForTheOther).as("delivery to b while the checkpoint save for a is slow").isLessThan(500L);
    }

    @Test
    void a_slow_save_of_a_quiet_position_does_not_hold_up_the_delivery_to_another_subscription() throws InterruptedException {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(everyEvent()).saveQuietPositionEvery(INTERVAL));
        // A position stored for a, which is what lets its quiet position be saved before its first event
        storage.save("a", START);
        model.subscribe("a", null, StartAt.subscriptionModelDefault(), __ -> {
        });
        model.subscribe("b", null, StartAt.checkpoint(START), __ -> {
        });
        Thread.sleep(INTERVAL.toMillis() + 50);
        storage.slow.set(true);

        // When
        long millisForTheOther = millisForTheOtherWhile(() -> wrapped.readNothing("a", new StringBasedCheckpoint("quiet")));

        // Then
        assertThat(storage.read("a")).as("checkpoint of a once the slow save of its quiet position returned").isEqualTo(new StringBasedCheckpoint("quiet"));
        assertThat(millisForTheOther).as("delivery to b while the save of a's quiet position is slow").isLessThan(500L);
    }

    @Test
    void a_slow_action_does_not_hold_up_the_delivery_to_another_subscription() throws InterruptedException {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(everyEvent()));
        model.subscribe("a", null, StartAt.checkpoint(START), __ -> {
            try {
                Thread.sleep(SLOW_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        model.subscribe("b", null, StartAt.checkpoint(START), __ -> {
        });

        // When
        long millisForTheOther = millisForTheOtherWhile(() -> wrapped.deliver("a", new StringBasedCheckpoint("a1")));

        // Then
        assertThat(millisForTheOther).as("delivery to b while a's action is slow").isLessThan(500L);
    }

    // Runs the slow call for a on one virtual thread, then delivers to b on another, and returns how long b took from
    // the moment its thread was started
    private long millisForTheOtherWhile(Runnable slowCallForA) throws InterruptedException {
        Thread slow = Thread.ofVirtual().start(slowCallForA);
        Thread.sleep(OTHER_STARTS_AFTER_MILLIS);
        long[] finishedAt = new long[1];
        long startedAt = System.nanoTime();
        Thread other = Thread.ofVirtual().start(() -> {
            wrapped.deliver("b", new StringBasedCheckpoint("b1"));
            finishedAt[0] = System.nanoTime();
        });
        other.join();
        slow.join();
        return Duration.ofNanos(finishedAt[0] - startedAt).toMillis();
    }

    private static final class SlowStorage extends InMemoryCheckpointStorage {
        final AtomicBoolean slow = new AtomicBoolean();

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            if (slow.get() && subscriptionId.equals("a")) {
                try {
                    Thread.sleep(SLOW_MILLIS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return super.save(subscriptionId, checkpoint, condition);
        }
    }
}
