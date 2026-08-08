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

package org.occurrent.dsl.projection;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class AppliedPositionStorageTest {

    @Test
    void appliedPosition_is_empty_for_a_projection_that_has_never_advanced() {
        AppliedPositionStorage storage = AppliedPositionStorage.inMemory();

        assertThat(storage.appliedPosition("orders")).isEmpty();
    }

    @Test
    void advance_records_the_position_and_appliedPosition_reads_it_back() {
        AppliedPositionStorage storage = AppliedPositionStorage.inMemory();

        storage.advance("orders", 42);

        assertThat(storage.appliedPosition("orders")).hasValue(42L);
    }

    @Test
    void advance_never_moves_the_recorded_position_backwards() {
        AppliedPositionStorage storage = AppliedPositionStorage.inMemory();

        storage.advance("orders", 50);
        storage.advance("orders", 10);

        assertThat(storage.appliedPosition("orders")).hasValue(50L);
    }

    @Test
    void advance_rejects_a_non_positive_position() {
        AppliedPositionStorage storage = AppliedPositionStorage.inMemory();

        assertThat(org.assertj.core.api.Assertions.catchThrowable(() -> storage.advance("orders", 0)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void waitUntilApplied_returns_true_immediately_when_the_position_is_already_applied() {
        AppliedPositionStorage storage = AppliedPositionStorage.inMemory();
        storage.advance("orders", 42);

        boolean caughtUp = storage.waitUntilApplied("orders", 42, Duration.ofSeconds(5));

        assertThat(caughtUp).isTrue();
    }

    @Test
    void waitUntilApplied_returns_true_once_a_position_at_or_beyond_the_requested_one_is_advanced_to() {
        AppliedPositionStorage storage = AppliedPositionStorage.inMemory();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            scheduler.schedule(() -> storage.advance("orders", 42), 50, TimeUnit.MILLISECONDS);

            boolean caughtUp = storage.waitUntilApplied("orders", 42, Duration.ofSeconds(5), Duration.ofMillis(10));

            assertThat(caughtUp).isTrue();
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void waitUntilApplied_returns_false_on_timeout_rather_than_throwing_when_the_position_never_arrives() {
        AppliedPositionStorage storage = AppliedPositionStorage.inMemory();

        boolean caughtUp = storage.waitUntilApplied("orders", 42, Duration.ofMillis(100), Duration.ofMillis(10));

        assertThat(caughtUp).isFalse();
    }

    @Test
    void waitUntilApplied_returns_true_for_a_position_lower_than_the_one_already_applied() {
        AppliedPositionStorage storage = AppliedPositionStorage.inMemory();
        storage.advance("orders", 100);

        boolean caughtUp = storage.waitUntilApplied("orders", 42, Duration.ofSeconds(5));

        assertThat(caughtUp).isTrue();
    }

    @Test
    void an_interrupted_wait_returns_false_and_restores_the_interrupt_flag() throws InterruptedException {
        AppliedPositionStorage storage = AppliedPositionStorage.inMemory();
        CountDownLatch started = new CountDownLatch(1);
        boolean[] result = new boolean[1];
        boolean[] interruptedAfterwards = new boolean[1];
        Thread waiter = new Thread(() -> {
            started.countDown();
            result[0] = storage.waitUntilApplied("orders", 42, Duration.ofSeconds(30), Duration.ofSeconds(30));
            interruptedAfterwards[0] = Thread.currentThread().isInterrupted();
        });
        waiter.start();
        started.await();
        Thread.sleep(50);
        waiter.interrupt();
        waiter.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(waiter.isAlive()).isFalse();
        assertThat(result[0]).isFalse();
        assertThat(interruptedAfterwards[0]).isTrue();
    }
}
