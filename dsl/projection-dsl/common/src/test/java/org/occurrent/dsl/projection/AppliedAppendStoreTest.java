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
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.retry.Backoff;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

@DisplayNameGeneration(ReplaceUnderscores.class)
class AppliedAppendStoreTest {

    @Test
    void hasApplied_is_false_for_an_append_that_was_never_recorded() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();

        assertThat(store.hasApplied("orders", AppendId.mint())).isFalse();
    }

    @Test
    void recordApplied_makes_hasApplied_true() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppendId appendId = AppendId.mint();

        store.recordApplied("orders", appendId);

        assertThat(store.hasApplied("orders", appendId)).isTrue();
    }

    @Test
    void recording_the_same_append_twice_is_a_no_op() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppendId appendId = AppendId.mint();

        store.recordApplied("orders", appendId);
        store.recordApplied("orders", appendId);

        assertThat(store.hasApplied("orders", appendId)).isTrue();
    }

    @Test
    void recording_is_scoped_per_projection_id() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppendId appendId = AppendId.mint();

        store.recordApplied("orders", appendId);

        assertThat(store.hasApplied("shipments", appendId)).isFalse();
    }

    @Test
    void clear_removes_every_append_recorded_for_a_projection() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppendId first = AppendId.mint();
        AppendId second = AppendId.mint();
        store.recordApplied("orders", first);
        store.recordApplied("orders", second);

        store.clear("orders");

        assertThat(store.hasApplied("orders", first)).isFalse();
        assertThat(store.hasApplied("orders", second)).isFalse();
    }

    @Test
    void clear_does_not_affect_another_projection() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppendId appendId = AppendId.mint();
        store.recordApplied("orders", appendId);
        store.recordApplied("shipments", appendId);

        store.clear("orders");

        assertThat(store.hasApplied("shipments", appendId)).isTrue();
    }

    @Test
    void waitUntilApplied_rejects_a_backoff_of_none_because_that_would_poll_the_store_in_a_busy_loop() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();

        assertThat(catchThrowable(() -> store.waitUntilApplied("orders", AppendId.mint(), Duration.ofMillis(50), Backoff.none())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Backoff.none()");
    }

    @Test
    void an_exponential_backoff_grows_the_interval_between_polls_and_stops_growing_at_its_max() {
        List<Long> pollGapsMillis = new ArrayList<>();
        long[] previousPoll = {System.nanoTime()};
        AppliedAppendStore neverApplied = new AppliedAppendStore() {
            @Override
            public void recordApplied(String projectionId, AppendId appendId) {
            }

            @Override
            public boolean hasApplied(String projectionId, AppendId appendId) {
                long now = System.nanoTime();
                pollGapsMillis.add(Duration.ofNanos(now - previousPoll[0]).toMillis());
                previousPoll[0] = now;
                return false;
            }

            @Override
            public void clear(String projectionId) {
            }
        };

        neverApplied.waitUntilApplied("orders", AppendId.mint(), Duration.ofMillis(900), Backoff.exponential(Duration.ofMillis(20), Duration.ofMillis(80), 2.0));

        // The first gap is the time before any sleep, so the sleeps show up from the second gap onwards: 20, 40, 80,
        // then 80 again because the max caps it. Timing is machine-dependent, so this asserts growth and the cap
        // rather than exact values.
        List<Long> sleeps = pollGapsMillis.subList(1, pollGapsMillis.size());
        assertThat(sleeps).hasSizeGreaterThanOrEqualTo(4);
        assertThat(sleeps.get(1)).isGreaterThan(sleeps.get(0));
        assertThat(sleeps).allSatisfy(gap -> assertThat(gap).isLessThan(200L));
    }

    @Test
    void waitUntilApplied_returns_true_immediately_when_the_append_is_already_recorded() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppendId appendId = AppendId.mint();
        store.recordApplied("orders", appendId);

        boolean applied = store.waitUntilApplied("orders", appendId, Duration.ofSeconds(5));

        assertThat(applied).isTrue();
    }

    @Test
    void waitUntilApplied_returns_true_once_another_thread_records_the_append_it_is_waiting_for() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppendId appendId = AppendId.mint();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            scheduler.schedule(() -> store.recordApplied("orders", appendId), 50, TimeUnit.MILLISECONDS);

            boolean applied = store.waitUntilApplied("orders", appendId, Duration.ofSeconds(5), Backoff.fixed(10));

            assertThat(applied).isTrue();
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void waitUntilApplied_returns_false_on_timeout_rather_than_throwing_when_the_append_never_arrives() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();

        boolean applied = store.waitUntilApplied("orders", AppendId.mint(), Duration.ofMillis(100), Backoff.fixed(10));

        assertThat(applied).isFalse();
    }

    @Test
    void waitUntilApplied_only_answers_true_for_the_exact_append_it_was_asked_about() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        store.recordApplied("orders", AppendId.mint());

        boolean applied = store.waitUntilApplied("orders", AppendId.mint(), Duration.ofMillis(100), Backoff.fixed(10));

        assertThat(applied).isFalse();
    }

    @Test
    void an_interrupted_wait_returns_false_and_restores_the_interrupt_flag() throws InterruptedException {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        CountDownLatch started = new CountDownLatch(1);
        boolean[] result = new boolean[1];
        boolean[] interruptedAfterwards = new boolean[1];
        Thread waiter = new Thread(() -> {
            started.countDown();
            result[0] = store.waitUntilApplied("orders", AppendId.mint(), Duration.ofSeconds(30), Backoff.fixed(Duration.ofSeconds(30)));
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
