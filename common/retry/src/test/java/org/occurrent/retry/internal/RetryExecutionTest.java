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

package org.occurrent.retry.internal;

import org.junit.jupiter.api.Test;
import org.occurrent.retry.RetryStrategy;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves that {@link RetryExecution} observes a shutdown signaled through its shutdown predicate while a backoff
 * is sleeping, not only right before the sleep starts. Uses a fake {@link RetryExecution.Sleeper} so the sleep
 * never actually runs on the wall clock: the test asserts on how many poll-sized chunks were slept before the
 * loop stopped, which is deterministic, rather than on how fast the test happened to run.
 */
class RetryExecutionTest {

    private static final long POLL_INTERVAL_MILLIS = 50;

    @Test
    void shutdown_requested_partway_through_a_long_backoff_stops_the_loop_without_a_further_attempt() {
        AtomicInteger attempts = new AtomicInteger();
        AtomicBoolean shutdownRequested = new AtomicBoolean(false);
        AtomicInteger sleptChunks = new AtomicInteger();

        // A fake sleeper that never actually sleeps, but requests shutdown after the third poll-sized chunk,
        // partway through a backoff that is many chunks long.
        RetryExecution.Sleeper sleeper = millis -> {
            sleptChunks.incrementAndGet();
            if (sleptChunks.get() == 3) {
                shutdownRequested.set(true);
            }
        };

        RetryStrategy retryStrategy = RetryStrategy.fixed(20 * POLL_INTERVAL_MILLIS).maxAttempts(1000);
        Runnable failingAction = () -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("always fails");
        };

        Runnable retrying = RetryExecution.executeWithRetry(failingAction, __ -> !shutdownRequested.get(), retryStrategy, sleeper);

        assertThatThrownBy(retrying::run).isInstanceOf(IllegalStateException.class).hasMessage("always fails");

        // Only the first attempt ran: shutdown was observed while the backoff after it was still sleeping, so no
        // second attempt was made.
        assertThat(attempts).hasValue(1);
        // Only 3 of the 20 possible chunks were slept: the bound on shutdown latency is the poll interval, not the
        // remaining backoff duration.
        assertThat(sleptChunks).hasValue(3);
    }

    @Test
    void backoff_runs_to_completion_and_the_next_attempt_happens_when_shutdown_is_never_requested() {
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger sleptChunks = new AtomicInteger();

        RetryExecution.Sleeper sleeper = millis -> sleptChunks.incrementAndGet();

        RetryStrategy retryStrategy = RetryStrategy.fixed(3 * POLL_INTERVAL_MILLIS).maxAttempts(2);
        Runnable failingAction = () -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("always fails");
        };

        Runnable retrying = RetryExecution.executeWithRetry(failingAction, __ -> true, retryStrategy, sleeper);

        assertThatThrownBy(retrying::run).isInstanceOf(IllegalStateException.class);

        assertThat(attempts).hasValue(2);
        assertThat(sleptChunks).hasValue(3);
    }

    @Test
    void the_retry_predicate_is_tested_once_per_attempt_even_though_the_backoff_after_it_is_polled_many_times() {
        AtomicInteger retryPredicateInvocations = new AtomicInteger();
        AtomicInteger sleptChunks = new AtomicInteger();

        RetryExecution.Sleeper sleeper = millis -> sleptChunks.incrementAndGet();

        RetryStrategy retryStrategy = RetryStrategy.fixed(20 * POLL_INTERVAL_MILLIS)
                .maxAttempts(2)
                .retryIf(e -> {
                    retryPredicateInvocations.incrementAndGet();
                    return true;
                });
        Runnable failingAction = () -> {
            throw new IllegalStateException("always fails");
        };

        Runnable retrying = RetryExecution.executeWithRetry(failingAction, __ -> true, retryStrategy, sleeper);

        assertThatThrownBy(retrying::run).isInstanceOf(IllegalStateException.class);

        // The backoff was polled 20 times, but the caller's own retry predicate is a separate, potentially
        // stateful thing: it must be asked once per failed attempt, the same as before this fix, not once per poll.
        assertThat(sleptChunks).hasValue(20);
        assertThat(retryPredicateInvocations).hasValue(1);
    }

    @Test
    void the_after_retry_listener_fires_only_once_for_an_attempt_that_shutdown_interrupts_mid_backoff() {
        AtomicInteger afterRetryInvocations = new AtomicInteger();
        AtomicBoolean shutdownRequested = new AtomicBoolean(false);
        AtomicInteger sleptChunks = new AtomicInteger();
        int chunksPerBackoff = 20;

        // The first attempt's backoff (20 chunks) runs to completion undisturbed. Shutdown is only requested on
        // the first chunk of the second attempt's backoff, once that attempt's own after-retry call has already
        // fired, so this proves shutdown does not cause a second, duplicate after-retry call for that attempt.
        RetryExecution.Sleeper sleeper = millis -> {
            if (sleptChunks.incrementAndGet() > chunksPerBackoff) {
                shutdownRequested.set(true);
            }
        };

        RetryStrategy retryStrategy = RetryStrategy.fixed(chunksPerBackoff * POLL_INTERVAL_MILLIS)
                .maxAttempts(1000)
                .onAfterRetry(__ -> afterRetryInvocations.incrementAndGet());
        Runnable failingAction = () -> {
            throw new IllegalStateException("always fails");
        };

        Runnable retrying = RetryExecution.executeWithRetry(failingAction, __ -> !shutdownRequested.get(), retryStrategy, sleeper);

        assertThatThrownBy(retrying::run).isInstanceOf(IllegalStateException.class);

        // The listener already reported the second attempt as failed-and-retrying right before its backoff
        // started. Shutdown cutting that backoff short must not report the same attempt a second time.
        assertThat(afterRetryInvocations).hasValue(1);
    }

    @Test
    void an_interrupted_backoff_sleep_restores_the_interrupt_status_instead_of_swallowing_it() {
        RetryExecution.Sleeper sleeper = millis -> {
            throw new InterruptedException("interrupted mid-backoff");
        };

        RetryStrategy retryStrategy = RetryStrategy.fixed(POLL_INTERVAL_MILLIS).maxAttempts(1000);
        Runnable failingAction = () -> {
            throw new IllegalStateException("always fails");
        };

        Runnable retrying = RetryExecution.executeWithRetry(failingAction, __ -> true, retryStrategy, sleeper);

        try {
            assertThatThrownBy(retrying::run).isInstanceOf(RuntimeException.class).hasCauseInstanceOf(IllegalStateException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            // Clear the flag we just asserted on so it doesn't leak into whatever runs next on this thread.
            Thread.interrupted();
        }
    }
}
