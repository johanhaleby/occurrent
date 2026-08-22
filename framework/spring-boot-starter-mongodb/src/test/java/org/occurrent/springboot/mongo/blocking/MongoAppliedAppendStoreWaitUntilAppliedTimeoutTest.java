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

package org.occurrent.springboot.mongo.blocking;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.retry.Backoff;
import org.occurrent.retry.RetryStrategy;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.query.Query;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link MongoAppliedAppendStore#waitUntilApplied(String, AppendId, Duration)} promises to return {@code false} once
 * {@code timeout} elapses. The store's read is wrapped in a {@link RetryStrategy} with an attempt budget of its
 * own, which says nothing about how long those attempts take, so a wait against a store that never stops failing
 * must stop that retry at its own deadline rather than let it run its attempts out. No Testcontainers needed, a
 * mocked {@link MongoOperations} whose read always throws is enough to force the sustained-outage path.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(10)
class MongoAppliedAppendStoreWaitUntilAppliedTimeoutTest {

    @Test
    void returns_false_within_its_timeout_against_a_store_whose_reads_keep_failing() {
        MongoOperations mongoOperations = mock(MongoOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(mock(IndexOperations.class));
        when(mongoOperations.exists(any(Query.class), anyString()))
                .thenThrow(new RuntimeException("store outage"));
        RetryStrategy fastRetry = RetryStrategy.exponentialBackoff(Duration.ofMillis(10), Duration.ofMillis(50), 2.0);
        AppliedAppendStore store = new MongoAppliedAppendStore(mongoOperations, "appliedAppends", Duration.ofDays(7), fastRetry, Backoff.fixed(20));
        Duration timeout = Duration.ofMillis(200);

        Instant start = Instant.now();
        boolean applied = store.waitUntilApplied("orders", AppendId.mint(), timeout);
        Duration elapsed = Duration.between(start, Instant.now());

        assertThat(applied).isFalse();
        // The deadline is only checked between retry attempts, so the last in-flight attempt can overrun it by up
        // to one retry interval. This slack stays well short of whole extra retry cycles.
        assertThat(elapsed).isLessThan(timeout.plusSeconds(2));
        verify(mongoOperations, atLeast(2)).exists(any(Query.class), anyString());
    }

    @Test
    void returns_false_within_its_timeout_when_a_fresh_stores_index_setup_keeps_failing() {
        MongoOperations mongoOperations = mock(MongoOperations.class);
        IndexOperations indexOperations = mock(IndexOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(indexOperations);
        when(indexOperations.ensureIndex(any())).thenThrow(new RuntimeException("store outage"));
        RetryStrategy fastRetry = RetryStrategy.exponentialBackoff(Duration.ofMillis(10), Duration.ofMillis(50), 2.0);
        AppliedAppendStore store = new MongoAppliedAppendStore(mongoOperations, "appliedAppends", Duration.ofDays(7), fastRetry, Backoff.fixed(20));
        Duration timeout = Duration.ofMillis(200);

        Instant start = Instant.now();
        boolean applied = store.waitUntilApplied("orders", AppendId.mint(), timeout);
        Duration elapsed = Duration.between(start, Instant.now());

        assertThat(applied).isFalse();
        assertThat(elapsed).isLessThan(timeout.plusSeconds(2));
        verify(indexOperations, atLeast(2)).ensureIndex(any());
    }

    @Test
    void an_interrupted_retry_sleep_returns_false_and_restores_the_interrupt_flag_rather_than_throwing() throws InterruptedException {
        MongoOperations mongoOperations = mock(MongoOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(mock(IndexOperations.class));
        when(mongoOperations.exists(any(Query.class), anyString()))
                .thenThrow(new RuntimeException("store outage"));
        // A long retry backoff, so the interrupt below lands while a retry attempt is sleeping inside
        // RetryExecution, not the outer poll sleep, which already handles interruption correctly on its own.
        RetryStrategy slowRetry = RetryStrategy.exponentialBackoff(Duration.ofSeconds(5), Duration.ofSeconds(5), 1.0);
        AppliedAppendStore store = new MongoAppliedAppendStore(mongoOperations, "appliedAppends", Duration.ofDays(7), slowRetry, Backoff.fixed(20));
        CountDownLatch started = new CountDownLatch(1);
        boolean[] result = new boolean[1];
        boolean[] interruptedAfterwards = new boolean[1];
        Thread waiter = new Thread(() -> {
            started.countDown();
            result[0] = store.waitUntilApplied("orders", AppendId.mint(), Duration.ofSeconds(30));
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

    @Test
    void the_constructor_rejects_a_busy_loop_poll_backoff_instead_of_waiting_until_the_first_wait_to_fail() {
        MongoOperations mongoOperations = mock(MongoOperations.class);

        assertThatThrownBy(() ->
                new MongoAppliedAppendStore(mongoOperations, "appliedAppends", Duration.ofDays(7),
                        RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f), Backoff.none())
        ).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Backoff.none()");
    }

    @Test
    void the_constructor_rejects_a_negative_retention_instead_of_retrying_the_resulting_mongo_error_forever() {
        MongoOperations mongoOperations = mock(MongoOperations.class);

        assertThatThrownBy(() ->
                new MongoAppliedAppendStore(mongoOperations, "appliedAppends", Duration.ofDays(-1),
                        RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f), Backoff.fixed(20))
        ).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retention cannot be negative");
    }

    @Test
    void keeps_polling_until_its_own_deadline_when_a_finite_retry_exhausts_well_before_it() {
        MongoOperations mongoOperations = mock(MongoOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(mock(IndexOperations.class));
        when(mongoOperations.exists(any(Query.class), anyString()))
                .thenThrow(new RuntimeException("persistent store error"));
        // Exhausts in roughly 20 ms, far short of the 300 ms wait below, so a poll's own retry exhaustion must not
        // end the wait early, only the wait's own deadline may. ADR 132 decision 5 states this unconditionally,
        // matching the reactive store's own equivalent test.
        RetryStrategy finiteRetry = RetryStrategy.retry().backoff(Backoff.fixed(10)).maxAttempts(2);
        AppliedAppendStore store = new MongoAppliedAppendStore(mongoOperations, "appliedAppends", Duration.ofDays(7), finiteRetry, Backoff.fixed(20));
        Duration timeout = Duration.ofMillis(300);

        Instant start = Instant.now();
        boolean applied = store.waitUntilApplied("orders", AppendId.mint(), timeout);
        Duration elapsed = Duration.between(start, Instant.now());

        assertThat(applied).isFalse();
        assertThat(elapsed).isGreaterThanOrEqualTo(timeout.minusMillis(50));
        assertThat(elapsed).isLessThan(timeout.plusSeconds(2));
    }

    /**
     * The bound this store documents, written down as a test rather than only as prose. The deadline is checked
     * between reads, so a read already in flight when it passes runs to completion and the wait returns after it.
     * A MongoDB client with no timeout of its own never completes that read while a connection it has accepted
     * stops responding, which is why the javadoc tells an application to configure one.
     */
    @Test
    void a_read_that_stops_responding_holds_the_wait_past_its_deadline_which_is_why_the_client_needs_a_timeout_of_its_own() {
        Duration stall = Duration.ofSeconds(2);
        MongoOperations mongoOperations = mock(MongoOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(mock(IndexOperations.class));
        when(mongoOperations.exists(any(Query.class), anyString())).thenAnswer(invocation -> {
            Thread.sleep(stall.toMillis());
            return false;
        });
        RetryStrategy oneAttempt = RetryStrategy.retry().backoff(Backoff.fixed(5)).maxAttempts(1);
        AppliedAppendStore store = new MongoAppliedAppendStore(mongoOperations, "appliedAppends", Duration.ofDays(7), oneAttempt, Backoff.fixed(20));
        Duration timeout = Duration.ofMillis(50);

        Instant start = Instant.now();
        boolean applied = store.waitUntilApplied("orders", AppendId.mint(), timeout);
        Duration elapsed = Duration.between(start, Instant.now());

        assertThat(applied).isFalse();
        assertThat(elapsed).isGreaterThanOrEqualTo(stall);
    }
}
