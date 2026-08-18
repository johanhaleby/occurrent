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
 * {@code timeout} elapses. The store's read is wrapped in a {@link RetryStrategy} that, left to its own default,
 * retries forever, so a wait against a store that never stops failing must stop that retry at its own deadline
 * itself rather than let it keep going on the store's own schedule. No Testcontainers needed, a mocked
 * {@link MongoOperations} whose read always throws is enough to force the sustained-outage path.
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
    void propagates_exception_when_finite_retry_attempts_exhausted() {
        MongoOperations mongoOperations = mock(MongoOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(mock(IndexOperations.class));
        RuntimeException storeFailure = new RuntimeException("persistent store error");
        when(mongoOperations.exists(any(Query.class), anyString()))
                .thenThrow(storeFailure);
        RetryStrategy finiteRetry = RetryStrategy.retry().backoff(Backoff.fixed(10)).maxAttempts(2);
        AppliedAppendStore store = new MongoAppliedAppendStore(mongoOperations, "appliedAppends", Duration.ofDays(7), finiteRetry, Backoff.fixed(100));
        Duration generousTimeout = Duration.ofSeconds(10);

        assertThatThrownBy(() ->
                store.waitUntilApplied("orders", AppendId.mint(), generousTimeout)
        ).isEqualTo(storeFailure);
    }
}
