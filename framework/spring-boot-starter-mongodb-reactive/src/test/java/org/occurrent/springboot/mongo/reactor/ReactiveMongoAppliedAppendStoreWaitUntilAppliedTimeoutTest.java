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

package org.occurrent.springboot.mongo.reactor;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.retry.Backoff;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.index.ReactiveIndexOperations;
import org.springframework.data.mongodb.core.query.Query;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link ReactiveMongoAppliedAppendStore#waitUntilApplied(String, AppendId, Duration)} promises to return
 * {@code false} once {@code timeout} elapses and never to throw, the same contract every
 * {@link AppliedAppendStore#waitUntilApplied(String, AppendId, Duration, Backoff)} implementation makes. A failing
 * read can miss that contract two different ways: its {@link Retry} sequence can still be running when the wait's
 * own deadline arrives, or it can exhaust on its own well before that deadline. Both are forced here with a mocked
 * {@link ReactiveMongoOperations} whose read always errors, no Testcontainers needed.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(10)
class ReactiveMongoAppliedAppendStoreWaitUntilAppliedTimeoutTest {

    private static ReactiveMongoOperations mongoOperationsWithIndexingStubbed() {
        ReactiveMongoOperations mongoOperations = mock(ReactiveMongoOperations.class);
        ReactiveIndexOperations indexOperations = mock(ReactiveIndexOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(indexOperations);
        when(indexOperations.ensureIndex(any())).thenReturn(Mono.just("index"));
        return mongoOperations;
    }

    @Test
    void returns_false_when_the_deadline_arrives_while_a_read_is_still_retrying() {
        ReactiveMongoOperations mongoOperations = mongoOperationsWithIndexingStubbed();
        AtomicInteger attempts = new AtomicInteger();
        // retryWhen resubscribes to this same Mono rather than calling exists(..) again, so attempts are counted by
        // subscription, not by the mock's own invocation count.
        when(mongoOperations.exists(any(Query.class), anyString()))
                .thenReturn(Mono.<Boolean>error(new RuntimeException("store outage")).doOnSubscribe(s -> attempts.incrementAndGet()));
        // Zero jitter makes the schedule exact: 30+60+120+240+480 = 930 ms to exhaust, well past the 200 ms wait
        // below, so the 500 ms assertion margin only fails if the wait's own deadline cuts the read off rather than
        // the retry running to its own exhaustion. The second attempt still lands at 30 ms, well inside the wait.
        Retry slowRetry = Retry.backoff(5, Duration.ofMillis(30))
                .maxBackoff(Duration.ofSeconds(2))
                .jitter(0)
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
        AppliedAppendStore store = new ReactiveMongoAppliedAppendStore(mongoOperations, "appliedAppends", Duration.ofDays(7), slowRetry, Backoff.fixed(20));
        Duration timeout = Duration.ofMillis(200);

        Instant start = Instant.now();
        boolean applied = store.waitUntilApplied("orders", AppendId.mint(), timeout);
        Duration elapsed = Duration.between(start, Instant.now());

        assertThat(applied).isFalse();
        // Limited by the deadline itself, not by however long the retry's own backoff would otherwise run.
        assertThat(elapsed).isLessThan(timeout.plusMillis(500));
        assertThat(attempts.get()).isGreaterThanOrEqualTo(2);
    }

    @Test
    void keeps_polling_until_its_own_deadline_when_a_finite_retry_exhausts_well_before_it() {
        ReactiveMongoOperations mongoOperations = mongoOperationsWithIndexingStubbed();
        when(mongoOperations.exists(any(Query.class), anyString()))
                .thenReturn(Mono.error(new RuntimeException("persistent store error")));
        // Exhausts in roughly 20 ms, far short of the 300 ms wait below, so a poll's own retry exhaustion must not
        // end the wait early, only the wait's own deadline may.
        Retry fastExhaustingRetry = Retry.fixedDelay(2, Duration.ofMillis(10))
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
        AppliedAppendStore store = new ReactiveMongoAppliedAppendStore(mongoOperations, "appliedAppends", Duration.ofDays(7), fastExhaustingRetry, Backoff.fixed(20));
        Duration timeout = Duration.ofMillis(300);

        Instant start = Instant.now();
        boolean applied = store.waitUntilApplied("orders", AppendId.mint(), timeout);
        Duration elapsed = Duration.between(start, Instant.now());

        assertThat(applied).isFalse();
        assertThat(elapsed).isGreaterThanOrEqualTo(timeout.minusMillis(50));
        assertThat(elapsed).isLessThan(timeout.plusMillis(500));
    }

    @Test
    void returns_true_when_a_read_recovers_within_the_deadline() {
        ReactiveMongoOperations mongoOperations = mongoOperationsWithIndexingStubbed();
        when(mongoOperations.exists(any(Query.class), anyString()))
                .thenReturn(Mono.error(new RuntimeException("transient store error")))
                .thenReturn(Mono.just(true));
        Retry retry = Retry.fixedDelay(3, Duration.ofMillis(10))
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
        AppliedAppendStore store = new ReactiveMongoAppliedAppendStore(mongoOperations, "appliedAppends", Duration.ofDays(7), retry, Backoff.fixed(20));

        boolean applied = store.waitUntilApplied("orders", AppendId.mint(), Duration.ofSeconds(5));

        assertThat(applied).isTrue();
    }
}
