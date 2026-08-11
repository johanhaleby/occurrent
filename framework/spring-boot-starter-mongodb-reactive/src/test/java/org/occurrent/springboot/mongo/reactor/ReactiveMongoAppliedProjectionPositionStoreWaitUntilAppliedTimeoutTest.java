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

import org.bson.Document;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.projection.AppliedProjectionPositionStore;
import org.occurrent.retry.Backoff;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.Query;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link ReactiveMongoAppliedProjectionPositionStore#waitUntilApplied(String, long, Duration)} promises to return
 * {@code false} once {@code timeout} elapses. Left unbounded, a single read's {@link Retry} sequence can run well
 * past a short wait's own deadline before it gives up and throws, which is exactly the failure this test forces: a
 * {@link Retry} whose backoff schedule outlives the wait's timeout, so the wait must cut the read off at its own
 * deadline rather than let the retry run to its own exhaustion. No Testcontainers needed, a mocked
 * {@link ReactiveMongoOperations} whose read always errors is enough to force the sustained-outage path.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(10)
class ReactiveMongoAppliedProjectionPositionStoreWaitUntilAppliedTimeoutTest {

    @Test
    void returns_false_within_its_timeout_against_a_store_whose_reads_keep_failing() {
        ReactiveMongoOperations mongoOperations = mock(ReactiveMongoOperations.class);
        AtomicInteger attempts = new AtomicInteger();
        // retryWhen resubscribes to this same Mono rather than calling findOne(..) again, so attempts are counted by
        // subscription, not by the mock's own invocation count.
        when(mongoOperations.findOne(any(Query.class), eq(Document.class), anyString()))
                .thenReturn(Mono.<Document>error(new RuntimeException("store outage")).doOnSubscribe(s -> attempts.incrementAndGet()));
        // Retries alone would not exhaust for the better part of a second, well past the 200 ms wait below, so the
        // wait's own deadline is what has to cut the read off rather than the retry's own exhaustion.
        Retry slowRetry = Retry.backoff(5, Duration.ofMillis(50))
                .maxBackoff(Duration.ofMillis(500))
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
        AppliedProjectionPositionStore storage = new ReactiveMongoAppliedProjectionPositionStore(mongoOperations, "appliedPositions", slowRetry, Backoff.fixed(20));
        Duration timeout = Duration.ofMillis(200);

        Instant start = Instant.now();
        boolean caughtUp = storage.waitUntilApplied("orders", 1, timeout);
        Duration elapsed = Duration.between(start, Instant.now());

        assertThat(caughtUp).isFalse();
        // The deadline is only checked between retry attempts, so the last in-flight attempt can overrun it by up
        // to one retry interval. This slack stays well short of whole extra retry cycles.
        assertThat(elapsed).isLessThan(timeout.plusSeconds(2));
        assertThat(attempts.get()).isGreaterThanOrEqualTo(2);
    }

    @Test
    void propagates_exception_when_finite_retry_attempts_exhausted() {
        ReactiveMongoOperations mongoOperations = mock(ReactiveMongoOperations.class);
        RuntimeException storeFailure = new RuntimeException("persistent store error");
        when(mongoOperations.findOne(any(Query.class), eq(Document.class), anyString()))
                .thenReturn(Mono.error(storeFailure));
        Retry finiteRetry = Retry.fixedDelay(2, Duration.ofMillis(10))
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
        AppliedProjectionPositionStore storage = new ReactiveMongoAppliedProjectionPositionStore(mongoOperations, "appliedPositions", finiteRetry, Backoff.fixed(100));
        Duration generousTimeout = Duration.ofSeconds(10);

        assertThatThrownBy(() ->
                storage.waitUntilApplied("orders", 1, generousTimeout)
        ).isEqualTo(storeFailure);
    }
}
