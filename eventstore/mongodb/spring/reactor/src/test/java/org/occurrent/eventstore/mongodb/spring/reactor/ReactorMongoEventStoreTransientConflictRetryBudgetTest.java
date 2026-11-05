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
package org.occurrent.eventstore.mongodb.spring.reactor;

import com.mongodb.MongoException;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore.ANY_STREAM_VERSION_CONFLICT_RETRY;
import static org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore.TRANSIENT_CONFLICT_RETRY;
import static org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore.transientConflictRetryWithDeadline;

/**
 * Checks the size of the retry budget that {@link ReactorMongoEventStoreTransientConflictRetryTest} cannot, since it makes
 * the server fail an append 16 times and so passes with any budget above that. It runs the retry in virtual time,
 * so a budget of two minutes costs milliseconds.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoEventStoreTransientConflictRetryBudgetTest {

    // Retry.backoff(320, ...) resends 320 times after the first attempt, so an operation gets 321 attempts.
    private static final int RETRIES = 320;

    @Test
    void an_operation_that_fails_on_its_first_320_attempts_succeeds_on_the_next() {
        AtomicInteger attempts = new AtomicInteger();

        StepVerifier.withVirtualTime(() -> failingTimes(RETRIES, attempts).retryWhen(TRANSIENT_CONFLICT_RETRY))
                .thenAwait(Duration.ofMinutes(5))
                .expectNext("committed")
                .verifyComplete();

        assertThat(attempts).hasValue(RETRIES + 1);
    }

    @Test
    void an_operation_that_fails_on_321_attempts_fails_with_the_error_of_the_last_one() {
        AtomicInteger attempts = new AtomicInteger();

        StepVerifier.withVirtualTime(() -> failingTimes(RETRIES + 1, attempts).retryWhen(TRANSIENT_CONFLICT_RETRY))
                .thenAwait(Duration.ofMinutes(5))
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(MongoException.class).hasMessage("conflict " + (RETRIES + 1)))
                .verify();

        assertThat(attempts).hasValue(RETRIES + 1);
    }

    @Test
    void the_retries_wait_for_about_two_minutes_before_giving_up() {
        // Each wait is jittered between 250 and 500 ms once the backoff reaches its 500 ms cap, which sums to about
        // 118 seconds with a standard deviation near a second and a half. The bounds are many deviations away.
        StepVerifier.withVirtualTime(() -> failingTimes(RETRIES + 1, new AtomicInteger()).retryWhen(TRANSIENT_CONFLICT_RETRY))
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(100))
                .thenAwait(Duration.ofSeconds(30))
                .expectError(MongoException.class)
                .verify();
    }

    @Test
    void an_error_that_is_not_a_transaction_conflict_is_not_retried() {
        AtomicInteger attempts = new AtomicInteger();

        StepVerifier.withVirtualTime(() -> Mono.<String>defer(() -> {
                    attempts.incrementAndGet();
                    return Mono.error(new IllegalStateException("not a conflict"));
                }).retryWhen(TRANSIENT_CONFLICT_RETRY))
                .thenAwait(Duration.ofMinutes(5))
                .expectError(IllegalStateException.class)
                .verify();

        assertThat(attempts).hasValue(1);
    }

    @Test
    void the_retry_stops_when_its_deadline_has_passed_even_though_retries_remain() {
        AtomicInteger attempts = new AtomicInteger();

        // The deadline is read off the system clock, so it runs in real time. The backoff starts at 10 ms and
        // reaches 500 ms after a few retries, which allows a handful of attempts inside 300 ms out of 321 permitted.
        StepVerifier.create(failingTimes(RETRIES + 1, attempts).retryWhen(transientConflictRetryWithDeadline(Duration.ofMillis(300))))
                .expectError(MongoException.class)
                .verify(Duration.ofSeconds(10));

        assertThat(attempts.get()).isBetween(2, 30);
    }

    @Test
    void a_lost_any_stream_version_race_is_retried_320_times_and_not_15() {
        AtomicInteger attempts = new AtomicInteger();

        StepVerifier.withVirtualTime(() -> Mono.<String>defer(() -> {
                    int attempt = attempts.incrementAndGet();
                    return attempt <= RETRIES ? Mono.error(new WriteConditionNotFulfilledException("stream", attempt, WriteCondition.anyStreamVersion(), "lost")) : Mono.just("committed");
                }).retryWhen(ANY_STREAM_VERSION_CONFLICT_RETRY))
                .thenAwait(Duration.ofMinutes(5))
                .expectNext("committed")
                .verifyComplete();

        assertThat(attempts).hasValue(RETRIES + 1);
    }

    private static Mono<String> failingTimes(int failures, AtomicInteger attempts) {
        return Mono.defer(() -> {
            int attempt = attempts.incrementAndGet();
            if (attempt <= failures) {
                MongoException conflict = new MongoException("conflict " + attempt);
                conflict.addLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL);
                return Mono.error(conflict);
            }
            return Mono.just("committed");
        });
    }
}
