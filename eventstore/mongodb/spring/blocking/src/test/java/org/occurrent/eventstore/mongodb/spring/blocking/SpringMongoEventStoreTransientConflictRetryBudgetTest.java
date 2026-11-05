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
package org.occurrent.eventstore.mongodb.spring.blocking;

import com.mongodb.MongoException;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.retry.Backoff;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore.TRANSIENT_CONFLICT_RETRY;
import static org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore.transientConflictRetryWithDeadline;

/**
 * Checks the number of attempts that {@link SpringMongoEventStoreTransientConflictRetryTest} cannot, since it makes the
 * server fail an append 16 times and so passes with any budget above that. The real strategy is used with its waits
 * taken out, so 240 attempts cost milliseconds. The length of the waits, 10 ms doubling up to 500 ms, is not
 * exercised here, because running them takes the two minutes this test exists to avoid.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoEventStoreTransientConflictRetryBudgetTest {

    private static final int ATTEMPTS = 240;

    @Test
    void an_operation_that_fails_on_all_but_its_last_permitted_attempt_succeeds() {
        AtomicInteger attempts = new AtomicInteger();

        String result = TRANSIENT_CONFLICT_RETRY.backoff(Backoff.none()).execute(() -> failingTimes(ATTEMPTS - 1, attempts));

        assertThat(result).isEqualTo("committed");
        assertThat(attempts).hasValue(ATTEMPTS);
    }

    @Test
    void an_operation_that_fails_on_every_permitted_attempt_fails_with_the_error_of_the_last_one() {
        AtomicInteger attempts = new AtomicInteger();

        assertThatThrownBy(() -> TRANSIENT_CONFLICT_RETRY.backoff(Backoff.none()).execute(() -> failingTimes(ATTEMPTS, attempts)))
                .isInstanceOf(MongoException.class)
                .hasMessage("conflict " + ATTEMPTS);

        assertThat(attempts).hasValue(ATTEMPTS);
    }

    @Test
    void an_error_that_is_not_a_transaction_conflict_is_not_retried() {
        AtomicInteger attempts = new AtomicInteger();

        assertThatThrownBy(() -> TRANSIENT_CONFLICT_RETRY.backoff(Backoff.none()).execute(() -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("not a conflict");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(attempts).hasValue(1);
    }

    @Test
    void the_retry_stops_when_its_deadline_has_passed_even_though_attempts_remain() {
        AtomicInteger attempts = new AtomicInteger();

        // Each attempt takes 20 ms, so a 200 ms deadline allows about ten of the 240 attempts.
        assertThatThrownBy(() -> transientConflictRetryWithDeadline(Duration.ofMillis(200)).backoff(Backoff.none()).execute(() -> {
            sleep(20);
            return failingTimes(ATTEMPTS, attempts);
        })).isInstanceOf(MongoException.class);

        assertThat(attempts.get()).isBetween(5, 30);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static String failingTimes(int failures, AtomicInteger attempts) {
        int attempt = attempts.incrementAndGet();
        if (attempt <= failures) {
            MongoException conflict = new MongoException("conflict " + attempt);
            conflict.addLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL);
            throw conflict;
        }
        return "committed";
    }
}
