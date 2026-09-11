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

package org.occurrent.retry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code execute} overloads that take a shutdown predicate. Each one stops the retry loop and rethrows the last
 * exception once the predicate answers {@code false}, where the overload without a predicate keeps retrying.
 * {@link org.occurrent.retry.internal.RetryExecutionTest} covers when the predicate is read, including while a
 * backoff is being slept out.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("a retry strategy given a shutdown predicate")
class RetryStrategyShutdownPredicateTest {

    private static final RetryStrategy RETRY_UP_TO_FIVE_TIMES = RetryStrategy.fixed(Duration.ofMillis(10)).maxAttempts(5);

    @Test
    void stops_retrying_a_runnable_once_the_predicate_answers_false() {
        AtomicInteger attempts = new AtomicInteger();
        AtomicBoolean shuttingDown = new AtomicBoolean(false);
        Runnable failsOnlyTheFirstTime = () -> {
            if (attempts.incrementAndGet() == 1) {
                shuttingDown.set(true);
                throw new IllegalStateException("the first attempt fails");
            }
        };

        assertThatThrownBy(() -> RETRY_UP_TO_FIVE_TIMES.execute(failsOnlyTheFirstTime, __ -> !shuttingDown.get()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("the first attempt fails");

        assertThat(attempts)
                .as("the second attempt would have succeeded, so an attempt count above one means the predicate was ignored")
                .hasValue(1);
    }

    @Test
    void stops_retrying_a_supplier_once_the_predicate_answers_false() {
        AtomicInteger attempts = new AtomicInteger();
        AtomicBoolean shuttingDown = new AtomicBoolean(false);

        assertThatThrownBy(() -> RETRY_UP_TO_FIVE_TIMES.<String>execute(() -> {
            if (attempts.incrementAndGet() == 1) {
                shuttingDown.set(true);
                throw new IllegalStateException("the first attempt fails");
            }
            return "the second attempt would have answered this";
        }, __ -> !shuttingDown.get()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("the first attempt fails");

        assertThat(attempts).hasValue(1);
    }

    @Test
    void stops_retrying_a_function_once_the_predicate_answers_false() {
        AtomicInteger attempts = new AtomicInteger();
        AtomicBoolean shuttingDown = new AtomicBoolean(false);

        assertThatThrownBy(() -> RETRY_UP_TO_FIVE_TIMES.<String>execute(retryInfo -> {
            if (attempts.incrementAndGet() == 1) {
                shuttingDown.set(true);
                throw new IllegalStateException("the first attempt fails");
            }
            return "attempt number " + retryInfo.getAttemptNumber();
        }, __ -> !shuttingDown.get()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("the first attempt fails");

        assertThat(attempts).hasValue(1);
    }

    @Test
    void keeps_retrying_while_the_predicate_answers_true() {
        AtomicInteger attempts = new AtomicInteger();
        Runnable failsOnlyTheFirstTime = () -> {
            if (attempts.incrementAndGet() == 1) {
                throw new IllegalStateException("the first attempt fails");
            }
        };

        RETRY_UP_TO_FIVE_TIMES.execute(failsOnlyTheFirstTime, __ -> true);

        assertThat(attempts).hasValue(2);
    }

    @Test
    void runs_a_strategy_implemented_outside_this_module_exactly_as_it_runs_itself() {
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger predicateReads = new AtomicInteger();
        RetryStrategy ownLoop = new RetryStrategy() {
            @Override
            public void execute(Runnable runnable) {
                // Stands in for any RetryStrategy a user implements. The retry loop in this module reads a
                // RetryImpl's own settings, so it cannot drive this one and must not try.
                attempts.incrementAndGet();
                runnable.run();
            }
        };

        ownLoop.execute(() -> {
        }, __ -> {
            predicateReads.incrementAndGet();
            return true;
        });

        assertThat(attempts)
                .as("the implementation's own execute should run, rather than this module's retry loop failing on a cast")
                .hasValue(1);
        assertThat(predicateReads)
                .as("there is no loop here to stop, so the predicate is never read")
                .hasValue(0);
    }

    @Test
    void rejects_a_null_predicate() {
        assertThatThrownBy(() -> RETRY_UP_TO_FIVE_TIMES.execute(() -> {
        }, null)).isInstanceOf(NullPointerException.class);
    }
}
