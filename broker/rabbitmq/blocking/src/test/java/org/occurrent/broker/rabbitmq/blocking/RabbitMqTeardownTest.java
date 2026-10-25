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

package org.occurrent.broker.rabbitmq.blocking;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class RabbitMqTeardownTest {

    // About 146 years, the longest a deadline is set ahead. Spelled out here rather than read from the class, so a
    // change to it has to change this test as well.
    private static final long LONGEST_TIMEOUT_NANOS = Long.MAX_VALUE / 2;

    @ParameterizedTest
    @MethodSource("timeoutsAtOrBelowTheLongest")
    void a_timeout_that_fits_is_returned_in_nanoseconds_as_it_is(Duration timeout, long expectedNanos) {
        assertThat(RabbitMqTeardown.timeoutNanos(timeout)).isEqualTo(expectedNanos);
    }

    @ParameterizedTest
    @MethodSource("timeoutsAboveTheLongest")
    void a_timeout_longer_than_the_longest_counts_as_the_longest(Duration timeout) {
        assertThat(RabbitMqTeardown.timeoutNanos(timeout)).isEqualTo(LONGEST_TIMEOUT_NANOS);
    }

    @ParameterizedTest
    @MethodSource("timeoutsAboveTheLongest")
    void a_timeout_longer_than_the_longest_does_not_throw(Duration timeout) {
        Throwable thrown = catchThrowable(() -> RabbitMqTeardown.timeoutNanos(timeout));

        assertThat(thrown).isNull();
    }

    @Test
    void the_longest_representable_duration_counts_as_the_longest() {
        Duration largest = Duration.ofSeconds(Long.MAX_VALUE, 999_999_999);

        assertThat(RabbitMqTeardown.timeoutNanos(largest)).isEqualTo(LONGEST_TIMEOUT_NANOS);
        assertThat(ChronoUnit.FOREVER.getDuration()).isEqualTo(largest);
    }

    @Test
    void the_result_never_shrinks_as_the_timeout_grows() {
        long previous = -1;
        for (Duration timeout : timeoutsAroundTheLongest().toList()) {
            long nanos = RabbitMqTeardown.timeoutNanos(timeout);

            assertThat(nanos).as("for %s", timeout).isGreaterThanOrEqualTo(previous);
            previous = nanos;
        }
    }

    /**
     * Deadlines are compared by subtraction, so one set that far ahead is still ahead of a clock that has moved on by
     * a while, wherever the clock started and even if adding the timeout wrapped the sum.
     */
    @ParameterizedTest
    @ValueSource(longs = {Long.MIN_VALUE, Long.MIN_VALUE / 2, -1L, 0L, 1L, Long.MAX_VALUE / 2, Long.MAX_VALUE - 1, Long.MAX_VALUE})
    void a_deadline_set_with_a_huge_timeout_has_not_passed_a_day_later_whatever_the_clock_started_at(long clockNanos) {
        long oneDayNanos = Duration.ofDays(1).toNanos();

        for (Duration timeout : timeoutsAboveTheLongest().toList()) {
            long deadline = clockNanos + RabbitMqTeardown.timeoutNanos(timeout);
            long later = clockNanos + oneDayNanos;

            assertThat(later - deadline).as("for %s with the clock at %s", timeout, clockNanos).isNegative();
            assertThat(deadline - clockNanos).as("time left for %s", timeout).isPositive();
        }
    }

    private static Stream<Arguments> timeoutsAtOrBelowTheLongest() {
        return Stream.of(
                Arguments.of(Duration.ZERO, 0L),
                Arguments.of(Duration.ofNanos(1), 1L),
                Arguments.of(Duration.ofSeconds(30), 30_000_000_000L),
                Arguments.of(Duration.ofDays(365L * 100), 365L * 100 * 24 * 60 * 60 * 1_000_000_000L),
                Arguments.of(Duration.ofNanos(LONGEST_TIMEOUT_NANOS - 1), LONGEST_TIMEOUT_NANOS - 1),
                Arguments.of(Duration.ofNanos(LONGEST_TIMEOUT_NANOS), LONGEST_TIMEOUT_NANOS));
    }

    private static Stream<Duration> timeoutsAboveTheLongest() {
        return Stream.of(
                Duration.ofNanos(LONGEST_TIMEOUT_NANOS + 1),
                Duration.ofNanos(Long.MAX_VALUE),
                Duration.ofNanos(Long.MAX_VALUE).plusNanos(1),
                Duration.ofDays(365L * 300),
                Duration.ofSeconds(Long.MAX_VALUE, 999_999_999),
                ChronoUnit.FOREVER.getDuration());
    }

    private static Stream<Duration> timeoutsAroundTheLongest() {
        return Stream.concat(
                Stream.of(Duration.ZERO, Duration.ofNanos(1), Duration.ofSeconds(30), Duration.ofNanos(LONGEST_TIMEOUT_NANOS - 1), Duration.ofNanos(LONGEST_TIMEOUT_NANOS)),
                timeoutsAboveTheLongest());
    }
}
