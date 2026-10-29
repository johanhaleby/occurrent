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
import java.util.ArrayList;
import java.util.List;
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

    @Test
    void attaching_a_failure_to_itself_does_nothing() {
        RuntimeException failure = new RuntimeException("failure");

        Throwable thrown = catchThrowable(() -> RabbitMqTeardown.attachSuppressed(failure, failure));

        assertThat(thrown).isNull();
        assertThat(failure.getSuppressed()).isEmpty();
    }

    @Test
    void attaching_a_different_failure_adds_it_as_suppressed() {
        RuntimeException primary = new RuntimeException("primary");
        IllegalStateException secondary = new IllegalStateException("secondary");

        RabbitMqTeardown.attachSuppressed(primary, secondary);

        assertThat(primary.getSuppressed()).containsExactly(secondary);
    }

    @Test
    void attaching_the_same_failure_twice_adds_it_once() {
        RuntimeException primary = new RuntimeException("primary");
        IllegalStateException secondary = new IllegalStateException("secondary");

        RabbitMqTeardown.attachSuppressed(primary, secondary);
        RabbitMqTeardown.attachSuppressed(primary, secondary);

        assertThat(primary.getSuppressed()).containsExactly(secondary);
    }

    @Test
    void a_failure_the_primary_already_carries_as_suppressed_is_not_added_again() {
        RuntimeException primary = new RuntimeException("primary");
        IllegalStateException secondary = new IllegalStateException("secondary");
        primary.addSuppressed(secondary);

        RabbitMqTeardown.attachSuppressed(primary, secondary);

        assertThat(primary.getSuppressed()).containsExactly(secondary);
    }

    @Test
    void a_distinct_failure_that_merely_looks_the_same_is_still_added() {
        RuntimeException primary = new RuntimeException("primary");
        IllegalStateException first = new IllegalStateException("same message");
        IllegalStateException second = new IllegalStateException("same message");

        RabbitMqTeardown.attachSuppressed(primary, first);
        RabbitMqTeardown.attachSuppressed(primary, second);

        assertThat(primary.getSuppressed()).containsExactly(first, second);
    }

    @Test
    void the_cause_of_the_primary_is_not_added_as_suppressed() {
        IllegalStateException cause = new IllegalStateException("cause");
        RuntimeException primary = new RuntimeException("primary", cause);

        Throwable thrown = catchThrowable(() -> RabbitMqTeardown.attachSuppressed(primary, cause));

        assertThat(thrown).isNull();
        assertThat(primary.getSuppressed()).isEmpty();
    }

    @Test
    void a_cause_further_down_the_chain_of_the_primary_is_not_added_as_suppressed() {
        IllegalStateException root = new IllegalStateException("root");
        RuntimeException middle = new RuntimeException("middle", root);
        RuntimeException primary = new RuntimeException("primary", middle);

        Throwable thrown = catchThrowable(() -> {
            RabbitMqTeardown.attachSuppressed(primary, root);
            RabbitMqTeardown.attachSuppressed(primary, middle);
        });

        assertThat(thrown).isNull();
        assertThat(primary.getSuppressed()).isEmpty();
    }

    @Test
    void a_cyclic_chain_of_causes_neither_hangs_nor_hides_a_failure_that_is_not_in_it() {
        RuntimeException primary = new RuntimeException("primary");
        RuntimeException a = new RuntimeException("a");
        RuntimeException b = new RuntimeException("b");
        primary.initCause(a);
        a.initCause(b);
        b.initCause(a);
        IllegalStateException unrelated = new IllegalStateException("unrelated");

        Throwable thrown = catchThrowable(() -> RabbitMqTeardown.attachSuppressed(primary, unrelated));

        assertThat(thrown).isNull();
        assertThat(primary.getSuppressed()).containsExactly(unrelated);
    }

    @Test
    void a_member_of_a_cyclic_chain_of_causes_is_not_added_as_suppressed() {
        RuntimeException primary = new RuntimeException("primary");
        RuntimeException a = new RuntimeException("a");
        RuntimeException b = new RuntimeException("b");
        primary.initCause(a);
        a.initCause(b);
        b.initCause(a);

        Throwable thrown = catchThrowable(() -> {
            RabbitMqTeardown.attachSuppressed(primary, a);
            RabbitMqTeardown.attachSuppressed(primary, b);
        });

        assertThat(thrown).isNull();
        assertThat(primary.getSuppressed()).isEmpty();
    }

    @Test
    void a_primary_that_does_not_record_suppressed_failures_is_left_as_it_is_without_throwing() {
        RuntimeException primary = withoutSuppression("primary");

        Throwable thrown = catchThrowable(() -> RabbitMqTeardown.attachSuppressed(primary, new IllegalStateException("secondary")));

        assertThat(thrown).isNull();
        assertThat(primary.getSuppressed()).isEmpty();
    }

    @Test
    void a_secondary_that_does_not_record_suppressed_failures_is_added_like_any_other() {
        RuntimeException primary = new RuntimeException("primary");
        RuntimeException secondary = withoutSuppression("secondary");

        RabbitMqTeardown.attachSuppressed(primary, secondary);

        assertThat(primary.getSuppressed()).containsExactly(secondary);
    }

    @Test
    void an_error_is_added_as_suppressed_to_a_runtime_exception_and_the_other_way_round() {
        RuntimeException runtime = new RuntimeException("runtime");
        StackOverflowError error = new StackOverflowError("error");

        RabbitMqTeardown.attachSuppressed(runtime, error);
        RabbitMqTeardown.attachSuppressed(error, runtime);

        assertThat(runtime.getSuppressed()).containsExactly(error);
        assertThat(error.getSuppressed()).containsExactly(runtime);
    }

    @Test
    void a_teardown_with_no_failing_step_throws_nothing_with_or_without_a_primary_failure() {
        RuntimeException primary = new RuntimeException("primary");

        Throwable withoutPrimary = catchThrowable(() -> new RabbitMqTeardown(null).run(() -> {
        }).run(() -> {
        }).finish());
        Throwable withPrimary = catchThrowable(() -> new RabbitMqTeardown(primary).run(() -> {
        }).finish());

        assertThat(withoutPrimary).isNull();
        assertThat(withPrimary).isNull();
        assertThat(primary.getSuppressed()).isEmpty();
    }

    @Test
    void without_a_primary_failure_the_one_failing_step_is_what_finish_throws() {
        IllegalStateException failure = new IllegalStateException("step");

        Throwable thrown = catchThrowable(() -> new RabbitMqTeardown(null).run(() -> {
            throw failure;
        }).finish());

        assertThat(thrown).isSameAs(failure);
        assertThat(thrown.getSuppressed()).isEmpty();
    }

    @Test
    void without_a_primary_failure_an_error_from_a_step_is_what_finish_throws() {
        StackOverflowError failure = new StackOverflowError("step");

        Throwable thrown = catchThrowable(() -> new RabbitMqTeardown(null).run(() -> {
            throw failure;
        }).finish());

        assertThat(thrown).isSameAs(failure);
    }

    @Test
    void without_a_primary_failure_the_first_failing_step_is_thrown_and_the_later_ones_are_suppressed_on_it() {
        IllegalStateException first = new IllegalStateException("first");
        StackOverflowError second = new StackOverflowError("second");
        IllegalArgumentException third = new IllegalArgumentException("third");

        Throwable thrown = catchThrowable(() -> new RabbitMqTeardown(null)
                .run(() -> {
                    throw first;
                })
                .run(() -> {
                    throw second;
                })
                .run(() -> {
                    throw third;
                })
                .finish());

        assertThat(thrown).isSameAs(first);
        assertThat(thrown.getSuppressed()).containsExactly(second, third);
    }

    @Test
    void without_a_primary_failure_a_later_step_throwing_the_same_instance_as_the_first_is_not_suppressed_on_itself() {
        IllegalStateException failure = new IllegalStateException("shared");

        Throwable thrown = catchThrowable(() -> new RabbitMqTeardown(null)
                .run(() -> {
                    throw failure;
                })
                .run(() -> {
                    throw failure;
                })
                .finish());

        assertThat(thrown).isSameAs(failure);
        assertThat(thrown.getSuppressed()).isEmpty();
    }

    @Test
    void with_a_primary_failure_every_failing_step_is_suppressed_on_it_and_finish_throws_nothing() {
        IllegalStateException primary = new IllegalStateException("primary");
        RuntimeException first = new RuntimeException("first");
        StackOverflowError second = new StackOverflowError("second");

        Throwable thrown = catchThrowable(() -> new RabbitMqTeardown(primary)
                .run(() -> {
                    throw first;
                })
                .run(() -> {
                })
                .run(() -> {
                    throw second;
                })
                .finish());

        assertThat(thrown).isNull();
        assertThat(primary.getSuppressed()).containsExactly(first, second);
        assertThat(first.getSuppressed()).isEmpty();
    }

    @Test
    void with_a_primary_failure_a_step_throwing_that_very_failure_is_not_suppressed_on_itself() {
        StackOverflowError primary = new StackOverflowError("primary");

        Throwable thrown = catchThrowable(() -> new RabbitMqTeardown(primary)
                .run(() -> {
                    throw primary;
                })
                .finish());

        assertThat(thrown).isNull();
        assertThat(primary.getSuppressed()).isEmpty();
    }

    @Test
    void with_a_primary_failure_a_step_throwing_its_cause_is_not_suppressed_on_it() {
        IllegalStateException cause = new IllegalStateException("cause");
        RuntimeException primary = new RuntimeException("primary", cause);

        Throwable thrown = catchThrowable(() -> new RabbitMqTeardown(primary)
                .run(() -> {
                    throw cause;
                })
                .finish());

        assertThat(thrown).isNull();
        assertThat(primary.getSuppressed()).isEmpty();
    }

    @Test
    void with_a_primary_failure_two_steps_throwing_the_same_instance_suppress_it_once() {
        IllegalStateException primary = new IllegalStateException("primary");
        RuntimeException shared = new RuntimeException("shared");

        Throwable thrown = catchThrowable(() -> new RabbitMqTeardown(primary)
                .run(() -> {
                    throw shared;
                })
                .run(() -> {
                    throw shared;
                })
                .finish());

        assertThat(thrown).isNull();
        assertThat(primary.getSuppressed()).containsExactly(shared);
    }

    @Test
    void every_step_runs_after_an_earlier_one_failed() {
        List<String> ran = new ArrayList<>();

        catchThrowable(() -> new RabbitMqTeardown(null)
                .run(() -> {
                    ran.add("first");
                    throw new IllegalStateException("first");
                })
                .run(() -> {
                    ran.add("second");
                    throw new StackOverflowError("second");
                })
                .run(() -> ran.add("third"))
                .finish());

        assertThat(ran).containsExactly("first", "second", "third");
    }

    /**
     * Kotlin doesn't check exceptions, so a step written in it can throw an {@code InterruptedException} that
     * {@code Runnable.run} doesn't declare.
     */
    @Test
    void a_step_that_throws_an_undeclared_interrupted_exception_lets_the_later_steps_run_and_is_what_leaves() {
        InterruptedException interrupted = new InterruptedException("thrown undeclared");
        List<String> ran = new ArrayList<>();

        Throwable thrown = catchThrowable(() -> new RabbitMqTeardown(null)
                .run(() -> {
                    ran.add("first");
                    throwUndeclared(interrupted);
                })
                .run(() -> ran.add("second"))
                .finish());
        boolean interruptStatusSet = Thread.interrupted();

        assertThat(ran).containsExactly("first", "second");
        assertThat(thrown).isSameAs(interrupted);
        assertThat(interruptStatusSet).isTrue();
    }

    @Test
    void with_a_primary_failure_an_undeclared_interrupted_exception_is_suppressed_and_the_interrupt_status_set_again() {
        IllegalStateException primary = new IllegalStateException("primary");
        InterruptedException interrupted = new InterruptedException("thrown undeclared");
        List<String> ran = new ArrayList<>();

        Throwable thrown = catchThrowable(() -> new RabbitMqTeardown(primary)
                .run(() -> throwUndeclared(interrupted))
                .run(() -> ran.add("second"))
                .finish());
        boolean interruptStatusSet = Thread.interrupted();

        assertThat(ran).containsExactly("second");
        assertThat(thrown).isNull();
        assertThat(primary.getSuppressed()).containsExactly(interrupted);
        assertThat(interruptStatusSet).isTrue();
    }

    /**
     * Every combination of a primary failure present or not, which of four teardown steps fail, and whether a failing
     * step throws the primary itself or something else.
     */
    @ParameterizedTest(name = "primary {0}, step failures {1}, failing steps {2}")
    @MethodSource("teardownScenarios")
    void whatever_fails_every_step_runs_and_the_primary_failure_is_what_leaves(PrimaryKind primaryKind, StepFailureKind stepFailureKind, int failingSteps) {
        Throwable primary = primaryKind.create();
        List<Integer> ran = new ArrayList<>();
        List<Throwable> distinctFailures = new ArrayList<>();
        RabbitMqTeardown teardown = new RabbitMqTeardown(primary);

        Throwable thrown = catchThrowable(() -> {
            for (int step = 0; step < TEARDOWN_STEPS; step++) {
                int current = step;
                Throwable failure = (failingSteps & (1 << step)) == 0 ? null : stepFailureKind.create(primary, step);
                if (failure != null && failure != primary) {
                    distinctFailures.add(failure);
                }
                teardown.run(() -> {
                    ran.add(current);
                    throwIfAny(failure);
                });
            }
            teardown.finish();
        });

        assertThat(ran).containsExactly(0, 1, 2, 3);
        if (primary != null) {
            assertThat(thrown).as("what finish throws when there is a primary failure").isNull();
            assertThat(primary.getSuppressed()).containsExactlyElementsOf(distinctFailures);
        } else if (distinctFailures.isEmpty()) {
            assertThat(thrown).as("what finish throws when nothing failed").isNull();
        } else {
            assertThat(thrown).isSameAs(distinctFailures.get(0));
            assertThat(thrown.getSuppressed()).containsExactlyElementsOf(distinctFailures.subList(1, distinctFailures.size()));
        }
    }

    private static final int TEARDOWN_STEPS = 4;

    private enum PrimaryKind {
        NONE, RUNTIME_EXCEPTION, ERROR;

        Throwable create() {
            return switch (this) {
                case NONE -> null;
                case RUNTIME_EXCEPTION -> new IllegalStateException("primary");
                case ERROR -> new StackOverflowError("primary");
            };
        }
    }

    private enum StepFailureKind {
        DISTINCT, THE_PRIMARY_ITSELF;

        // Alternates between an exception and an Error, since the steps in the sources throw both.
        Throwable create(Throwable primary, int step) {
            if (this == THE_PRIMARY_ITSELF) {
                return primary;
            }
            return step % 2 == 0 ? new IllegalArgumentException("step " + step) : new LinkageError("step " + step);
        }
    }

    private static Stream<Arguments> teardownScenarios() {
        List<Arguments> scenarios = new ArrayList<>();
        for (int failingSteps = 0; failingSteps < (1 << TEARDOWN_STEPS); failingSteps++) {
            scenarios.add(Arguments.of(PrimaryKind.NONE, StepFailureKind.DISTINCT, failingSteps));
            for (PrimaryKind primaryKind : new PrimaryKind[]{PrimaryKind.RUNTIME_EXCEPTION, PrimaryKind.ERROR}) {
                for (StepFailureKind stepFailureKind : StepFailureKind.values()) {
                    scenarios.add(Arguments.of(primaryKind, stepFailureKind, failingSteps));
                }
            }
        }
        return scenarios.stream();
    }

    private static void throwIfAny(Throwable failure) {
        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (failure instanceof Error error) {
            throw error;
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void throwUndeclared(Throwable failure) throws T {
        throw (T) failure;
    }

    private static RuntimeException withoutSuppression(String message) {
        return new RuntimeException(message, null, false, true) {
        };
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
