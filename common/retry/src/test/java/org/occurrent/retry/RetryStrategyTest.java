package org.occurrent.retry;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.retry.RetryStrategy.Retry;

import java.time.Duration;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.fail;
import static org.occurrent.retry.Backoff.*;

@DisplayName("Retry Strategy")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(10)
public class RetryStrategyTest {

    @Test
    void does_not_retry_when_retry_strategy_is_none() {
        // Given
        RetryStrategy retryStrategy = RetryStrategy.none();

        AtomicInteger counter = new AtomicInteger(0);

        // When
        Throwable throwable = catchThrowable(() -> retryStrategy.execute(() -> {
            if (counter.incrementAndGet() == 1) {
                throw new IllegalArgumentException("expected");
            }
        }));

        // Then
        assertAll(
                () -> assertThat(counter).hasValue(1),
                () -> assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class).hasMessage("expected")
        );
    }

    @Timeout(2000)
    @Test
    void example_of_retry_executed_with_function_that_takes_retry_info() {
        // Given
        CopyOnWriteArrayList<RetryInfo> retryInfos = new CopyOnWriteArrayList<>();
        Retry retryStrategy = RetryStrategy.retry()
                .maxAttempts(4);

        AtomicInteger counter = new AtomicInteger(0);

        // When
        Throwable throwable = catchThrowable(() -> retryStrategy.execute(info -> {
            retryInfos.add(info);
            counter.incrementAndGet();
            throw new IllegalArgumentException("expected");
        }));

        // Then
        assertAll(
                () -> assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class).hasMessage("expected"),
                () -> assertThat(counter).hasValue(4),
                () -> assertThat(retryInfos).hasSize(4),
                () -> assertThat(retryInfos).extracting(RetryInfo::getRetryCount).containsExactly(0, 1, 2, 3),
                () -> assertThat(retryInfos).extracting(RetryInfo::getAttemptNumber).containsExactly(1, 2, 3, 4),
                () -> assertThat(retryInfos).extracting(RetryInfo::getBackoff).containsExactly(Duration.ofMillis(0), Duration.ofMillis(0), Duration.ofMillis(0), Duration.ofMillis(0)),
                () -> assertThat(retryInfos).extracting(RetryInfo::getMaxAttempts).containsExactly(4, 4, 4, 4),
                () -> assertThat(retryInfos).extracting(RetryInfo::getAttemptsLeft).containsExactly(4, 3, 2, 1),
                () -> assertThat(retryInfos).extracting(RetryInfo::isFirstAttempt).containsExactly(true, false, false, false),
                () -> assertThat(retryInfos).extracting(RetryInfo::isLastAttempt).containsExactly(false, false, false, true),
                () -> assertThat(retryInfos).extracting(RetryInfo::isInfiniteRetriesLeft).containsExactly(false, false, false, false)
        );
    }

    @Nested
    @DisplayName("MapError")
    class MapErrorTest {

        @Test
        void mapError_for_all_exceptions() {
            // Given
            Retry retryStrategy = RetryStrategy.retry().backoff(fixed(10));

            // When
            Throwable throwable = catchThrowable(() ->
                    retryStrategy
                            .mapError(t -> {
                                if (t instanceof IllegalArgumentException iae) {
                                    return new IllegalStateException(iae.getMessage());
                                } else {
                                    return t;
                                }
                            })
                            .maxAttempts(2)
                            .execute(() -> {
                                throw new IllegalArgumentException("expected");
                            }));

            // Then
            assertAll(
                    () -> assertThat(throwable).isExactlyInstanceOf(IllegalStateException.class).hasMessage("expected")
            );
        }

        @Nested
        @DisplayName("mapError for specific exception")
        class MapErrorForSpecificExceptionTest {

            @Test
            void maps_exception_to_another_exception_when_mapError_matches_the_thrown_exception() {
                // Given
                Retry retryStrategy = RetryStrategy.retry().backoff(fixed(10));

                // When
                Throwable throwable = catchThrowable(() ->
                        retryStrategy
                                .mapError(IllegalArgumentException.class, IllegalStateException::new)
                                .maxAttempts(2)
                                .execute(() -> {
                                    throw new IllegalArgumentException("expected");
                                }));

                // Then
                assertAll(
                        () -> assertThat(throwable).isExactlyInstanceOf(IllegalStateException.class).hasRootCauseMessage("expected")
                );
            }

            @Test
            void maps_exception_to_another_exception_when_mapError_matches_the_thrown_exception_and_there_are_multiple_error_mappers_defined() {
                // Given
                Retry retryStrategy = RetryStrategy.retry().backoff(fixed(10));

                // When
                Throwable throwable = catchThrowable(() ->
                        retryStrategy
                                .mapError(IllegalStateException.class, IllegalArgumentException::new)
                                .mapError(IllegalArgumentException.class, IllegalStateException::new)
                                .maxAttempts(2)
                                .execute(() -> {
                                    throw new IllegalArgumentException("expected");
                                }));

                // Then
                assertAll(
                        () -> assertThat(throwable).isExactlyInstanceOf(IllegalStateException.class).hasRootCauseMessage("expected")
                );
            }

            @Test
            void maps_exception_to_another_exception_when_mapError_matches_the_thrown_exception_and_there_are_multiple_error_mappers_defined_for_the_same_exception_then_the_first_has_precedence() {
                // Given
                Retry retryStrategy = RetryStrategy.retry().backoff(fixed(10));

                // When
                Throwable throwable = catchThrowable(() ->
                        retryStrategy
                                .mapError(IllegalArgumentException.class, IllegalStateException::new)
                                .mapError(IllegalArgumentException.class, RuntimeException::new)
                                .maxAttempts(2)
                                .execute(() -> {
                                    throw new IllegalArgumentException("expected");
                                }));

                // Then
                assertAll(
                        () -> assertThat(throwable).isExactlyInstanceOf(IllegalStateException.class).hasRootCauseMessage("expected")
                );
            }

            @Test
            void example_of_multiple_mapErrors_in_a_chain() {
                // Given
                Retry retryStrategy = RetryStrategy.retry().backoff(fixed(10));

                // When
                Throwable throwable = catchThrowable(() ->
                        retryStrategy
                                .mapError(IllegalArgumentException.class, IllegalStateException::new)
                                .mapError(IllegalStateException.class, RuntimeException::new)
                                .maxAttempts(2)
                                .execute(() -> {
                                    throw new IllegalArgumentException("expected");
                                }));

                // Then
                assertAll(
                        () -> assertThat(throwable).isExactlyInstanceOf(RuntimeException.class).hasCauseExactlyInstanceOf(IllegalStateException.class)
                                .hasRootCauseExactlyInstanceOf(IllegalArgumentException.class).hasRootCauseMessage("expected")
                );
            }
        }
    }

    @Nested
    @DisplayName("backoff")
    class BackoffTest {

        @Test
        void fixed_with_int_retries_according_to_specification() {
            // Given
            Retry retryStrategy = RetryStrategy.retry().backoff(fixed(100));

            AtomicInteger counter = new AtomicInteger(0);

            // When
            final long startTime = System.currentTimeMillis();
            retryStrategy.execute(() -> {
                if (counter.incrementAndGet() == 1) {
                    throw new IllegalArgumentException("expected");
                }
            });
            final long endTime = System.currentTimeMillis();

            // Then
            assertAll(
                    () -> assertThat(endTime - startTime).isGreaterThanOrEqualTo(100).isLessThan(150),
                    () -> assertThat(counter).hasValue(2)
            );
        }

        @Test
        void fixed_with_duration_retries_according_to_specification() {
            // Given
            Retry retryStrategy = RetryStrategy.retry().backoff(fixed(Duration.ofMillis(100)));

            AtomicInteger counter = new AtomicInteger(0);

            // When
            final long startTime = System.currentTimeMillis();
            retryStrategy.execute(() -> {
                if (counter.incrementAndGet() == 1) {
                    throw new IllegalArgumentException("expected");
                }
            });
            final long endTime = System.currentTimeMillis();

            // Then
            assertAll(
                    () -> assertThat(endTime - startTime).isGreaterThanOrEqualTo(100).isLessThan(150),
                    () -> assertThat(counter).hasValue(2)
            );
        }

        @Test
        void exponential_backoff_retries_according_to_specification() {
            // Given
            Retry retryStrategy = RetryStrategy.retry().backoff(exponential(Duration.ofMillis(50), Duration.ofMillis(200), 2.0));

            AtomicInteger counter = new AtomicInteger(0);

            // When
            final long startTime = System.currentTimeMillis();
            retryStrategy.execute(() -> {
                if (counter.incrementAndGet() <= 4) {
                    throw new IllegalArgumentException("expected");
                }
            });
            final long endTime = System.currentTimeMillis();

            // Then
            assertAll(
                    () -> assertThat(endTime - startTime).isGreaterThanOrEqualTo(50 + 100 + 200 + 200).isLessThan(700),
                    () -> assertThat(counter).hasValue(5)
            );
        }

        @Test
        void none_does_retries_without_delay() {
            // Given
            Retry retryStrategy = RetryStrategy.retry().backoff(none());

            AtomicInteger counter = new AtomicInteger(0);

            // When
            final long startTime = System.currentTimeMillis();
            retryStrategy.execute(() -> {
                if (counter.incrementAndGet() == 1) {
                    throw new IllegalArgumentException("expected");
                }
            });
            final long endTime = System.currentTimeMillis();

            // Then
            assertAll(
                    () -> assertThat(counter).hasValue(2),
                    () -> assertThat(endTime - startTime).isLessThan(100)
            );
        }
    }

    @Nested
    @DisplayName("max attempts")
    class MaxAttemptsTest {

        @Test
        void retries_at_most_the_number_of_specified_attempts_when_defined_with_int() {
            // Given
            Retry retryStrategy = RetryStrategy.retry().maxAttempts(3);

            AtomicInteger counter = new AtomicInteger(0);

            // When
            Throwable throwable = catchThrowable(() -> retryStrategy.execute(() -> {
                counter.incrementAndGet();
                throw new IllegalArgumentException("expected");
            }));

            // Then
            assertAll(
                    () -> assertThat(counter).hasValue(3),
                    () -> assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class).hasMessage("expected")
            );
        }

        @Test
        void retries_infinite_amount_of_times_when_infinite_attempts_are_specified() {
            // Given
            Retry retryStrategy = RetryStrategy.retry().infiniteAttempts();

            AtomicInteger counter = new AtomicInteger(0);

            // When
            retryStrategy.execute(() -> {
                if (counter.incrementAndGet() < 100) {
                    throw new IllegalArgumentException("expected");
                }
            });

            // Then
            assertThat(counter).hasValue(100);
        }
    }

    @Nested
    @DisplayName("retry if")
    class RetryIfTest {

        @Timeout(2000)
        @Test
        void only_retries_if_retry_predicate_is_fulfilled() {
            // Given
            AtomicInteger counter = new AtomicInteger(0);
            Retry retryStrategy = RetryStrategy.retry().retryIf(__ -> counter.getAndIncrement() < 3);


            // When
            Throwable throwable = catchThrowable(() -> retryStrategy.execute(() -> {
                throw new IllegalArgumentException("expected");
            }));

            // Then
            assertAll(
                    () -> assertThat(counter).hasValue(4),
                    () -> assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class).hasMessage("expected")
            );
        }

        @Timeout(2000)
        @Test
        void only_retries_if_retry_predicate_is_fulfilled_when_taking_throwable_into_account() {
            // Given
            AtomicInteger counter = new AtomicInteger(0);
            Retry retryStrategy = RetryStrategy.retry().retryIf(IllegalArgumentException.class::isInstance);


            // When
            Throwable throwable = catchThrowable(() -> retryStrategy.execute(() -> {
                if (counter.incrementAndGet() < 4) {
                    throw new IllegalArgumentException("expected");
                }
                throw new IllegalStateException("expected");
            }));

            // Then
            assertAll(
                    () -> assertThat(counter).hasValue(4),
                    () -> assertThat(throwable).isExactlyInstanceOf(IllegalStateException.class).hasMessage("expected")
            );
        }

        @Test
        void on_before_retry_listener_retry_info_data_is_correct_when_max_attempts_and_exponential_backoff_is_specified() {
            // Given
            CopyOnWriteArrayList<RetryInfo> retryInfos = new CopyOnWriteArrayList<>();
            Retry retryStrategy = RetryStrategy
                    .exponentialBackoff(Duration.ofMillis(1), Duration.ofMillis(10), 2.0)
                    .maxAttempts(40)
                    .onBeforeRetry((__, info) -> retryInfos.add(info));

            AtomicInteger counter = new AtomicInteger(0);

            // When
            retryStrategy.execute(() -> {
                if (counter.incrementAndGet() <= 3) {
                    throw new IllegalArgumentException("expected");
                }
            });

            // Then
            assertAll(
                    () -> assertThat(counter).hasValue(4),
                    () -> assertThat(retryInfos).hasSize(3),
                    () -> assertThat(retryInfos).extracting(RetryInfo::getRetryCount).containsExactly(1, 2, 3),
                    () -> assertThat(retryInfos).extracting(RetryInfo::getAttemptNumber).containsExactly(2, 3, 4),
                    () -> assertThat(retryInfos).extracting(RetryInfo::getNumberOfPreviousAttempts).containsExactly(1, 2, 3),
                    () -> assertThat(retryInfos).extracting(RetryInfo::getBackoff).containsExactly(Duration.ofMillis(1), Duration.ofMillis(2), Duration.ofMillis(4)),
                    () -> assertThat(retryInfos).extracting(RetryInfo::getMaxAttempts).containsExactly(40, 40, 40),
                    () -> assertThat(retryInfos).extracting(RetryInfo::getAttemptsLeft).containsExactly(39, 38, 37),
                    () -> assertThat(retryInfos).extracting(RetryInfo::isFirstAttempt).containsExactly(false, false, false),
                    () -> assertThat(retryInfos).extracting(RetryInfo::isLastAttempt).containsExactly(false, false, false),
                    () -> assertThat(retryInfos).extracting(RetryInfo::isInfiniteRetriesLeft).containsExactly(false, false, false)
            );
        }
    }

    @Nested
    @DisplayName("error listener")
    class ErrorListenerTest {

        @Test
        void error_listener_is_invoked_for_both_final_and_intermediate_errors() {
            // Given
            CopyOnWriteArrayList<Throwable> throwables = new CopyOnWriteArrayList<>();
            Retry retryStrategy = RetryStrategy.retry().onError((throwable, __) -> throwables.add(throwable));

            AtomicInteger counter = new AtomicInteger(0);

            // When
            retryStrategy.execute(() -> {
                if (counter.incrementAndGet() <= 4) {
                    throw new IllegalArgumentException("expected");
                }
            });

            // Then
            assertAll(
                    () -> assertThat(counter).hasValue(5),
                    () -> assertThat(throwables)
                            .hasSize(4)
                            .extracting(Throwable::getClass, Throwable::getMessage).containsOnly(tuple(IllegalArgumentException.class, "expected"))
            );
        }

        @Test
        void error_listener_is_invoked_when_end_result_is_error() {
            // Given
            CopyOnWriteArrayList<Throwable> throwables = new CopyOnWriteArrayList<>();
            Retry retryStrategy = RetryStrategy.retry().maxAttempts(5).onError(throwable -> throwables.add(throwable));

            AtomicInteger counter = new AtomicInteger(0);

            // When
            Throwable throwable = catchThrowable(() -> retryStrategy.execute(() -> {
                counter.incrementAndGet();
                throw new IllegalArgumentException("expected");
            }));

            // Then
            assertAll(
                    () -> assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class).hasMessage("expected"),
                    () -> assertThat(counter).hasValue(5),
                    () -> assertThat(throwables).hasSize(5),
                    () -> assertThat(throwables).extracting(Throwable::getClass, Throwable::getMessage).containsOnly(tuple(IllegalArgumentException.class, "expected"))
            );
        }

        @Test
        void error_listener_supports_checking_if_error_is_retryable_when_retry_predicate_is_defined() {
            // Given
            CopyOnWriteArrayList<Throwable> retryableExceptions = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<Throwable> finalException = new CopyOnWriteArrayList<>();
            AtomicInteger counter = new AtomicInteger(0);
            int millis = 150;
            Retry retryStrategy = RetryStrategy.fixed(millis)
                    .onError((throwable, info) -> {
                        if (info.isRetryable()) {
                            retryableExceptions.add(throwable);
                        } else {
                            finalException.add(throwable);
                        }
                    })
                    .retryIf(e -> e instanceof IllegalArgumentException && e.getMessage().equals("intermediate"));

            // When
            final long startTime = System.currentTimeMillis();
            Throwable throwable = catchThrowable(() -> retryStrategy.execute(() -> {
                int count = counter.incrementAndGet();
                if (count <= 4) {
                    throw new IllegalArgumentException("intermediate");
                } else {
                    throw new IllegalArgumentException("final");
                }
            }));
            final long endTime = System.currentTimeMillis();

            // Then
            assertAll(
                    () -> assertThat(counter).hasValue(5),
                    () -> assertThat(retryableExceptions).hasSize(4),
                    () -> assertThat(retryableExceptions).extracting(Throwable::getClass, Throwable::getMessage).containsOnly(tuple(IllegalArgumentException.class, "intermediate")),
                    () -> assertThat(finalException).hasSize(1),
                    () -> assertThat(finalException).extracting(Throwable::getClass, Throwable::getMessage).containsOnly(tuple(IllegalArgumentException.class, "final")),
                    () -> assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class).hasMessage("final"),
                    () -> assertThat(endTime - startTime).isGreaterThanOrEqualTo(4 * millis).isLessThan(5 * millis)
            );
        }

        @Test
        void error_listener_supports_checking_if_error_is_retryable_when_no_retry_predicate_defined() {
            // Given
            CopyOnWriteArrayList<Throwable> throwables = new CopyOnWriteArrayList<>();
            AtomicInteger counter = new AtomicInteger(0);
            int millis = 150;
            Retry retryStrategy = RetryStrategy.fixed(millis)
                    .onError((throwable, info) -> {
                        if (info.isRetryable()) {
                            throwables.add(throwable);
                        } else {
                            fail("Expected retryable");
                        }
                    });

            // When
            retryStrategy.execute(() -> {
                int count = counter.incrementAndGet();
                if (count <= 4) {
                    throw new IllegalArgumentException("intermediate");
                } else if (count == 5) {
                    throw new IllegalArgumentException("final");
                }
            });

            // Then
            assertAll(
                    () -> assertThat(counter).hasValue(6),
                    () -> assertThat(throwables).hasSize(5),
                    () -> assertThat(throwables.subList(0, 4)).extracting(Throwable::getClass, Throwable::getMessage).containsOnly(tuple(IllegalArgumentException.class, "intermediate")),
                    () -> assertThat(throwables.subList(4, 5)).extracting(Throwable::getClass, Throwable::getMessage).containsOnly(tuple(IllegalArgumentException.class, "final"))
            );
        }
    }

    @Nested
    @DisplayName("on before retry listener")
    class OnBeforeRetry {

        @Test
        void on_before_retry_listener_retry_info_data_is_correct_when_max_attempts_and_exponential_backoff_is_specified() {
            // Given
            CopyOnWriteArrayList<RetryInfo> retryInfos = new CopyOnWriteArrayList<>();
            Retry retryStrategy = RetryStrategy
                    .exponentialBackoff(Duration.ofMillis(1), Duration.ofMillis(10), 2.0)
                    .maxAttempts(40)
                    .onBeforeRetry((__, info) -> retryInfos.add(info));

            AtomicInteger counter = new AtomicInteger(0);

            // When
            retryStrategy.execute(() -> {
                if (counter.incrementAndGet() <= 3) {
                    throw new IllegalArgumentException("expected");
                }
            });

            // Then
            assertAll(
                    () -> assertThat(counter).hasValue(4),
                    () -> assertThat(retryInfos).hasSize(3),
                    () -> assertThat(retryInfos).extracting(RetryInfo::getRetryCount).containsExactly(1, 2, 3),
                    () -> assertThat(retryInfos).extracting(RetryInfo::getAttemptNumber).containsExactly(2, 3, 4),
                    () -> assertThat(retryInfos).extracting(RetryInfo::getBackoff).containsExactly(Duration.ofMillis(1), Duration.ofMillis(2), Duration.ofMillis(4)),
                    () -> assertThat(retryInfos).extracting(RetryInfo::getMaxAttempts).containsExactly(40, 40, 40),
                    () -> assertThat(retryInfos).extracting(RetryInfo::getAttemptsLeft).containsExactly(39, 38, 37),
                    () -> assertThat(retryInfos).extracting(RetryInfo::isFirstAttempt).containsExactly(false, false, false),
                    () -> assertThat(retryInfos).extracting(RetryInfo::isLastAttempt).containsExactly(false, false, false),
                    () -> assertThat(retryInfos).extracting(RetryInfo::isInfiniteRetriesLeft).containsExactly(false, false, false)
            );
        }

        @Test
        void on_before_retry_retry_info_data_is_correct_when_infinite_max_attempts_and_fixed_backoff_is_specified() {
            // Given
            CopyOnWriteArrayList<RetryInfo> retryInfos = new CopyOnWriteArrayList<>();
            Retry retryStrategy = RetryStrategy
                    .fixed(10)
                    .infiniteAttempts()
                    .onBeforeRetry((__, info) -> retryInfos.add(info));

            AtomicInteger counter = new AtomicInteger(0);

            // When
            retryStrategy.execute(() -> {
                if (counter.incrementAndGet() <= 3) {
                    throw new IllegalArgumentException("expected");
                }
            });

            // Then
            assertAll(
                    () -> assertThat(counter).hasValue(4),
                    () -> assertThat(retryInfos).hasSize(3),
                    () -> assertThat(retryInfos).extracting(RetryInfo::getRetryCount).containsExactly(1, 2, 3),
                    () -> assertThat(retryInfos).extracting(RetryInfo::getAttemptNumber).containsExactly(2, 3, 4),
                    () -> assertThat(retryInfos).extracting(RetryInfo::getBackoff).containsExactly(Duration.ofMillis(10), Duration.ofMillis(10), Duration.ofMillis(10)),
                    () -> assertThat(retryInfos).extracting(RetryInfo::getMaxAttempts).containsExactly(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE),
                    () -> assertThat(retryInfos).extracting(RetryInfo::getAttemptsLeft).containsExactly(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE),
                    () -> assertThat(retryInfos).extracting(RetryInfo::isFirstAttempt).containsExactly(false, false, false),
                    () -> assertThat(retryInfos).extracting(RetryInfo::isLastAttempt).containsExactly(false, false, false),
                    () -> assertThat(retryInfos).extracting(RetryInfo::isInfiniteRetriesLeft).containsExactly(true, true, true)
            );
        }

        @Timeout(2000)
        @Test
        void on_before_retry_listener_retry_info_data_is_correct_when_finite_max_attempts_are_exceeded() {
            // Given
            CopyOnWriteArrayList<RetryInfo> retryInfos = new CopyOnWriteArrayList<>();
            Retry retryStrategy = RetryStrategy.retry()
                    .maxAttempts(4)
                    .onBeforeRetry((__, info) -> retryInfos.add(info));

            AtomicInteger counter = new AtomicInteger(0);

            // When
            Throwable throwable = catchThrowable(() -> retryStrategy.execute(() -> {
                counter.incrementAndGet();
                throw new IllegalArgumentException("expected");
            }));

            // Then
            assertAll(
                    () -> assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class).hasMessage("expected"),
                    () -> assertThat(counter).hasValue(4),
                    () -> assertThat(retryInfos).hasSize(3),
                    () -> assertThat(retryInfos).extracting(RetryInfo::getRetryCount).containsExactly(1, 2, 3),
                    () -> assertThat(retryInfos).extracting(RetryInfo::getNumberOfPreviousAttempts).containsExactly(1, 2, 3),
                    () -> assertThat(retryInfos).extracting(RetryInfo::getAttemptNumber).containsExactly(2, 3, 4),
                    () -> assertThat(retryInfos).extracting(RetryInfo::getBackoff).containsExactly(Duration.ofMillis(0), Duration.ofMillis(0), Duration.ofMillis(0)),
                    () -> assertThat(retryInfos).extracting(RetryInfo::getMaxAttempts).containsExactly(4, 4, 4),
                    () -> assertThat(retryInfos).extracting(RetryInfo::getAttemptsLeft).containsExactly(3, 2, 1),
                    () -> assertThat(retryInfos).extracting(RetryInfo::isFirstAttempt).containsExactly(false, false, false),
                    () -> assertThat(retryInfos).extracting(RetryInfo::isLastAttempt).containsExactly(false, false, true),
                    () -> assertThat(retryInfos).extracting(RetryInfo::isInfiniteRetriesLeft).containsExactly(false, false, false)
            );
        }
    }

    @Nested
    @DisplayName("on after retry listener")
    class OnAfterRetry {

        @Test
        void on_after_retry_listener_retry_info_data_is_correct_when_max_attempts_and_exponential_backoff_is_specified() {
            // Given
            CopyOnWriteArrayList<AfterRetryInfo> retryInfos = new CopyOnWriteArrayList<>();
            Retry retryStrategy = RetryStrategy
                    .exponentialBackoff(Duration.ofMillis(1), Duration.ofMillis(10), 2.0)
                    .maxAttempts(40)
                    .onAfterRetry((__, info) -> retryInfos.add(info));

            AtomicInteger counter = new AtomicInteger(0);

            // When
            retryStrategy.execute(() -> {
                int count = counter.incrementAndGet();
                if (count <= 3) {
                    throw new IllegalArgumentException("expected: %s".formatted(count));
                }
            });

            // Then
            assertAll(
                    () -> assertThat(counter).hasValue(4),
                    () -> assertThat(retryInfos).hasSize(3),
                    () -> assertThat(retryInfos).extracting(AfterRetryInfo::getRetryCount).containsExactly(1, 2, 3),
                    () -> assertThat(retryInfos).extracting(AfterRetryInfo::getAttemptNumber).containsExactly(2, 3, 4),
                    () -> assertThat(retryInfos).extracting(AfterRetryInfo::getBackoff).containsExactly(Duration.ofMillis(1), Duration.ofMillis(2), Duration.ofMillis(4)),
                    () -> assertThat(retryInfos).extracting(AfterRetryInfo::getBackoffBeforeNextRetryAttempt)
                            .extracting(it -> it.orElse(null))
                            .containsExactly(Duration.ofMillis(2), Duration.ofMillis(4), null),
                    () -> assertThat(retryInfos).extracting(AfterRetryInfo::wasSuccessfulRetryAttempt).containsExactly(false, false, true),
                    () -> assertThat(retryInfos).extracting(AfterRetryInfo::wasFailedRetryAttempt).containsExactly(true, true, false),
                    () -> assertThat(retryInfos).extracting(AfterRetryInfo::getFailedRetryAttemptException)
                            .extracting(it -> it.map(Throwable::getMessage))
                            .extracting(it -> it.orElse(null))
                            .containsExactly("expected: 2", "expected: 3", null),
                    () -> assertThat(retryInfos).extracting(AfterRetryInfo::getMaxAttempts).containsExactly(40, 40, 40),
                    () -> assertThat(retryInfos).extracting(AfterRetryInfo::getAttemptsLeft).containsExactly(39, 38, 37),
                    () -> assertThat(retryInfos).extracting(AfterRetryInfo::isFirstAttempt).containsExactly(false, false, false),
                    () -> assertThat(retryInfos).extracting(AfterRetryInfo::isLastAttempt).containsExactly(false, false, false),
                    () -> assertThat(retryInfos).extracting(AfterRetryInfo::isInfiniteRetriesLeft).containsExactly(false, false, false)
            );
        }

        @Test
        void on_after_retry_retry_info_data_is_correct_when_infinite_max_attempts_and_fixed_backoff_is_specified() {
            // Given
            CopyOnWriteArrayList<RetryInfo> retryInfos = new CopyOnWriteArrayList<>();
            Retry retryStrategy = RetryStrategy
                    .fixed(10)
                    .infiniteAttempts()
                    .onAfterRetry((__, info) -> retryInfos.add(info));

            AtomicInteger counter = new AtomicInteger(0);

            // When
            retryStrategy.execute(() -> {
                if (counter.incrementAndGet() <= 3) {
                    throw new IllegalArgumentException("expected");
                }
            });

            // Then
            assertAll(
                    () -> assertThat(counter).hasValue(4),
                    () -> assertThat(retryInfos).hasSize(3),
                    () -> assertThat(retryInfos).extracting(RetryInfo::getRetryCount).containsExactly(1, 2, 3),
                    () -> assertThat(retryInfos).extracting(RetryInfo::getAttemptNumber).containsExactly(2, 3, 4),
                    () -> assertThat(retryInfos).extracting(RetryInfo::getBackoff).containsExactly(Duration.ofMillis(10), Duration.ofMillis(10), Duration.ofMillis(10)),
                    () -> assertThat(retryInfos).extracting(RetryInfo::getMaxAttempts).containsExactly(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE),
                    () -> assertThat(retryInfos).extracting(RetryInfo::getAttemptsLeft).containsExactly(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE),
                    () -> assertThat(retryInfos).extracting(RetryInfo::isFirstAttempt).containsExactly(false, false, false),
                    () -> assertThat(retryInfos).extracting(RetryInfo::isLastAttempt).containsExactly(false, false, false),
                    () -> assertThat(retryInfos).extracting(RetryInfo::isInfiniteRetriesLeft).containsExactly(true, true, true)
            );
        }

        @Timeout(2000)
        @Test
        void on_after_retry_listener_retry_info_data_is_correct_when_finite_max_attempts_are_exceeded() {
            // Given
            CopyOnWriteArrayList<AfterRetryInfo> retryInfos = new CopyOnWriteArrayList<>();
            Retry retryStrategy = RetryStrategy.retry()
                    .maxAttempts(4)
                    .onAfterRetry((__, info) -> retryInfos.add(info));

            AtomicInteger counter = new AtomicInteger(0);

            // When
            Throwable throwable = catchThrowable(() -> retryStrategy.execute(() -> {
                int count = counter.incrementAndGet();
                throw new IllegalArgumentException("expected: %s".formatted(count));
            }));

            // Then
            assertAll(
                    () -> assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class).hasMessage("expected: 4"),
                    () -> assertThat(counter).hasValue(4),
                    () -> assertThat(retryInfos).hasSize(3),
                    () -> assertThat(retryInfos).extracting(AfterRetryInfo::getRetryCount).containsExactly(1, 2, 3),
                    () -> assertThat(retryInfos).extracting(AfterRetryInfo::getNumberOfPreviousAttempts).containsExactly(1, 2, 3),
                    () -> assertThat(retryInfos).extracting(AfterRetryInfo::getAttemptNumber).containsExactly(2, 3, 4),
                    () -> assertThat(retryInfos).extracting(AfterRetryInfo::getBackoff).containsExactly(Duration.ofMillis(0), Duration.ofMillis(0), Duration.ofMillis(0)),
                    () -> assertThat(retryInfos).extracting(AfterRetryInfo::getMaxAttempts).containsExactly(4, 4, 4),
                    () -> assertThat(retryInfos).extracting(AfterRetryInfo::getAttemptsLeft).containsExactly(3, 2, 1),
                    () -> assertThat(retryInfos).extracting(AfterRetryInfo::isFirstAttempt).containsExactly(false, false, false),
                    () -> assertThat(retryInfos).extracting(AfterRetryInfo::isLastAttempt).containsExactly(false, false, true),
                    () -> assertThat(retryInfos).extracting(AfterRetryInfo::getBackoffBeforeNextRetryAttempt)
                            .extracting(it -> it.orElse(null))
                            .containsExactly(Duration.ZERO, Duration.ZERO, null),
                    () -> assertThat(retryInfos).extracting(AfterRetryInfo::wasSuccessfulRetryAttempt).containsExactly(false, false, false),
                    () -> assertThat(retryInfos).extracting(AfterRetryInfo::wasFailedRetryAttempt).containsExactly(true, true, true),
                    () -> assertThat(retryInfos).extracting(AfterRetryInfo::getFailedRetryAttemptException)
                            .extracting(it -> it.map(Throwable::getMessage))
                            .extracting(it -> it.orElse(null))
                            .containsExactly("expected: 2", "expected: 3", "expected: 4"),
                    () -> assertThat(retryInfos).extracting(AfterRetryInfo::isInfiniteRetriesLeft).containsExactly(false, false, false)
            );
        }
    }

    @Nested
    @DisplayName("use cases")
    class UseCasesTest {

        @Test
        void combining_backoff_with_max_attempts_and_error_listener() {
            // Given
            CopyOnWriteArrayList<Throwable> throwables = new CopyOnWriteArrayList<>();
            int millis = 150;
            Retry retryStrategy = RetryStrategy.fixed(millis).onError(throwable -> throwables.add(throwable)).maxAttempts(5);

            AtomicInteger counter = new AtomicInteger(0);

            // When
            final long startTime = System.currentTimeMillis();
            Throwable throwable = catchThrowable(() -> retryStrategy.execute(() -> {
                counter.incrementAndGet();
                throw new IllegalArgumentException("expected");
            }));
            final long endTime = System.currentTimeMillis();

            // Then
            assertAll(
                    () -> assertThat(counter).hasValue(5),
                    () -> assertThat(throwables).hasSize(5),
                    () -> assertThat(throwables).extracting(Throwable::getClass, Throwable::getMessage).containsOnly(tuple(IllegalArgumentException.class, "expected")),
                    () -> assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class).hasMessage("expected"),
                    () -> assertThat(endTime - startTime).isGreaterThanOrEqualTo(4 * millis).isLessThan(5 * millis)
            );
        }

        @Test
        void combining_backoff_with_max_attempts_and_on_before_retry_listener() {
            // Given
            CopyOnWriteArrayList<Throwable> throwables = new CopyOnWriteArrayList<>();
            int millis = 150;
            Retry retryStrategy = RetryStrategy.fixed(millis).onBeforeRetry(e -> throwables.add(e)).maxAttempts(5);

            AtomicInteger counter = new AtomicInteger(0);

            // When
            final long startTime = System.currentTimeMillis();
            Throwable throwable = catchThrowable(() -> retryStrategy.execute(() -> {
                counter.incrementAndGet();
                throw new IllegalArgumentException("expected");
            }));
            final long endTime = System.currentTimeMillis();

            // Then
            assertAll(
                    () -> assertThat(counter).hasValue(5),
                    () -> assertThat(throwables).hasSize(4),
                    () -> assertThat(throwables).extracting(Throwable::getClass, Throwable::getMessage).containsOnly(tuple(IllegalArgumentException.class, "expected")),
                    () -> assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class).hasMessage("expected"),
                    () -> assertThat(endTime - startTime).isGreaterThanOrEqualTo(4 * millis).isLessThan(5 * millis)
            );
        }

        @Test
        void combining_backoff_and_retry_predicate_and_error_listener() {
            // Given
            CopyOnWriteArrayList<Throwable> throwables = new CopyOnWriteArrayList<>();
            AtomicInteger counter = new AtomicInteger(0);
            int millis = 150;
            Retry retryStrategy = RetryStrategy.fixed(millis)
                    .onError(throwable -> throwables.add(throwable))
                    .retryIf(__ -> counter.get() < 5);


            // When
            final long startTime = System.currentTimeMillis();
            Throwable throwable = catchThrowable(() -> retryStrategy.execute(() -> {
                counter.incrementAndGet();
                throw new IllegalArgumentException("expected");
            }));
            final long endTime = System.currentTimeMillis();

            // Then
            assertAll(
                    () -> assertThat(counter).hasValue(5),
                    () -> assertThat(throwables).hasSize(5),
                    () -> assertThat(throwables).extracting(Throwable::getClass, Throwable::getMessage).containsOnly(tuple(IllegalArgumentException.class, "expected")),
                    () -> assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class).hasMessage("expected"),
                    () -> assertThat(endTime - startTime).isGreaterThanOrEqualTo(4 * millis).isLessThan(5 * millis)
            );
        }

        @Test
        void combining_backoff_retry_predicate_and_on_before_retry_listener() {
            // Given
            CopyOnWriteArrayList<Throwable> throwables = new CopyOnWriteArrayList<>();
            AtomicInteger counter = new AtomicInteger(0);
            int millis = 150;
            Retry retryStrategy = RetryStrategy.fixed(millis).onBeforeRetry((e, __) -> throwables.add(e)).retryIf(__ -> counter.get() < 5);


            // When
            final long startTime = System.currentTimeMillis();
            Throwable throwable = catchThrowable(() -> retryStrategy.execute(() -> {
                counter.incrementAndGet();
                throw new IllegalArgumentException("expected");
            }));
            final long endTime = System.currentTimeMillis();

            // Then
            assertAll(
                    () -> assertThat(counter).hasValue(5),
                    () -> assertThat(throwables).hasSize(4), // Expect one less than the counter, since onBeforeRetry is only called for _retries_
                    () -> assertThat(throwables).extracting(Throwable::getClass, Throwable::getMessage).containsOnly(tuple(IllegalArgumentException.class, "expected")),
                    () -> assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class).hasMessage("expected"),
                    () -> assertThat(endTime - startTime).isGreaterThanOrEqualTo(4 * millis).isLessThan(5 * millis)
            );
        }
    }
}