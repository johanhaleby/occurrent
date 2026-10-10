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

package org.occurrent.dsl.saga.blocking;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayName("SagaRunnerConfig")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaRunnerConfigTest {

    private static final SagaRunnerConfig NON_DEFAULT = new SagaRunnerConfig(Duration.ofSeconds(7), 13, 9, RedeliveryDetection.BEST_EFFORT, Optional.of(Duration.ofSeconds(42)));

    @Nested
    @DisplayName("when quarantine is disabled")
    class When_quarantine_is_disabled {

        @Test
        void has_an_empty_quarantineAfter() {
            // When
            SagaRunnerConfig config = NON_DEFAULT.disableQuarantine();

            // Then
            assertThat(config.quarantineAfter()).isEmpty();
        }

        @Test
        void keeps_every_other_component_unchanged() {
            // When
            SagaRunnerConfig config = NON_DEFAULT.disableQuarantine();

            // Then
            assertAll(
                    () -> assertThat(config.timerPollInterval()).isEqualTo(Duration.ofSeconds(7)),
                    () -> assertThat(config.timerBatchLimit()).isEqualTo(13),
                    () -> assertThat(config.maxCasAttempts()).isEqualTo(9),
                    () -> assertThat(config.redeliveryDetection()).isEqualTo(RedeliveryDetection.BEST_EFFORT)
            );
        }

        @Test
        void leaves_the_original_configuration_untouched() {
            // When
            NON_DEFAULT.disableQuarantine();

            // Then
            assertThat(NON_DEFAULT.quarantineAfter()).contains(Duration.ofSeconds(42));
        }

        @Test
        void is_turned_back_on_by_withQuarantineAfter() {
            // Given
            SagaRunnerConfig disabled = NON_DEFAULT.disableQuarantine();

            // When
            SagaRunnerConfig config = disabled.withQuarantineAfter(Duration.ofMinutes(2));

            // Then
            assertThat(config.quarantineAfter()).contains(Duration.ofMinutes(2));
        }
    }

    @Nested
    @DisplayName("when no quarantine budget is asked for")
    class When_no_quarantine_budget_is_asked_for {

        @Test
        void defaults_never_quarantine() {
            // When
            SagaRunnerConfig config = SagaRunnerConfig.defaults();

            // Then
            assertThat(config.quarantineAfter()).isEmpty();
        }

        @Test
        void the_three_argument_constructor_never_quarantines() {
            // When
            SagaRunnerConfig config = new SagaRunnerConfig(Duration.ofSeconds(7), 13, 9);

            // Then
            assertThat(config.quarantineAfter()).isEmpty();
        }

        @Test
        void the_four_argument_constructor_never_quarantines() {
            // When
            SagaRunnerConfig config = new SagaRunnerConfig(Duration.ofSeconds(7), 13, 9, RedeliveryDetection.BEST_EFFORT);

            // Then
            assertThat(config.quarantineAfter()).isEmpty();
        }

        @Test
        void is_turned_on_by_withQuarantineAfter_on_the_defaults() {
            // When
            SagaRunnerConfig config = SagaRunnerConfig.defaults().withQuarantineAfter(Duration.ofMinutes(5));

            // Then
            assertThat(config.quarantineAfter()).isEqualTo(Optional.of(Duration.ofMinutes(5)));
        }
    }

    @Nested
    @DisplayName("when a quarantine budget is configured")
    class When_a_quarantine_budget_is_configured {

        @Test
        void withQuarantineAfter_replaces_the_budget_and_keeps_every_other_component() {
            // When
            SagaRunnerConfig config = NON_DEFAULT.withQuarantineAfter(Duration.ofSeconds(1));

            // Then
            assertAll(
                    () -> assertThat(config.quarantineAfter()).contains(Duration.ofSeconds(1)),
                    () -> assertThat(config.timerPollInterval()).isEqualTo(Duration.ofSeconds(7)),
                    () -> assertThat(config.timerBatchLimit()).isEqualTo(13),
                    () -> assertThat(config.maxCasAttempts()).isEqualTo(9),
                    () -> assertThat(config.redeliveryDetection()).isEqualTo(RedeliveryDetection.BEST_EFFORT)
            );
        }

        @Test
        void accepts_the_smallest_positive_budget() {
            // When
            SagaRunnerConfig config = NON_DEFAULT.withQuarantineAfter(Duration.ofNanos(1));

            // Then
            assertThat(config.quarantineAfter()).contains(Duration.ofNanos(1));
        }

        @Test
        void throws_NullPointerException_naming_disableQuarantine_when_withQuarantineAfter_is_given_null() {
            // When
            Throwable thrown = catchThrowable(() -> NON_DEFAULT.withQuarantineAfter(null));

            // Then
            assertThat(thrown)
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("disableQuarantine()");
        }

        @Test
        void throws_NullPointerException_naming_disableQuarantine_when_the_constructor_is_given_a_null_Optional() {
            // When
            Throwable thrown = catchThrowable(() -> new SagaRunnerConfig(Duration.ofSeconds(7), 13, 9, RedeliveryDetection.REQUIRED, null));

            // Then
            assertThat(thrown)
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("disableQuarantine()");
        }

        @Test
        void throws_IllegalArgumentException_when_withQuarantineAfter_is_given_zero() {
            // When
            Throwable thrown = catchThrowable(() -> NON_DEFAULT.withQuarantineAfter(Duration.ZERO));

            // Then
            assertThat(thrown)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("quarantineAfter must be positive");
        }

        @ParameterizedTest
        @ValueSource(longs = {-1, -300_000})
        @DisplayName("throws IllegalArgumentException when withQuarantineAfter is given a negative budget")
        void throws_IllegalArgumentException_when_withQuarantineAfter_is_given_a_negative_budget(long millis) {
            // When
            Throwable thrown = catchThrowable(() -> NON_DEFAULT.withQuarantineAfter(Duration.ofMillis(millis)));

            // Then
            assertThat(thrown)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("quarantineAfter must be positive");
        }

        @Test
        void throws_IllegalArgumentException_when_the_constructor_is_given_a_zero_budget() {
            // When
            Throwable thrown = catchThrowable(() -> new SagaRunnerConfig(Duration.ofSeconds(7), 13, 9, RedeliveryDetection.REQUIRED, Optional.of(Duration.ZERO)));

            // Then
            assertThat(thrown)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("quarantineAfter must be positive");
        }
    }
}
