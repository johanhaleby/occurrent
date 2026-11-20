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

package org.occurrent.springboot.blocking;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.dsl.saga.blocking.SagaRunnerConfig;
import org.occurrent.springboot.common.OccurrentProperties;

import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A saga on the annotation path takes its quarantine budget from {@code occurrent.saga.quarantine-after}. Leaving it out
 * gives the runner's default of five minutes, and {@code occurrent.saga.quarantine-enabled=false} turns quarantine off.
 * Zero is refused rather than read as off, because it reads as "never" as easily as "immediately", and a negative value
 * is refused as well.
 */
@DisplayName("The saga quarantine budget property")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaQuarantineBudgetPropertyTest {

    @Test
    void has_no_value_of_its_own_and_quarantine_is_on_with_the_runner_default_of_five_minutes() {
        OccurrentProperties.SagaProperties properties = new OccurrentProperties.SagaProperties();

        assertAll(
                () -> assertThat(properties.getQuarantineAfter()).isNull(),
                () -> assertThat(properties.isQuarantineEnabled()).isTrue(),
                () -> assertThat(SagaRunnerConfig.defaults().quarantineAfter()).contains(Duration.ofMinutes(5))
        );
    }

    @Test
    void quarantines_after_five_minutes_when_the_property_is_not_set() {
        Duration unset = new OccurrentProperties.SagaProperties().getQuarantineAfter();

        // Starts from a config with quarantine off, so the five minutes come from the unset property and not from it
        assertThat(SagaAnnotationRegistrar.withQuarantineBudget(SagaRunnerConfig.defaults().disableQuarantine(), unset).quarantineAfter())
                .contains(Duration.ofMinutes(5));
    }

    @Test
    void quarantines_after_five_minutes_when_neither_the_budget_nor_the_switch_is_set() {
        OccurrentProperties.SagaProperties properties = new OccurrentProperties.SagaProperties();

        SagaRunnerConfig config = SagaAnnotationRegistrar.withQuarantine(SagaRunnerConfig.defaults().disableQuarantine(), properties);

        assertThat(config.quarantineAfter()).contains(Duration.ofMinutes(5));
    }

    @Test
    void turns_quarantine_off_when_it_is_disabled_even_with_a_budget_set() {
        OccurrentProperties.SagaProperties properties = new OccurrentProperties.SagaProperties();
        properties.setQuarantineAfter(Duration.ofSeconds(30));
        properties.setQuarantineEnabled(false);

        assertThat(SagaAnnotationRegistrar.withQuarantine(SagaRunnerConfig.defaults(), properties).quarantineAfter()).isEmpty();
    }

    @Test
    void passes_the_budget_to_the_runner() {
        OccurrentProperties.SagaProperties properties = new OccurrentProperties.SagaProperties();
        properties.setQuarantineAfter(Duration.ofSeconds(30));

        assertThat(SagaAnnotationRegistrar.withQuarantine(SagaRunnerConfig.defaults(), properties).quarantineAfter()).contains(Duration.ofSeconds(30));
    }

    @Test
    void turns_quarantine_on_with_five_minutes() {
        assertThat(SagaAnnotationRegistrar.withQuarantineBudget(SagaRunnerConfig.defaults(), Duration.ofMinutes(5)).quarantineAfter())
                .isEqualTo(Optional.of(Duration.ofMinutes(5)));
    }

    @Test
    void passes_a_configured_budget_through_unchanged() {
        assertThat(SagaAnnotationRegistrar.withQuarantineBudget(SagaRunnerConfig.defaults(), Duration.ofSeconds(30)).quarantineAfter())
                .isEqualTo(Optional.of(Duration.ofSeconds(30)));
    }

    @Test
    void refuses_a_zero_budget_passed_straight_to_the_config_rather_than_quarantining_on_the_first_failure() {
        assertThatThrownBy(() -> SagaRunnerConfig.defaults().withQuarantineAfter(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("quarantineAfter must be positive");
    }

    @Test
    void throws_IllegalArgumentException_for_a_zero_budget_rather_than_reading_it_as_never() {
        assertThatThrownBy(() -> SagaAnnotationRegistrar.withQuarantineBudget(SagaRunnerConfig.defaults(), Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("occurrent.saga.quarantine-after")
                .hasMessageContaining("must be a positive duration")
                .hasMessageContaining("Leave the property out");
    }

    @Test
    void throws_IllegalArgumentException_for_a_negative_budget_rather_than_reading_a_typo_as_never() {
        assertThatThrownBy(() -> SagaAnnotationRegistrar.withQuarantineBudget(SagaRunnerConfig.defaults(), Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("occurrent.saga.quarantine-after")
                .hasMessageContaining("must be a positive duration")
                .hasMessageContaining("Leave the property out");
    }
}
