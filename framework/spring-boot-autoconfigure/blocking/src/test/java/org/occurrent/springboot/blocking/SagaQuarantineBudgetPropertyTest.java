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
 * A saga on the annotation path takes its quarantine budget from {@code occurrent.saga.quarantine-after}, and
 * quarantine stays off until that property is set to a positive duration. A {@code Duration} property that is not set
 * binds to its default rather than to null, so zero is what says "never" here.
 */
@DisplayName("The saga quarantine budget property")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaQuarantineBudgetPropertyTest {

    @Test
    void defaults_to_zero_which_agrees_with_the_runner_never_quarantining_by_default() {
        assertAll(
                () -> assertThat(new OccurrentProperties.SagaProperties().getQuarantineAfter()).isEqualTo(Duration.ZERO),
                () -> assertThat(SagaRunnerConfig.defaults().quarantineAfter()).isEmpty()
        );
    }

    @Test
    void leaves_quarantine_off_when_the_property_is_not_set() {
        Duration unset = new OccurrentProperties.SagaProperties().getQuarantineAfter();

        assertThat(SagaAnnotationRegistrar.withQuarantineBudget(SagaRunnerConfig.defaults(), unset).quarantineAfter()).isEmpty();
    }

    @Test
    void passes_a_configured_budget_through_unchanged() {
        assertThat(SagaAnnotationRegistrar.withQuarantineBudget(SagaRunnerConfig.defaults(), Duration.ofSeconds(30)).quarantineAfter())
                .isEqualTo(Optional.of(Duration.ofSeconds(30)));
    }

    @Test
    void refuses_a_zero_budget_passed_straight_to_the_config_rather_than_quarantining_on_the_first_failure() {
        // The property reads zero as never, so accepting it here as "quarantine immediately" would make one literal
        // mean opposite things depending on how the saga was configured.
        assertThatThrownBy(() -> SagaRunnerConfig.defaults().withQuarantineAfter(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("quarantineAfter must be positive");
    }

    @Test
    void reads_zero_as_the_pre_0_34_0_behaviour_of_retrying_forever() {
        assertThat(SagaAnnotationRegistrar.withQuarantineBudget(SagaRunnerConfig.defaults(), Duration.ZERO).quarantineAfter()).isEmpty();
    }

    @Test
    void reads_an_absent_budget_as_never_too() {
        assertThat(SagaAnnotationRegistrar.withQuarantineBudget(SagaRunnerConfig.defaults(), null).quarantineAfter()).isEmpty();
    }

    @Test
    void passes_a_negative_budget_on_so_SagaRunnerConfig_rejects_it_rather_than_reading_a_typo_as_never() {
        assertThatThrownBy(() -> SagaAnnotationRegistrar.withQuarantineBudget(SagaRunnerConfig.defaults(), Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("quarantineAfter must be positive");
    }
}
