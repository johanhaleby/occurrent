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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A saga on the annotation path takes its quarantine budget from {@code occurrent.saga.quarantine-after}, and the
 * migration guide tells a reader to switch quarantine off to keep the pre-0.34.0 behaviour. A {@code Duration} property
 * that is not set binds to its default rather than to null, so zero is what says "never" here.
 */
@DisplayName("The saga quarantine budget property")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaQuarantineBudgetPropertyTest {

    @Test
    void defaults_to_the_same_five_minutes_the_runner_defaults_to() {
        assertAll(
                () -> assertThat(new OccurrentProperties.SagaProperties().getQuarantineAfter()).isEqualTo(Duration.ofMinutes(5)),
                () -> assertThat(SagaRunnerConfig.defaults().quarantineAfter()).isEqualTo(Duration.ofMinutes(5))
        );
    }

    @Test
    void passes_a_configured_budget_through_unchanged() {
        assertThat(SagaAnnotationRegistrar.quarantineBudgetOf(Duration.ofSeconds(30))).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void reads_zero_as_the_pre_0_34_0_behaviour_of_retrying_forever() {
        assertThat(SagaAnnotationRegistrar.quarantineBudgetOf(Duration.ZERO)).isNull();
    }

    @Test
    void reads_a_negative_budget_the_same_way_rather_than_quarantining_on_the_first_failure() {
        assertThat(SagaAnnotationRegistrar.quarantineBudgetOf(Duration.ofSeconds(-1))).isNull();
    }
}
