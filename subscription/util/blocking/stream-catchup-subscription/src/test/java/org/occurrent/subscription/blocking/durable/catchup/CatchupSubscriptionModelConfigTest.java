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

package org.occurrent.subscription.blocking.durable.catchup;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.SortBy;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that every configurable field of {@link CatchupSubscriptionModelConfig} is part of equals/hashCode, so a
 * config used as (or inside) a map key does not silently collide with a config that differs only in
 * {@code catchupPhaseSortBy} or {@code dcbCatchupPositionWindowSize}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupSubscriptionModelConfigTest {

    @Test
    void configs_that_differ_only_in_catchup_phase_sort_by_are_not_equal() {
        CatchupSubscriptionModelConfig config1 = new CatchupSubscriptionModelConfig(10).catchupPhaseSortBy(SortBy.ascending("a"));
        CatchupSubscriptionModelConfig config2 = new CatchupSubscriptionModelConfig(10).catchupPhaseSortBy(SortBy.ascending("b"));

        assertThat(config1).isNotEqualTo(config2);
        assertThat(config1.hashCode()).isNotEqualTo(config2.hashCode());
    }

    @Test
    void configs_that_differ_only_in_dcb_catchup_position_window_size_are_not_equal() {
        CatchupSubscriptionModelConfig config1 = new CatchupSubscriptionModelConfig(10).dcbCatchupPositionWindowSize(100);
        CatchupSubscriptionModelConfig config2 = new CatchupSubscriptionModelConfig(10).dcbCatchupPositionWindowSize(200);

        assertThat(config1).isNotEqualTo(config2);
        assertThat(config1.hashCode()).isNotEqualTo(config2.hashCode());
    }

    @Test
    void configs_with_the_same_settings_are_equal() {
        CatchupSubscriptionModelConfig config1 = new CatchupSubscriptionModelConfig(10).catchupPhaseSortBy(SortBy.ascending("a")).dcbCatchupPositionWindowSize(50);
        CatchupSubscriptionModelConfig config2 = new CatchupSubscriptionModelConfig(10).catchupPhaseSortBy(SortBy.ascending("a")).dcbCatchupPositionWindowSize(50);

        assertThat(config1).isEqualTo(config2);
        assertThat(config1.hashCode()).isEqualTo(config2.hashCode());
    }
}
