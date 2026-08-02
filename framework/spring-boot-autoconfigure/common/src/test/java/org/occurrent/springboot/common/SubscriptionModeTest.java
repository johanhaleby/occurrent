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

package org.occurrent.springboot.common;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.springboot.common.SubscriptionMode.AUTO;
import static org.occurrent.springboot.common.SubscriptionMode.DISABLED;
import static org.occurrent.springboot.common.SubscriptionMode.MANUAL;

/**
 * Covers how {@code occurrent.subscription.mode} and the deprecated {@code occurrent.subscription.enabled} combine.
 * The pair is allowed while they agree, because a recipe rewrites configuration files but cannot reach an environment
 * variable, so an application mid-migration can legitimately have both set.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SubscriptionModeTest {

    @Test
    void defaults_to_auto_when_neither_is_set() {
        assertThat(SubscriptionMode.resolve(null, null)).isEqualTo(AUTO);
    }

    @Test
    void uses_mode_when_only_mode_is_set() {
        assertThat(SubscriptionMode.resolve(MANUAL, null)).isEqualTo(MANUAL);
    }

    @Test
    void translates_the_deprecated_enabled_when_only_it_is_set() {
        assertThat(SubscriptionMode.resolve(null, false)).isEqualTo(DISABLED);
        assertThat(SubscriptionMode.resolve(null, true)).isEqualTo(AUTO);
    }

    @Test
    void accepts_both_when_they_agree() {
        assertThat(SubscriptionMode.resolve(AUTO, true)).isEqualTo(AUTO);
        assertThat(SubscriptionMode.resolve(DISABLED, false)).isEqualTo(DISABLED);
    }

    @Test
    void fails_when_both_are_set_and_contradict_each_other() {
        assertThatThrownBy(() -> SubscriptionMode.resolve(MANUAL, false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("occurrent.subscription.mode is manual")
                .hasMessageContaining("occurrent.subscription.enabled is false")
                .hasMessageContaining("environment variables");
    }

    @Test
    void manual_contradicts_enabled_being_true_because_enabled_always_meant_started() {
        assertThatThrownBy(() -> SubscriptionMode.resolve(MANUAL, true))
                .isInstanceOf(IllegalStateException.class);
    }
}
