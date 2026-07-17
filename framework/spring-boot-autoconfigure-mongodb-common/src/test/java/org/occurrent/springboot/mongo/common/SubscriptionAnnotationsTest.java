/*
 *
 *  Copyright 2024 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.springboot.mongo.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SubscriptionAnnotationsTest {

    @Test
    void synchronous_with_startAt_is_rejected() {
        assertThatThrownBy(() -> SubscriptionAnnotations.validateModeStartKnobs("@Projection", "orders", true, true, false, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("@Projection")
                .hasMessageContaining("orders")
                .hasMessageContaining("mode = SYNCHRONOUS");
    }

    @Test
    void synchronous_with_startAtPosition_is_rejected() {
        assertThatThrownBy(() -> SubscriptionAnnotations.validateModeStartKnobs("@Snapshot", "ledger", true, false, true, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("@Snapshot")
                .hasMessageContaining("ledger")
                .hasMessageContaining("mode = SYNCHRONOUS");
    }

    @Test
    void synchronous_with_resumeBehavior_is_rejected() {
        assertThatThrownBy(() -> SubscriptionAnnotations.validateModeStartKnobs("@Projection", "orders", true, false, false, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mode = SYNCHRONOUS");
    }

    @Test
    void startAt_together_with_startAtPosition_is_rejected() {
        assertThatThrownBy(() -> SubscriptionAnnotations.validateModeStartKnobs("@Snapshot", "ledger", false, true, true, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("@Snapshot")
                .hasMessageContaining("ledger")
                .hasMessageContaining("both startAt and startAtPosition");
    }

    @Test
    void asynchronous_with_all_catch_up_knobs_except_the_startAt_pair_is_allowed() {
        assertThatCode(() -> SubscriptionAnnotations.validateModeStartKnobs("@Projection", "orders", false, true, false, true))
                .doesNotThrowAnyException();
    }

    @Test
    void synchronous_without_any_start_knob_is_allowed() {
        assertThatCode(() -> SubscriptionAnnotations.validateModeStartKnobs("@Snapshot", "ledger", true, false, false, false))
                .doesNotThrowAnyException();
    }

    @Test
    void asynchronous_with_only_startAtPosition_is_allowed() {
        assertThatCode(() -> SubscriptionAnnotations.validateModeStartKnobs("@Projection", "orders", false, false, true, true))
                .doesNotThrowAnyException();
    }
}
