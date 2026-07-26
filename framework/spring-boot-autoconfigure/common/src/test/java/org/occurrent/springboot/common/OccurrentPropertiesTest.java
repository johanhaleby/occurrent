/*
 *
 *  Copyright 2026 Johan Haleby
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

package org.occurrent.springboot.common;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.springboot.common.OccurrentProperties.SubscriptionProperties.CatchupThenLiveProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Plain unit tests for the hand-written validation on {@link OccurrentProperties}. No Spring context: the setters are
 * where the rejection happens, and Spring only turns the resulting exception into a startup failure, which
 * {@code OccurrentMongoAutoConfigurationCharacterizationTest} covers separately.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class OccurrentPropertiesTest {

    @Test
    void the_catch_up_then_live_tunables_are_unset_by_default_so_the_built_in_defaults_apply() {
        CatchupThenLiveProperties properties = new OccurrentProperties().getSubscription().getCatchupThenLive();

        // Null rather than a copy of the real default, so the numbers live in one place and cannot drift.
        assertThat(properties.getDedupCacheSize()).isNull();
        assertThat(properties.getMaxBufferedEvents()).isNull();
    }

    @Test
    void a_non_positive_dedup_cache_size_is_rejected_with_the_property_key_in_the_message() {
        CatchupThenLiveProperties properties = new CatchupThenLiveProperties();

        assertThatThrownBy(() -> properties.setDedupCacheSize(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("occurrent.subscription.catchup-then-live.dedup-cache-size must be greater than zero");
        assertThatThrownBy(() -> properties.setDedupCacheSize(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void a_non_positive_max_buffered_events_is_rejected_with_the_property_key_in_the_message() {
        CatchupThenLiveProperties properties = new CatchupThenLiveProperties();

        assertThatThrownBy(() -> properties.setMaxBufferedEvents(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("occurrent.subscription.catchup-then-live.max-buffered-events must be greater than zero");
        assertThatThrownBy(() -> properties.setMaxBufferedEvents(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void clearing_a_tunable_back_to_unset_is_allowed() {
        CatchupThenLiveProperties properties = new CatchupThenLiveProperties();
        properties.setDedupCacheSize(50_000);

        properties.setDedupCacheSize(null);

        assertThat(properties.getDedupCacheSize()).isNull();
    }
}
