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

package org.occurrent.subscription.internal;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class HandoverOptionsTest {

    @Test
    void defaults_use_the_documented_dedup_cache_size_and_max_buffered_events() {
        HandoverOptions options = HandoverOptions.defaults();

        assertThat(options.dedupCacheSize()).isEqualTo(HandoverOptions.DEFAULT_DEDUP_CACHE_SIZE);
        assertThat(options.maxBufferedEvents()).isEqualTo(HandoverOptions.DEFAULT_MAX_BUFFERED_EVENTS);
    }

    @Test
    void a_non_positive_dedup_cache_size_is_rejected() {
        assertThatThrownBy(() -> new HandoverOptions(0, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("dedupCacheSize must be greater than zero");
        assertThatThrownBy(() -> new HandoverOptions(-1, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("dedupCacheSize must be greater than zero");
    }

    @Test
    void a_non_positive_max_buffered_events_is_rejected() {
        assertThatThrownBy(() -> new HandoverOptions(10, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxBufferedEvents must be greater than zero");
        assertThatThrownBy(() -> new HandoverOptions(10, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxBufferedEvents must be greater than zero");
    }
}
