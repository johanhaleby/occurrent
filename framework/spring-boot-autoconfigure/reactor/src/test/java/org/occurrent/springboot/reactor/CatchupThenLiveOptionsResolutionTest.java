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

package org.occurrent.springboot.reactor;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.CatchupThenLiveOptions;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * How {@code occurrent.subscription.catchup-then-live.*} turns into {@link CatchupThenLiveOptions}. No Spring context:
 * the resolution is a pure function, and it is the part most likely to regress silently, since a wrong fallback still
 * produces a valid options object.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupThenLiveOptionsResolutionTest {

    @Test
    void both_unset_yields_the_built_in_defaults() {
        assertThat(ProjectionAnnotationRegistrar.catchupThenLiveOptions(new OccurrentProperties()))
                .isEqualTo(CatchupThenLiveOptions.defaults());
    }

    @Test
    void setting_only_the_dedup_cache_size_leaves_the_buffer_cap_at_its_default() {
        OccurrentProperties properties = new OccurrentProperties();
        properties.getSubscription().getCatchupThenLive().setDedupCacheSize(50_000);

        CatchupThenLiveOptions options = ProjectionAnnotationRegistrar.catchupThenLiveOptions(properties);

        assertThat(options.dedupCacheSize()).isEqualTo(50_000);
        assertThat(options.maxBufferedEvents()).isEqualTo(CatchupThenLiveOptions.DEFAULT_MAX_BUFFERED_EVENTS);
    }

    @Test
    void setting_only_the_buffer_cap_leaves_the_dedup_cache_size_at_its_default() {
        OccurrentProperties properties = new OccurrentProperties();
        properties.getSubscription().getCatchupThenLive().setMaxBufferedEvents(200_000);

        CatchupThenLiveOptions options = ProjectionAnnotationRegistrar.catchupThenLiveOptions(properties);

        assertThat(options.dedupCacheSize()).isEqualTo(CatchupThenLiveOptions.DEFAULT_DEDUP_CACHE_SIZE);
        assertThat(options.maxBufferedEvents()).isEqualTo(200_000);
    }

    @Test
    void setting_both_uses_both() {
        OccurrentProperties properties = new OccurrentProperties();
        properties.getSubscription().getCatchupThenLive().setDedupCacheSize(7);
        properties.getSubscription().getCatchupThenLive().setMaxBufferedEvents(9);

        assertThat(ProjectionAnnotationRegistrar.catchupThenLiveOptions(properties))
                .isEqualTo(new CatchupThenLiveOptions(7, 9));
    }
}
