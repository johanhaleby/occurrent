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
class BoundedIdCacheTest {

    @Test
    void contains_an_added_id() {
        BoundedIdCache cache = new BoundedIdCache(2);

        cache.add("a");

        assertThat(cache.contains("a")).isTrue();
        assertThat(cache.contains("b")).isFalse();
    }

    @Test
    void evicts_the_oldest_id_when_the_cap_is_exceeded() {
        BoundedIdCache cache = new BoundedIdCache(2);

        cache.add("a");
        cache.add("b");
        cache.add("c");

        assertThat(cache.contains("a")).isFalse();
        assertThat(cache.contains("b")).isTrue();
        assertThat(cache.contains("c")).isTrue();
    }

    @Test
    void re_adding_an_id_does_not_make_it_younger() {
        BoundedIdCache cache = new BoundedIdCache(2);

        cache.add("a");
        cache.add("b");
        cache.add("a");
        cache.add("c");

        // "a" is still the oldest insertion, so it is the one evicted even though it was added again.
        assertThat(cache.contains("a")).isFalse();
        assertThat(cache.contains("b")).isTrue();
        assertThat(cache.contains("c")).isTrue();
    }

    @Test
    void retains_exactly_the_most_recent_ids_up_to_the_cap() {
        BoundedIdCache cache = new BoundedIdCache(3);

        for (int i = 0; i < 10; i++) {
            cache.add("id-" + i);
        }

        assertThat(cache.contains("id-6")).isFalse();
        assertThat(cache.contains("id-7")).isTrue();
        assertThat(cache.contains("id-8")).isTrue();
        assertThat(cache.contains("id-9")).isTrue();
    }

    @Test
    void a_cache_of_one_holds_only_the_latest_id() {
        BoundedIdCache cache = new BoundedIdCache(1);

        cache.add("a");
        cache.add("b");

        assertThat(cache.contains("a")).isFalse();
        assertThat(cache.contains("b")).isTrue();
    }

    @Test
    void rejects_a_max_size_below_one() {
        assertThatThrownBy(() -> new BoundedIdCache(0))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxSize must be at least 1, was 0");

        assertThatThrownBy(() -> new BoundedIdCache(-1))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxSize must be at least 1, was -1");
    }
}
