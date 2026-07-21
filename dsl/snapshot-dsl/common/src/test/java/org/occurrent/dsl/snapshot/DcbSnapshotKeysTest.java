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

package org.occurrent.dsl.snapshot;

import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;

import static org.assertj.core.api.Assertions.assertThat;

class DcbSnapshotKeysTest {

    @Test
    void is_stable_across_tag_order() {
        DcbCriteria a = DcbCriteria.type("Account").tags(Tag.of("customer", "1"), Tag.of("region", "eu"));
        DcbCriteria b = DcbCriteria.type("Account").tags(Tag.of("region", "eu"), Tag.of("customer", "1"));

        assertThat(DcbSnapshotKeys.canonicalKey(a)).isEqualTo(DcbSnapshotKeys.canonicalKey(b));
    }

    @Test
    void is_stable_across_alternative_order() {
        DcbCriteria a = DcbCriteria.anyOf(DcbCriteria.tags(Tag.of("order", "1")), DcbCriteria.tags(Tag.of("customer", "2")));
        DcbCriteria b = DcbCriteria.anyOf(DcbCriteria.tags(Tag.of("customer", "2")), DcbCriteria.tags(Tag.of("order", "1")));

        assertThat(DcbSnapshotKeys.canonicalKey(a)).isEqualTo(DcbSnapshotKeys.canonicalKey(b));
    }

    @Test
    void differs_for_different_boundaries() {
        DcbCriteria one = DcbCriteria.tags(Tag.of("customer", "1"));
        DcbCriteria two = DcbCriteria.tags(Tag.of("customer", "2"));

        assertThat(DcbSnapshotKeys.canonicalKey(one)).isNotEqualTo(DcbSnapshotKeys.canonicalKey(two));
    }

    @Test
    void renders_match_all_as_a_stable_key() {
        assertThat(DcbSnapshotKeys.canonicalKey(DcbCriteria.all())).isEqualTo(DcbSnapshotKeys.canonicalKey(DcbCriteria.all()));
    }

    @Test
    void does_not_collide_when_a_type_name_contains_the_delimiter_character() {
        // Without the length-prefixed join, both of these rendered as the literal string "types[A,B]" and would have
        // collided onto the same snapshot key.
        DcbCriteria singleTypeContainingComma = DcbCriteria.type("A,B");
        DcbCriteria twoTypes = DcbCriteria.types("A", "B");

        assertThat(DcbSnapshotKeys.canonicalKey(singleTypeContainingComma)).isNotEqualTo(DcbSnapshotKeys.canonicalKey(twoTypes));
    }

    @Test
    void key_is_the_readable_canonical_string_not_a_hash() {
        String key = DcbSnapshotKeys.canonicalKey(DcbCriteria.tags(Tag.of("customer", "1")));

        assertThat(key).contains("tags[");
    }
}
