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

package org.occurrent.eventstore.api.dcb;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbCloudEventsTagCodecTest {

    @Test
    void decode_of_encode_round_trips_to_the_canonical_tag_set() {
        // Include a value with an embedded ':' so a naive split-on-colon codec would corrupt it.
        List<Tag> tags = List.of(Tag.of("email", "a:b@x"), Tag.of("order", "1"), Tag.of("order", "1"));

        assertThat(DcbCloudEvents.decodeTags(DcbCloudEvents.encodeTags(tags)))
                .isEqualTo(DcbCloudEvents.canonicalizeTags(tags));
    }

    @Test
    void encode_produces_newline_joined_sorted_canonical_strings() {
        List<Tag> tags = List.of(Tag.of("order", "1"), Tag.of("email", "a:b@x"));

        // Sorted by canonical form: "email:a:b@x" < "order:1".
        assertThat(DcbCloudEvents.encodeTags(tags)).isEqualTo("email:a:b@x\norder:1");
    }
}
