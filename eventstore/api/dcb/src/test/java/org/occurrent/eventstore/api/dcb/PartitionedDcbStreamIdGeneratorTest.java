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

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class PartitionedDcbStreamIdGeneratorTest {

    private final PartitionedDcbStreamIdGenerator generator = new PartitionedDcbStreamIdGenerator(64, "dcb:partition:");

    @Test
    void tag_sets_that_differ_only_by_a_separator_character_canonicalize_differently() {
        // A tag may legally contain "|", so two different sets must not canonicalize to the same string (which would
        // force them onto the same partition). Asserted on the canonical form because two distinct strings can still
        // land on the same partition by chance.
        assertThat(PartitionedDcbStreamIdGenerator.canonicalize(Set.of("a|b", "c")))
                .isNotEqualTo(PartitionedDcbStreamIdGenerator.canonicalize(Set.of("a", "b", "c")));
    }

    @Test
    void same_tag_set_always_maps_to_the_same_stream_id_regardless_of_iteration_order() {
        assertThat(generator.generateStreamId(Set.of("game:1", "tenant:42")))
                .isEqualTo(generator.generateStreamId(Set.of("tenant:42", "game:1")));
    }

    @Test
    void generated_stream_id_starts_with_the_configured_prefix() {
        assertThat(generator.generateStreamId(Set.of("game:1"))).startsWith("dcb:partition:");
    }
}
