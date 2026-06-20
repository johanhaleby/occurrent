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

package org.occurrent.dsl.dcb.blocking;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.dsl.subscription.blocking.EventMetadata;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbEventMetadataTest {

    @Test
    void reads_position_when_stored_as_number() {
        EventMetadata metadata = new EventMetadata(Map.of(DcbCloudEvents.POSITION, 7L));

        assertThat(DcbEventMetadata.from(metadata).dcbPosition()).hasValue(7L);
    }

    @Test
    void reads_position_when_stored_as_string() {
        EventMetadata metadata = new EventMetadata(Map.of(DcbCloudEvents.POSITION, "7"));

        assertThat(DcbEventMetadata.from(metadata).dcbPosition()).hasValue(7L);
    }

    @Test
    void position_is_empty_when_absent() {
        EventMetadata metadata = new EventMetadata(Map.of());

        assertThat(DcbEventMetadata.from(metadata).dcbPosition()).isEmpty();
    }

    @Test
    void reads_and_canonicalizes_tags() {
        EventMetadata metadata = new EventMetadata(Map.of(DcbCloudEvents.TAGS, DcbCloudEvents.encodeTags(Set.of("name:1", "tenant:2"))));

        assertThat(DcbEventMetadata.from(metadata).dcbTags()).containsExactlyInAnyOrder("name:1", "tenant:2");
    }

    @Test
    void tags_are_empty_when_absent() {
        EventMetadata metadata = new EventMetadata(Map.of());

        assertThat(DcbEventMetadata.from(metadata).dcbTags()).isEmpty();
    }
}
