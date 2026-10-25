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
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.dcb.DcbEventMetadata;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.StringBasedCheckpoint;

import java.net.URI;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbEventMetadataTest {

    // position() reads the event's global position from the POSITION extension, or empty when there is none. It does
    // not depend on the event being DCB, a stream event with a position reads back the same way.
    @Test
    void reads_position_when_stored_as_number() {
        EventMetadata metadata = new EventMetadata(Map.of(OccurrentCloudEventExtension.POSITION, 7L));

        assertThat(DcbEventMetadata.from(metadata).position()).hasValue(7L);
    }

    @Test
    void reads_position_when_stored_as_string() {
        EventMetadata metadata = new EventMetadata(Map.of(OccurrentCloudEventExtension.POSITION, "7"));

        assertThat(DcbEventMetadata.from(metadata).position()).hasValue(7L);
    }

    @Test
    void position_is_empty_when_absent() {
        EventMetadata metadata = new EventMetadata(Map.of(DcbCloudEvents.TAGS, DcbCloudEvents.encodeTags(Set.of(Tag.of("name", "1")))));

        assertThat(DcbEventMetadata.from(metadata).position()).isEmpty();
    }

    @Test
    void reads_and_canonicalizes_tags() {
        EventMetadata metadata = new EventMetadata(Map.of(DcbCloudEvents.TAGS, DcbCloudEvents.encodeTags(Set.of(Tag.of("name", "1"), Tag.of("tenant", "2")))));

        assertThat(DcbEventMetadata.from(metadata).dcbTags()).containsExactlyInAnyOrder(Tag.of("name", "1"), Tag.of("tenant", "2"));
    }

    @Test
    void tags_are_empty_when_absent() {
        EventMetadata metadata = new EventMetadata(Map.of());

        assertThat(DcbEventMetadata.from(metadata).dcbTags()).isEmpty();
    }

    @Test
    void checkpoint_is_the_one_the_event_was_delivered_with() {
        // Given
        CloudEvent event = new CheckpointAwareCloudEvent(plainEvent(), new StringBasedCheckpoint("position:1;origin:0;liveFrom:token"));

        // When
        DcbEventMetadata metadata = DcbEventMetadata.from(EventMetadata.from(event));

        // Then
        assertThat(metadata.checkpoint()).hasValue("position:1;origin:0;liveFrom:token");
    }

    @Test
    void checkpoint_is_empty_when_the_event_was_delivered_without_one() {
        // Given
        DcbEventMetadata metadata = DcbEventMetadata.from(EventMetadata.from(plainEvent()));

        // When
        var checkpoint = metadata.checkpoint();

        // Then
        assertThat(checkpoint).isEmpty();
    }

    @Test
    void checkpoint_is_empty_when_the_metadata_was_built_from_a_map() {
        // Given
        DcbEventMetadata metadata = DcbEventMetadata.from(new EventMetadata(Map.of(OccurrentCloudEventExtension.POSITION, 7L)));

        // When
        var checkpoint = metadata.checkpoint();

        // Then
        assertThat(checkpoint).isEmpty();
    }

    private static CloudEvent plainEvent() {
        return CloudEventBuilder.v1().withId("e1").withSource(URI.create("urn:test")).withType("type").build();
    }
}
