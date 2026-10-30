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

package org.occurrent.subscription;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;

import java.net.URI;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class CheckpointAwareCloudEventMetadataTest {

    @Test
    void metadata_of_a_checkpoint_aware_event_exposes_the_checkpoint_as_the_string_the_event_carries() {
        // Given
        CloudEvent event = new CheckpointAwareCloudEvent(streamEvent("e1", 1), new StringBasedCheckpoint("position:1;origin:0;liveFrom:token"));

        // When
        EventMetadata metadata = EventMetadata.from(event);

        // Then
        assertThat(metadata.getCheckpoint()).hasValue("position:1;origin:0;liveFrom:token");
    }

    @Test
    void metadata_of_a_global_checkpoint_event_exposes_the_string_form_a_restart_parses() {
        // Given
        CloudEvent event = new CheckpointAwareCloudEvent(streamEvent("e1", 1), GlobalCheckpoint.of(7));

        // When
        EventMetadata metadata = EventMetadata.from(event);

        // Then
        assertThat(metadata.getCheckpoint()).hasValue(GlobalCheckpoint.of(7).asString());
    }

    @Test
    void metadata_of_an_event_without_a_checkpoint_has_none() {
        // Given
        CloudEvent event = streamEvent("e1", 1);

        // When
        EventMetadata metadata = EventMetadata.from(event);

        // Then
        assertThat(metadata.getCheckpoint()).isEmpty();
    }

    @Test
    void metadata_built_from_a_map_has_no_checkpoint() {
        // Given
        EventMetadata metadata = new EventMetadata(Map.of(OccurrentCloudEventExtension.STREAM_ID, "stream"));

        // When
        var checkpoint = metadata.getCheckpoint();

        // Then
        assertThat(checkpoint).isEmpty();
    }

    @Test
    void metadata_built_from_the_data_of_a_checkpoint_aware_event_has_no_checkpoint() {
        // Given
        EventMetadata fromEvent = EventMetadata.from(new CheckpointAwareCloudEvent(streamEvent("e1", 1), new StringBasedCheckpoint("c")));

        // When
        EventMetadata rebuilt = new EventMetadata(fromEvent.getData());

        // Then
        assertThat(fromEvent.getCheckpoint()).isPresent();
        assertThat(rebuilt.getCheckpoint()).isEmpty();
    }

    @Test
    void the_checkpoint_stays_out_of_the_extensions_so_a_forwarded_event_cannot_carry_it() {
        // Given
        CloudEvent plain = streamEvent("e1", 1);
        CloudEvent event = new CheckpointAwareCloudEvent(plain, new StringBasedCheckpoint("secret-checkpoint"));

        // When
        EventMetadata metadata = EventMetadata.from(event);

        // Then
        assertThat(event.getExtensionNames()).isEqualTo(plain.getExtensionNames());
        assertThat(metadata.getData()).isEqualTo(EventMetadata.from(plain).getData());
        assertThat(metadata.getData().values()).doesNotContain("secret-checkpoint");
    }

    @Test
    void metadata_of_the_same_event_delivered_by_a_replay_and_delivered_live_is_equal_and_hashes_equal() {
        // Given
        CloudEvent plain = streamEvent("e1", 1);
        EventMetadata replayed = EventMetadata.from(new CheckpointAwareCloudEvent(plain, GlobalCheckpoint.of(1)));
        EventMetadata live = EventMetadata.from(new CheckpointAwareCloudEvent(plain, new StringBasedCheckpoint("{\"_data\": \"8266F2\"}")));

        // When
        boolean equal = replayed.equals(live);

        // Then
        assertThat(replayed.getCheckpoint()).isNotEqualTo(live.getCheckpoint());
        assertThat(equal).isTrue();
        assertThat(replayed).hasSameHashCodeAs(live);
        assertThat(replayed).isEqualTo(EventMetadata.from(plain));
    }

    @Test
    void metadata_of_different_events_is_not_equal() {
        // Given
        EventMetadata first = EventMetadata.from(new CheckpointAwareCloudEvent(streamEvent("e1", 1), GlobalCheckpoint.of(1)));
        EventMetadata second = EventMetadata.from(new CheckpointAwareCloudEvent(streamEvent("e2", 2), GlobalCheckpoint.of(1)));

        // When
        boolean equal = first.equals(second);

        // Then
        assertThat(equal).isFalse();
    }

    private static CloudEvent streamEvent(String id, long streamVersion) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("type")
                .withExtension(new OccurrentCloudEventExtension("stream", streamVersion))
                .build();
    }
}
