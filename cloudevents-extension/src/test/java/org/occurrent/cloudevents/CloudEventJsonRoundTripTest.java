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

package org.occurrent.cloudevents;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A deployment that forwards events to a broker sends them as CloudEvents JSON and rebuilds them on the listener side,
 * which is what a push subscription is fed with. The CloudEvents SDK writes any extension number that is not an
 * {@code Integer} as a JSON string, so an Occurrent {@code long} comes back a {@code String}. These pin that the
 * accessors read it anyway, because otherwise every pushed event fails on the way in.
 */
@DisplayName("An Occurrent event that has been through CloudEvents JSON")
@DisplayNameGeneration(ReplaceUnderscores.class)
class CloudEventJsonRoundTripTest {

    private static final EventFormat JSON = Objects.requireNonNull(EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE));

    @Test
    void carries_its_stream_version_as_a_string_which_is_the_whole_reason_the_accessors_are_lenient() {
        CloudEvent roundTripped = roundTrip(occurrentEvent());

        assertThat(roundTripped.getExtension(OccurrentCloudEventExtension.STREAM_VERSION)).isInstanceOf(String.class);
    }

    @Test
    void still_reads_back_through_the_extension_getters() {
        CloudEvent roundTripped = roundTrip(occurrentEvent());

        assertAll(
                () -> assertThat(OccurrentExtensionGetter.getStreamId(roundTripped)).isEqualTo("stream-1"),
                () -> assertThat(OccurrentExtensionGetter.getStreamVersion(roundTripped)).isEqualTo(3L),
                () -> assertThat(OccurrentCloudEventExtension.getPosition(roundTripped)).isEqualTo(7L)
        );
    }

    @Test
    void still_reads_back_through_event_metadata() {
        EventMetadata metadata = EventMetadata.from(roundTrip(occurrentEvent()));

        assertAll(
                () -> assertThat(metadata.getStreamId()).isEqualTo("stream-1"),
                () -> assertThat(metadata.getStreamVersion()).isEqualTo(3L),
                () -> assertThat(metadata.getPosition()).isEqualTo(7L)
        );
    }

    private static CloudEvent occurrentEvent() {
        CloudEvent event = CloudEventBuilder.v1()
                .withId("event-1")
                .withSource(URI.create("urn:occurrent:test"))
                .withType("OrderPlaced")
                .withExtension(new OccurrentCloudEventExtension("stream-1", 3L))
                .build();
        return OccurrentCloudEventExtension.withPosition(event, 7L);
    }

    private static CloudEvent roundTrip(CloudEvent cloudEvent) {
        return JSON.deserialize(JSON.serialize(cloudEvent));
    }
}
