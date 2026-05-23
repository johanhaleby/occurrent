/*
 * Copyright 2020 Johan Haleby
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

import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;

import java.net.URI;
import java.util.List;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbApiTest {

    @Test
    void query_must_be_all_or_contain_at_least_one_item() {
        assertThatThrownBy(() -> DcbQuery.fromItems(List.of()))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("A query must contain at least one query item unless it matches all events");
    }

    @Test
    void query_item_requires_type_or_tag() {
        assertThatThrownBy(() -> new DcbQueryItem(Set.of(), Set.of()))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("A query item must contain at least one type or tag");
    }

    @Test
    void cloud_event_helper_strips_deduplicates_and_encodes_tags() {
        io.cloudevents.CloudEvent event = DcbCloudEvents.withTags(cloudEvent(), List.of(" name:1 ", "name:1", "course:2"));

        assertThat(DcbCloudEvents.getTags(event)).containsExactlyInAnyOrder("course:2", "name:1");
        assertThat(event.getExtension(DcbCloudEvents.TAGS)).isEqualTo("course:2\nname:1");
    }

    @Test
    void cloud_event_helper_rejects_blank_tags() {
        assertThatThrownBy(() -> DcbCloudEvents.withTags(cloudEvent(), List.of(" ")))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Tags cannot contain blank values");
    }

    @Test
    void cloud_event_helper_adds_position() {
        io.cloudevents.CloudEvent event = DcbCloudEvents.withPosition(cloudEvent(), 42);

        assertThat(event.getExtension(DcbCloudEvents.POSITION)).isEqualTo(42L);
    }

    @Test
    void read_options_and_append_conditions_reject_negative_sequence_positions() {
        assertThatThrownBy(() -> DcbReadOptions.afterSequencePosition(-1))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("After sequence position cannot be negative");
        assertThatThrownBy(() -> DcbAppendCondition.failIfEventsMatch(DcbQuery.all(), -1))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("After sequence position cannot be negative");
    }

    private static io.cloudevents.CloudEvent cloudEvent() {
        return CloudEventBuilder.v1()
                .withId("id")
                .withSource(URI.create("urn:test"))
                .withType("type")
                .withData("{}".getBytes(UTF_8))
                .build();
    }
}
