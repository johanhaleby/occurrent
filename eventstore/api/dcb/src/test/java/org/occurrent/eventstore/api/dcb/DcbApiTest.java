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
import java.util.Arrays;
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
                .hasMessage("A query must contain at least one query item");
    }

    @Test
    void query_item_requires_type_or_tag() {
        assertThatThrownBy(() -> new DcbQueryItem(Set.of(), Set.of()))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("A query item must contain at least one type or tag");
    }

    @Test
    void existing_query_item_factories_use_empty_excluded_types() {
        assertThat(DcbQueryItem.types(List.of("NameDefined")).excludedTypes()).isEmpty();
        assertThat(DcbQueryItem.tagsAllOf(List.of("name:1")).excludedTypes()).isEmpty();
        assertThat(DcbQueryItem.typeAndTagsAllOf(List.of("NameDefined"), List.of("name:1")).excludedTypes()).isEmpty();
    }

    @Test
    void query_item_can_exclude_event_types() {
        DcbQueryItem item = DcbQueryItem.tagsAllOfExcludingTypes(
                List.of(" name:1 "),
                List.of("NameSnapshot", " NameSnapshot ", "NameImported"));

        assertThat(item.types()).isEmpty();
        assertThat(item.tags()).containsExactly("name:1");
        assertThat(item.excludedTypes()).containsExactlyInAnyOrder("NameImported", "NameSnapshot");
        assertThat(DcbQuery.tagsAllOfExcludingTypes(List.of("name:1"), List.of("NameSnapshot")))
                .isInstanceOfSatisfying(DcbQuery.Items.class, items ->
                        assertThat(items.items().get(0).excludedTypes()).containsExactly("NameSnapshot"));
    }

    @Test
    void query_item_rejects_invalid_excluded_types() {
        assertThatThrownBy(() -> DcbQueryItem.tagsAllOfExcludingTypes(List.of("name:1"), List.of(" ")))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Excluded types cannot contain blank values");
        assertThatThrownBy(() -> DcbQueryItem.tagsAllOfExcludingTypes(List.of("name:1"), Arrays.asList("NameDefined", null)))
                .isExactlyInstanceOf(NullPointerException.class)
                .hasMessage("Excluded type cannot be null");
    }

    @Test
    void query_item_rejects_overlapping_included_and_excluded_types() {
        assertThatThrownBy(() -> DcbQueryItem.typeAndTagsAllOfExcludingTypes(List.of("NameDefined"), List.of("name:1"), List.of("NameDefined")))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Types and excluded types cannot overlap");
    }

    @Test
    void query_item_rejects_excluded_types_without_positive_selector() {
        assertThatThrownBy(() -> new DcbQueryItem(Set.of(), Set.of(), Set.of("NameDefined")))
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
        assertThat(DcbCloudEvents.getPosition(event)).isEqualTo(42);
    }

    @Test
    void cloud_event_helper_rejects_malformed_position() {
        io.cloudevents.CloudEvent event = CloudEventBuilder.v1(cloudEvent()).withExtension(DcbCloudEvents.POSITION, true).build();

        assertThatThrownBy(() -> DcbCloudEvents.getPosition(event))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("DCB position extension must be a Number or String");
    }

    @Test
    void cloud_event_helper_matches_dcb_queries() {
        io.cloudevents.CloudEvent event = DcbCloudEvents.withTags(cloudEvent("NameDefined"), List.of("name:1", "tenant:1"));

        assertThat(DcbCloudEvents.matches(event, DcbQuery.all())).isTrue();
        assertThat(DcbCloudEvents.matches(event, DcbQuery.types("NameDefined"))).isTrue();
        assertThat(DcbCloudEvents.matches(event, DcbQuery.tagsAllOf("name:1", "tenant:1"))).isTrue();
        assertThat(DcbCloudEvents.matches(event, DcbQuery.tagsAllOfExcludingTypes(List.of("name:1"), List.of("NameWasChanged")))).isTrue();
        assertThat(DcbCloudEvents.matches(event, DcbQuery.tagsAllOfExcludingTypes(List.of("name:1"), List.of("NameDefined")))).isFalse();
        assertThat(DcbCloudEvents.matches(event, DcbQuery.types("NameWasChanged"))).isFalse();
        assertThat(DcbCloudEvents.matches(event, DcbQuery.tagsAllOf("name:1", "tenant:2"))).isFalse();
    }

    @Test
    void cloud_event_helper_matches_any_query_item() {
        io.cloudevents.CloudEvent event = DcbCloudEvents.withTags(cloudEvent("OrderPlaced"), List.of("order:1"));

        DcbQuery query = DcbQuery.fromItems(List.of(
                DcbQueryItem.tagsAllOf(List.of("name:1")),
                DcbQueryItem.types(List.of("OrderPlaced"))));

        assertThat(DcbCloudEvents.matches(event, query)).isTrue();
    }

    @Test
    void cloud_event_helper_matches_type_tags_and_excluded_types_together() {
        io.cloudevents.CloudEvent event = DcbCloudEvents.withTags(cloudEvent("NameDefined"), List.of("name:1", "tenant:1"));

        assertThat(DcbCloudEvents.matches(event, DcbQuery.typeAndTagsAllOfExcludingTypes(
                List.of("NameDefined"),
                List.of("name:1", "tenant:1"),
                List.of("NameWasChanged")))).isTrue();
        assertThat(DcbCloudEvents.matches(event, DcbQuery.typeAndTagsAllOfExcludingTypes(
                List.of("NameWasChanged"),
                List.of("name:1", "tenant:1"),
                List.of("NameImported")))).isFalse();
        assertThat(DcbCloudEvents.matches(event, DcbQuery.typeAndTagsAllOfExcludingTypes(
                List.of("NameDefined"),
                List.of("name:1", "tenant:2"),
                List.of("NameWasChanged")))).isFalse();
        assertThat(DcbCloudEvents.matches(DcbCloudEvents.withTags(cloudEvent("NameImported"), List.of("name:1", "tenant:1")), DcbQuery.typeAndTagsAllOfExcludingTypes(
                List.of("NameDefined"),
                List.of("name:1", "tenant:1"),
                List.of("NameImported")))).isFalse();
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
        return cloudEvent("type");
    }

    private static io.cloudevents.CloudEvent cloudEvent(String type) {
        return CloudEventBuilder.v1()
                .withId("id")
                .withSource(URI.create("urn:test"))
                .withType(type)
                .withData("{}".getBytes(UTF_8))
                .build();
    }
}
