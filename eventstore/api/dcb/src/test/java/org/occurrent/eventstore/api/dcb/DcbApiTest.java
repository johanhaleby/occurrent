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
import org.occurrent.cloudevents.OccurrentCloudEventExtension;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbApiTest {

    @Test
    void query_must_be_all_or_contain_at_least_one_item() {
        assertThatThrownBy(() -> DcbCriteria.anyOf(List.of()))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("A criteria must contain at least one criterion");
    }

    @Test
    void query_item_requires_type_or_tag() {
        assertThatThrownBy(() -> new DcbCriterion(Set.of(), Set.of()))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("A criterion must contain at least one type or tag");
    }

    @Test
    void existing_query_item_factories_use_empty_excluded_types() {
        assertThat(DcbCriteria.types(List.of("NameDefined")).excludedTypes()).isEmpty();
        assertThat(DcbCriteria.tags(List.of(Tag.of("name", "1"))).excludedTypes()).isEmpty();
        assertThat(DcbCriteria.types(List.of("NameDefined")).tags(List.of(Tag.of("name", "1"))).excludedTypes()).isEmpty();
    }

    @Test
    void query_item_can_exclude_event_types() {
        DcbCriterion item = DcbCriteria.tags(List.of(Tag.of("name", "1"))).excludingTypes(List.of("NameSnapshot", " NameSnapshot ", "NameImported"));

        assertThat(item.types()).isEmpty();
        assertThat(item.tags()).containsExactly(Tag.of("name", "1"));
        assertThat(item.excludedTypes()).containsExactlyInAnyOrder("NameImported", "NameSnapshot");
        assertThat(DcbCriteria.tags(List.of(Tag.of("name", "1"))).excludingTypes(List.of("NameSnapshot")))
                .isInstanceOfSatisfying(DcbCriterion.class, single ->
                        assertThat(single.excludedTypes()).containsExactly("NameSnapshot"));
    }

    @Test
    void query_item_rejects_invalid_excluded_types() {
        assertThatThrownBy(() -> DcbCriteria.tags(List.of(Tag.of("name", "1"))).excludingTypes(List.of(" ")))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Excluded types cannot contain blank values");
        assertThatThrownBy(() -> DcbCriteria.tags(List.of(Tag.of("name", "1"))).excludingTypes(Arrays.asList("NameDefined", null)))
                .isExactlyInstanceOf(NullPointerException.class)
                .hasMessage("Excluded type cannot be null");
    }

    @Test
    void query_item_rejects_overlapping_included_and_excluded_types() {
        assertThatThrownBy(() -> DcbCriteria.types(List.of("NameDefined")).tags(List.of(Tag.of("name", "1"))).excludingTypes(List.of("NameDefined")))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Types and excluded types cannot overlap");
    }

    @Test
    void query_item_rejects_excluded_types_without_positive_selector() {
        assertThatThrownBy(() -> new DcbCriterion(Set.of(), Set.of(), Set.of("NameDefined")))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("A criterion must contain at least one type or tag");
    }

    @Test
    void cloud_event_helper_strips_deduplicates_and_encodes_tags() {
        io.cloudevents.CloudEvent event = DcbCloudEvents.withTags(cloudEvent(), List.of(Tag.of("name", "1"), Tag.of("name", "1"), Tag.of("course", "2")));

        assertThat(DcbCloudEvents.getTags(event)).containsExactlyInAnyOrder(Tag.of("course", "2"), Tag.of("name", "1"));
        assertThat(event.getExtension(DcbCloudEvents.TAGS)).isEqualTo("course:2\nname:1");
    }

    @Test
    void tag_rejects_blank_key_or_value() {
        assertThatThrownBy(() -> Tag.of(" ", "1"))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Tag key cannot be blank");
        assertThatThrownBy(() -> Tag.of("name", " "))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Tag value cannot be blank");
    }

    @Test
    void cloud_event_helper_adds_position() {
        io.cloudevents.CloudEvent event = OccurrentCloudEventExtension.withPosition(cloudEvent(), 42);

        assertThat(event.getExtension(OccurrentCloudEventExtension.POSITION)).isEqualTo(42L);
        assertThat(OccurrentCloudEventExtension.getPosition(event)).isEqualTo(42);
    }

    @Test
    void cloud_event_helper_rejects_malformed_position() {
        io.cloudevents.CloudEvent event = CloudEventBuilder.v1(cloudEvent()).withExtension(OccurrentCloudEventExtension.POSITION, true).build();

        assertThatThrownBy(() -> OccurrentCloudEventExtension.getPosition(event))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Position extension must be a Number or String");
    }

    @Test
    void cloud_event_helper_matches_dcb_queries() {
        io.cloudevents.CloudEvent event = DcbCloudEvents.withTags(cloudEvent("NameDefined"), List.of(Tag.of("name", "1"), Tag.of("tenant", "1")));

        assertThat(DcbCloudEvents.matches(event, DcbCriteria.all())).isTrue();
        assertThat(DcbCloudEvents.matches(event, DcbCriteria.types("NameDefined"))).isTrue();
        assertThat(DcbCloudEvents.matches(event, DcbCriteria.tags(Tag.of("name", "1"), Tag.of("tenant", "1")))).isTrue();
        assertThat(DcbCloudEvents.matches(event, DcbCriteria.tags(List.of(Tag.of("name", "1"))).excludingTypes(List.of("NameWasChanged")))).isTrue();
        assertThat(DcbCloudEvents.matches(event, DcbCriteria.tags(List.of(Tag.of("name", "1"))).excludingTypes(List.of("NameDefined")))).isFalse();
        assertThat(DcbCloudEvents.matches(event, DcbCriteria.types("NameWasChanged"))).isFalse();
        assertThat(DcbCloudEvents.matches(event, DcbCriteria.tags(Tag.of("name", "1"), Tag.of("tenant", "2")))).isFalse();
    }

    @Test
    void cloud_event_helper_matches_any_query_item() {
        io.cloudevents.CloudEvent event = DcbCloudEvents.withTags(cloudEvent("OrderPlaced"), List.of(Tag.of("order", "1")));

        DcbCriteria criteria = DcbCriteria.anyOf(List.of(
                DcbCriteria.tags(List.of(Tag.of("name", "1"))),
                DcbCriteria.types(List.of("OrderPlaced"))));

        assertThat(DcbCloudEvents.matches(event, criteria)).isTrue();
    }

    @Test
    void cloud_event_helper_matches_type_tags_and_excluded_types_together() {
        io.cloudevents.CloudEvent event = DcbCloudEvents.withTags(cloudEvent("NameDefined"), List.of(Tag.of("name", "1"), Tag.of("tenant", "1")));

        assertThat(DcbCloudEvents.matches(event, DcbCriteria.types(List.of("NameDefined")).tags(List.of(Tag.of("name", "1"), Tag.of("tenant", "1"))).excludingTypes(List.of("NameWasChanged")))).isTrue();
        assertThat(DcbCloudEvents.matches(event, DcbCriteria.types(List.of("NameWasChanged")).tags(List.of(Tag.of("name", "1"), Tag.of("tenant", "1"))).excludingTypes(List.of("NameImported")))).isFalse();
        assertThat(DcbCloudEvents.matches(event, DcbCriteria.types(List.of("NameDefined")).tags(List.of(Tag.of("name", "1"), Tag.of("tenant", "2"))).excludingTypes(List.of("NameWasChanged")))).isFalse();
        assertThat(DcbCloudEvents.matches(DcbCloudEvents.withTags(cloudEvent("NameImported"), List.of(Tag.of("name", "1"), Tag.of("tenant", "1"))), DcbCriteria.types(List.of("NameDefined")).tags(List.of(Tag.of("name", "1"), Tag.of("tenant", "1"))).excludingTypes(List.of("NameImported")))).isFalse();
    }

    @Test
    void read_options_and_append_conditions_reject_negative_sequence_positions() {
        assertThatThrownBy(() -> DcbReadOptions.afterPosition(-1))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("After position cannot be negative");
        assertThatThrownBy(() -> DcbAppendCondition.failIfEventsMatch(DcbCriteria.all(), DcbConsistencyToken.of(-1)))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Consistency token value cannot be negative");
    }

    @Test
    void query_factory_shortcuts_are_consistent() {
        DcbCriterion item = DcbCriteria.tags(java.util.List.of(Tag.of("t", "1")));

        // anyOf(Collection) is equivalent to anyOf(varargs).
        assertThat(DcbCriteria.anyOf(java.util.List.of(item))).isEqualTo(DcbCriteria.anyOf(item));

        // type(String) is the single-type shorthand.
        assertThat(DcbCriteria.type("X")).isEqualTo(DcbCriteria.types("X"));
        assertThat(DcbCriteria.type("X")).isEqualTo(DcbCriteria.types(java.util.List.of("X")));
    }

    @Test
    void whole_store_lock_is_equivalent_to_fail_if_events_match_with_match_all() {
        assertThat(DcbAppendCondition.wholeStoreLock()).isEqualTo(DcbAppendCondition.failIfEventsMatch(DcbCriteria.all()));

        DcbConsistencyToken token = DcbConsistencyToken.of(3);
        assertThat(DcbAppendCondition.wholeStoreLock(token)).isEqualTo(DcbAppendCondition.failIfEventsMatch(DcbCriteria.all(), token));
    }

    @Test
    void append_condition_not_fulfilled_exception_exposes_condition_and_position_via_accessors() {
        DcbAppendCondition condition = DcbAppendCondition.wholeStoreLock();

        DcbAppendConditionNotFulfilledException exception = new DcbAppendConditionNotFulfilledException(condition, 7, "conflict");

        assertThat(exception.appendCondition()).isEqualTo(condition);
        assertThat(exception.currentPosition()).isEqualTo(7);
        assertThat(exception.getMessage()).isEqualTo("conflict");
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
