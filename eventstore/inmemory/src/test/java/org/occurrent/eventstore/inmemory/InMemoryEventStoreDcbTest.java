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

package org.occurrent.eventstore.inmemory;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.eventstore.api.DuplicateCloudEventException;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.dcb.*;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.eventstore.api.SortBy.SortDirection.ASCENDING;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.eventstore.api.dcb.DcbQuery.*;

@DisplayNameGeneration(ReplaceUnderscores.class)
class InMemoryEventStoreDcbTest {

    private static final URI SOURCE = URI.create("urn:test");

    @Test
    void dcb_writes_are_visible_as_normal_cloud_events() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.append("dcb:partition:0", List.of(taggedEvent("NameDefined", "name:1")));

        assertThat(eventStore.all())
                .extracting(CloudEvent::getType)
                .containsExactly("NameDefined");
        assertThat(eventStore.read(tagsAllOf("name:1")).events())
                .extracting(CloudEvent::getType)
                .containsExactly("NameDefined");
    }

    @Test
    void dcb_appends_participate_in_global_natural_insertion_order() {
        InMemoryEventStore eventStore = new InMemoryEventStore();

        // Interleave DCB appends with a regular stream write. Natural order must follow the order things were
        // written, regardless of which write path produced them.
        eventStore.append("dcb:partition:0", List.of(taggedEvent("NameDefined", "name:1")));
        eventStore.write("stream:1", WriteCondition.streamVersionEq(0), Stream.of(event("OrderPlaced")));
        eventStore.append("dcb:partition:0", List.of(taggedEvent("NameChanged", "name:1")));

        assertThat(eventStore.all(SortBy.natural(ASCENDING)))
                .extracting(CloudEvent::getType)
                .containsExactly("NameDefined", "OrderPlaced", "NameChanged");
    }

    @Test
    void dcb_read_returns_events_in_global_position_order_across_streams() {
        InMemoryEventStore eventStore = new InMemoryEventStore();

        // Positions 1 and 3 land in one partition stream, position 2 in another. A DCB read must return them in
        // global dcbposition order, not grouped by the stream they happen to be stored in.
        eventStore.append("dcb:partition:0", List.of(taggedEvent("NameDefined", "name:1")));
        eventStore.append("dcb:partition:1", List.of(taggedEvent("NameChanged", "name:1")));
        eventStore.append("dcb:partition:0", List.of(taggedEvent("OrderPlaced", "name:1")));

        DcbEventStream eventStream = eventStore.read(tagsAllOf("name:1"));

        assertThat(eventStream.events())
                .extracting(CloudEvent::getType)
                .containsExactly("NameDefined", "NameChanged", "OrderPlaced");
    }

    @Test
    void reads_events_matching_type_or_all_tags_after_sequence_position() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.append("dcb:partition:0", List.of(
                taggedEvent("NameDefined", "name:1"),
                taggedEvent("NameChanged", "name:1", "tenant:1"),
                taggedEvent("OrderPlaced", "order:1")));

        DcbEventStream eventStream = eventStore.read(
                fromItems(List.of(
                        DcbQueryItem.types(List.of("OrderPlaced")),
                        DcbQueryItem.tagsAllOf(List.of("name:1", "tenant:1")))),
                DcbReadOptions.afterSequencePosition(1));

        assertThat(eventStream.events())
                .extracting(CloudEvent::getType)
                .containsExactly("NameChanged", "OrderPlaced");
        assertThat(eventStream.lastSequencePosition()).isEqualTo(3);
    }

    @Test
    void reads_tagged_events_except_excluded_types() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.append("dcb:partition:0", List.of(
                taggedEvent("NameDefined", "name:1"),
                taggedEvent("NameSnapshot", "name:1"),
                taggedEvent("OrderPlaced", "order:1")));

        DcbEventStream eventStream = eventStore.read(tagsAllOfExcludingTypes(List.of("name:1"), List.of("NameSnapshot")));

        assertThat(eventStream.events())
                .extracting(CloudEvent::getType)
                .containsExactly("NameDefined");
    }

    @Test
    void reads_type_and_tagged_events_except_excluded_types() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.append("dcb:partition:0", List.of(
                taggedEvent("NameDefined", "name:1"),
                taggedEvent("NameChanged", "name:1"),
                taggedEvent("OrderPlaced", "name:1")));

        DcbEventStream eventStream = eventStore.read(typeAndTagsAllOfExcludingTypes(
                List.of("NameDefined", "NameChanged"),
                List.of("name:1"),
                List.of("OrderPlaced")));

        assertThat(eventStream.events())
                .extracting(CloudEvent::getType)
                .containsExactly("NameDefined", "NameChanged");
    }

    @Test
    void applies_excluded_types_per_query_item() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.append("dcb:partition:0", List.of(
                taggedEvent("NameSnapshot", "name:1"),
                taggedEvent("NameDefined", "name:1"),
                taggedEvent("OrderPlaced", "order:1")));

        DcbEventStream eventStream = eventStore.read(fromItems(List.of(
                DcbQueryItem.tagsAllOfExcludingTypes(List.of("name:1"), List.of("NameSnapshot")),
                DcbQueryItem.tagsAllOf(List.of("order:1")))));

        assertThat(eventStream.events())
                .extracting(CloudEvent::getType)
                .containsExactly("NameDefined", "OrderPlaced");
    }

    @Test
    void rejects_append_when_matching_event_exists_after_condition_position() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.append("dcb:partition:0", List.of(taggedEvent("NameDefined", "name:1")));
        DcbEventStream readModel = eventStore.read(tagsAllOf("name:1"));

        eventStore.append("dcb:partition:0", List.of(taggedEvent("NameChanged", "name:1")));

        assertThatThrownBy(() -> eventStore.append(
                "dcb:partition:0",
                List.of(taggedEvent("NameChanged", "name:1")),
                failIfEventsMatch(tagsAllOf("name:1"), readModel.lastSequencePosition())))
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
    }

    @Test
    void append_condition_ignores_excluded_event_types_after_condition_position() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.append("dcb:partition:0", List.of(taggedEvent("NameDefined", "name:1")));
        DcbQuery query = tagsAllOfExcludingTypes(List.of("name:1"), List.of("NameSnapshot"));
        DcbEventStream readModel = eventStore.read(query);

        eventStore.append("dcb:partition:0", List.of(taggedEvent("NameSnapshot", "name:1")));

        DcbAppendResult result = eventStore.append(
                "dcb:partition:0",
                List.of(taggedEvent("NameChanged", "name:1")),
                failIfEventsMatch(query, readModel.lastSequencePosition()));

        assertThat(result.firstSequencePosition()).isEqualTo(3);
    }

    @Test
    void append_condition_rejects_non_excluded_event_types_after_condition_position() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.append("dcb:partition:0", List.of(taggedEvent("NameDefined", "name:1")));
        DcbQuery query = tagsAllOfExcludingTypes(List.of("name:1"), List.of("NameSnapshot"));
        DcbEventStream readModel = eventStore.read(query);

        eventStore.append("dcb:partition:0", List.of(taggedEvent("NameChanged", "name:1")));

        assertThatThrownBy(() -> eventStore.append(
                "dcb:partition:0",
                List.of(taggedEvent("NameImported", "name:1")),
                failIfEventsMatch(query, readModel.lastSequencePosition())))
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
    }

    @Test
    void rejects_duplicate_cloud_event_id_and_source() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        CloudEvent cloudEvent = taggedEvent("NameDefined", "name:1");

        eventStore.append("dcb:partition:0", List.of(cloudEvent));

        assertThatThrownBy(() -> eventStore.append("dcb:partition:0", List.of(cloudEvent)))
                .isExactlyInstanceOf(DuplicateCloudEventException.class);
    }

    @Test
    void does_not_inspect_payload_when_matching_tags() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        CloudEvent cloudEvent = DcbCloudEvents.withTags(CloudEventBuilder.v1(event("NameDefined"))
                .withDataContentType("application/json")
                .withData("{\"tags\":[\"name:1\"]}".getBytes(UTF_8))
                .build(), Set.of("name:2"));

        eventStore.append("dcb:partition:0", List.of(cloudEvent));

        assertThat(eventStore.read(tagsAllOf("name:1")).events()).isEmpty();
        assertThat(eventStore.read(tagsAllOf("name:2")).events()).hasSize(1);
    }

    @Test
    void delete_all_resets_dcb_sequence() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.append("dcb:partition:0", List.of(taggedEvent("NameDefined", "name:1")));

        eventStore.deleteAll();

        assertThat(eventStore.read(all()).lastSequencePosition()).isZero();
        assertThat(eventStore.append("dcb:partition:0", List.of(taggedEvent("NameDefined", "name:1"))).firstSequencePosition()).isEqualTo(1);
    }

    private static CloudEvent taggedEvent(String type, String... tags) {
        return DcbCloudEvents.withTags(event(type), Set.of(tags));
    }

    private static CloudEvent event(String type) {
        return CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(SOURCE)
                .withType(type)
                .withTime(OffsetDateTime.now())
                .withData("{}".getBytes(UTF_8))
                .build();
    }
}
