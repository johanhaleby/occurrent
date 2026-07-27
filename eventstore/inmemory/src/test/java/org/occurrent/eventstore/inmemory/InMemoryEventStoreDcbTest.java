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
import org.occurrent.cloudevents.OccurrentExtensionGetter;
import org.occurrent.eventstore.api.DuplicateCloudEventException;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.dcb.*;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.eventstore.api.SortBy.SortDirection.ASCENDING;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.*;

@DisplayNameGeneration(ReplaceUnderscores.class)
class InMemoryEventStoreDcbTest {

    private static final URI SOURCE = URI.create("urn:test");

    @Test
    void dcb_writes_are_visible_as_normal_cloud_events() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1")));

        assertThat(eventStore.all())
                .extracting(CloudEvent::getType)
                .containsExactly("NameDefined");
        assertThat(eventStore.read(tags(tag("name:1"))).events())
                .extracting(CloudEvent::getType)
                .containsExactly("NameDefined");
    }

    @Test
    void exists_and_count_honour_the_read_options_position_window() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.append(List.of(taggedEvent("E", "t:1")));   // position 1
        eventStore.append(List.of(taggedEvent("E", "t:1")));   // position 2
        eventStore.append(List.of(taggedEvent("E", "t:1")));   // position 3

        assertThat(eventStore.count(tags(tag("t:1")))).isEqualTo(3);
        assertThat(eventStore.count(tags(tag("t:1")), DcbReadOptions.afterPosition(1))).isEqualTo(2);
        assertThat(eventStore.count(tags(tag("t:1")), DcbReadOptions.between(1, 2))).isEqualTo(1);
        assertThat(eventStore.exists(tags(tag("t:1")), DcbReadOptions.between(2, 3))).isTrue();
        assertThat(eventStore.exists(tags(tag("t:1")), DcbReadOptions.afterPosition(3))).isFalse();
        assertThat(eventStore.exists(tags(tag("missing:1")))).isFalse();
    }

    @Test
    void direction_skip_and_limit_select_matches_without_changing_ascending_order_or_the_token() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        for (int i = 0; i < 5; i++) {
            eventStore.append(List.of(taggedEvent("E", "seq:1")));
        }
        DcbCriteria criteria = tags(tag("seq:1"));
        List<String> all = ids(eventStore.read(criteria));
        assertThat(all).hasSize(5);

        assertThat(ids(eventStore.read(criteria, DcbReadOptions.fromBeginning().forwards().skip(1).limit(2))))
                .containsExactly(all.get(1), all.get(2));
        assertThat(ids(eventStore.read(criteria, DcbReadOptions.fromBeginning().backwards().skip(1).limit(2))))
                .containsExactly(all.get(2), all.get(3));
        assertThat(ids(eventStore.read(criteria, DcbReadOptions.fromBeginning().backwards().skip(99)))).isEmpty();
        assertThat(ids(eventStore.read(criteria, DcbReadOptions.fromBeginning().backwards().limit(99)))).isEqualTo(all);

        assertThat(eventStore.read(criteria, DcbReadOptions.fromBeginning().backwards().skip(1).limit(1)).consistencyToken())
                .isEqualTo(eventStore.read(criteria).consistencyToken());
    }

    @Test
    void exists_and_count_ignore_direction_skip_and_limit() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.append(List.of(taggedEvent("E", "t:1")));
        eventStore.append(List.of(taggedEvent("E", "t:1")));
        eventStore.append(List.of(taggedEvent("E", "t:1")));
        DcbReadOptions options = DcbReadOptions.afterPosition(1).backwards().skip(99).limit(1);

        assertThat(eventStore.read(tags(tag("t:1")), options).events()).isEmpty();
        assertThat(eventStore.exists(tags(tag("t:1")), options)).isTrue();
        assertThat(eventStore.count(tags(tag("t:1")), options)).isEqualTo(2);
    }

    private static List<String> ids(DcbEventStream stream) {
        return stream.events().stream().map(CloudEvent::getId).toList();
    }

    @Test
    void no_token_append_condition_reflects_current_existence_not_past_appends() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        DcbCriteria criteria = tags(tag("name:1"));
        CloudEvent existing = taggedEvent("NameDefined", "name:1");
        eventStore.append(List.of(existing));

        // While a matching event exists, the no-token guard conflicts.
        assertThatThrownBy(() -> eventStore.append(List.of(taggedEvent("NameChanged", "name:1")), failIfEventsMatch(criteria)))
                .isInstanceOf(DcbAppendConditionNotFulfilledException.class);

        // After the matching event is deleted, the no-token guard succeeds again: it means "currently exists". This is
        // the cross-store contract the Spring Mongo store now matches.
        eventStore.deleteEvent(existing.getId(), existing.getSource());
        DcbAppendResult result = eventStore.append(List.of(taggedEvent("NameChanged", "name:1")), failIfEventsMatch(criteria));
        assertThat(result.eventCount()).isEqualTo(1);
    }

    @Test
    void dcb_appends_participate_in_global_natural_insertion_order() {
        InMemoryEventStore eventStore = new InMemoryEventStore();

        // Interleave DCB appends with a regular stream write. Natural order must follow the order things were
        // written, regardless of which write path produced them.
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1")));
        eventStore.write("stream:1", WriteCondition.streamVersionEq(0), List.of(event("OrderPlaced")));
        eventStore.append(List.of(taggedEvent("NameChanged", "name:1")));

        assertThat(eventStore.all(SortBy.natural(ASCENDING)))
                .extracting(CloudEvent::getType)
                .containsExactly("NameDefined", "OrderPlaced", "NameChanged");
    }

    @Test
    void dcb_read_returns_events_in_global_position_order_across_streams() {
        InMemoryEventStore eventStore = new InMemoryEventStore();

        // Positions 1 and 3 land in one partition stream, position 2 in another. A DCB read must return them in
        // global position order, not grouped by the stream they happen to be stored in.
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1")));
        eventStore.append(List.of(taggedEvent("NameChanged", "name:1")));
        eventStore.append(List.of(taggedEvent("OrderPlaced", "name:1")));

        DcbEventStream eventStream = eventStore.read(tags(tag("name:1")));

        assertThat(eventStream.events())
                .extracting(CloudEvent::getType)
                .containsExactly("NameDefined", "NameChanged", "OrderPlaced");
    }

    @Test
    void dcb_append_with_condition_places_the_same_boundary_in_the_same_stream_regardless_of_per_event_tags() {
        InMemoryEventStore eventStore = new InMemoryEventStore();

        // Two appends to the same boundary (game:1), but each event carries a different extra tag. Placement must
        // follow the condition's boundary tags, not the per-event tags, so both land in the same partition stream.
        eventStore.append(List.of(taggedEvent("NameDefined", "game:1", "extra:a")), failIfEventsMatch(tags(tag("game:1"))));
        DcbConsistencyToken token = eventStore.read(tags(tag("game:1"))).consistencyToken();
        eventStore.append(List.of(taggedEvent("NameChanged", "game:1", "extra:b")), failIfEventsMatch(tags(tag("game:1")), token));

        List<String> streamIds = eventStore.read(tags(tag("game:1"))).events().stream()
                .map(OccurrentExtensionGetter::getStreamId)
                .distinct()
                .toList();
        assertThat(streamIds).hasSize(1);
        assertThat(streamIds.get(0)).startsWith("dcb:partition:");
    }

    @Test
    void reads_events_matching_type_or_all_tags_after_sequence_position() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.append(List.of(
                taggedEvent("NameDefined", "name:1"),
                taggedEvent("NameChanged", "name:1", "tenant:1"),
                taggedEvent("OrderPlaced", "order:1")));

        DcbEventStream eventStream = eventStore.read(
                anyOf(List.of(
                        DcbCriteria.types(List.of("OrderPlaced")),
                        DcbCriteria.tags(List.of(tag("name:1"), tag("tenant:1"))))),
                DcbReadOptions.afterPosition(1));

        assertThat(eventStream.events())
                .extracting(CloudEvent::getType)
                .containsExactly("NameChanged", "OrderPlaced");
        assertThat(eventStream.lastSequencePosition()).isEqualTo(3);
    }

    @Test
    void reads_tagged_events_except_excluded_types() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.append(List.of(
                taggedEvent("NameDefined", "name:1"),
                taggedEvent("NameSnapshot", "name:1"),
                taggedEvent("OrderPlaced", "order:1")));

        DcbEventStream eventStream = eventStore.read(tags(List.of(tag("name:1"))).excludingTypes(List.of("NameSnapshot")));

        assertThat(eventStream.events())
                .extracting(CloudEvent::getType)
                .containsExactly("NameDefined");
    }

    @Test
    void reads_type_and_tagged_events_except_excluded_types() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.append(List.of(
                taggedEvent("NameDefined", "name:1"),
                taggedEvent("NameChanged", "name:1"),
                taggedEvent("OrderPlaced", "name:1")));

        DcbEventStream eventStream = eventStore.read(types(List.of("NameDefined", "NameChanged")).tags(List.of(tag("name:1"))).excludingTypes(List.of("OrderPlaced")));

        assertThat(eventStream.events())
                .extracting(CloudEvent::getType)
                .containsExactly("NameDefined", "NameChanged");
    }

    @Test
    void applies_excluded_types_per_query_item() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.append(List.of(
                taggedEvent("NameSnapshot", "name:1"),
                taggedEvent("NameDefined", "name:1"),
                taggedEvent("OrderPlaced", "order:1")));

        DcbEventStream eventStream = eventStore.read(anyOf(List.of(
                DcbCriteria.tags(List.of(tag("name:1"))).excludingTypes(List.of("NameSnapshot")),
                DcbCriteria.tags(List.of(tag("order:1"))))));

        assertThat(eventStream.events())
                .extracting(CloudEvent::getType)
                .containsExactly("NameDefined", "OrderPlaced");
    }

    @Test
    void rejects_append_when_matching_event_exists_after_condition_position() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1")));
        DcbEventStream readModel = eventStore.read(tags(tag("name:1")));

        eventStore.append(List.of(taggedEvent("NameChanged", "name:1")));

        assertThatThrownBy(() -> eventStore.append(
                List.of(taggedEvent("NameChanged", "name:1")),
                failIfEventsMatch(tags(tag("name:1")), readModel.consistencyToken())))
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
    }

    @Test
    void append_condition_ignores_excluded_event_types_after_condition_position() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1")));
        DcbCriteria criteria = tags(List.of(tag("name:1"))).excludingTypes(List.of("NameSnapshot"));
        DcbEventStream readModel = eventStore.read(criteria);

        eventStore.append(List.of(taggedEvent("NameSnapshot", "name:1")));

        DcbAppendResult result = eventStore.append(
                List.of(taggedEvent("NameChanged", "name:1")),
                failIfEventsMatch(criteria, readModel.consistencyToken()));

        assertThat(result.firstSequencePosition()).isEqualTo(3);
    }

    @Test
    void append_condition_rejects_non_excluded_event_types_after_condition_position() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1")));
        DcbCriteria criteria = tags(List.of(tag("name:1"))).excludingTypes(List.of("NameSnapshot"));
        DcbEventStream readModel = eventStore.read(criteria);

        eventStore.append(List.of(taggedEvent("NameChanged", "name:1")));

        assertThatThrownBy(() -> eventStore.append(
                List.of(taggedEvent("NameImported", "name:1")),
                failIfEventsMatch(criteria, readModel.consistencyToken())))
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
    }

    @Test
    void rejects_duplicate_cloud_event_id_and_source() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        CloudEvent cloudEvent = taggedEvent("NameDefined", "name:1");

        eventStore.append(List.of(cloudEvent));

        assertThatThrownBy(() -> eventStore.append(List.of(cloudEvent)))
                .isExactlyInstanceOf(DuplicateCloudEventException.class);
    }

    @Test
    void does_not_inspect_payload_when_matching_tags() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        CloudEvent cloudEvent = DcbCloudEvents.withTags(CloudEventBuilder.v1(event("NameDefined"))
                .withDataContentType("application/json")
                .withData("{\"tags\":[\"name:1\"]}".getBytes(UTF_8))
                .build(), Set.of(tag("name:2")));

        eventStore.append(List.of(cloudEvent));

        assertThat(eventStore.read(tags(tag("name:1"))).events()).isEmpty();
        assertThat(eventStore.read(tags(tag("name:2"))).events()).hasSize(1);
    }

    @Test
    void delete_all_resets_dcb_sequence() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1")));

        eventStore.deleteAll();

        assertThat(eventStore.read(all()).lastSequencePosition()).isZero();
        assertThat(eventStore.append(List.of(taggedEvent("NameDefined", "name:1"))).firstSequencePosition()).isEqualTo(1);
    }

    @Test
    void last_sequence_position_is_the_store_head_not_the_max_matched_position() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.append(List.of(
                taggedEvent("NameDefined", "name:1"),
                taggedEvent("NameChanged", "name:1"),
                taggedEvent("OrderPlaced", "name:2")));

        // The criteria matches only the two "name:1" events (positions 1 and 2), but the store head is 3.
        DcbEventStream matchesSome = eventStore.read(tags(tag("name:1")));
        assertThat(matchesSome.events()).extracting(CloudEvent::getType).containsExactly("NameDefined", "NameChanged");
        assertThat(matchesSome.lastSequencePosition()).isEqualTo(3);

        // A criteria that matches nothing still observes the store head.
        DcbEventStream matchesNone = eventStore.read(tags(tag("name:absent")));
        assertThat(matchesNone.events()).isEmpty();
        assertThat(matchesNone.lastSequencePosition()).isEqualTo(3);
    }

    @Test
    void failed_append_does_not_consume_a_dcb_position() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        CloudEvent duplicate = taggedEvent("NameDefined", "name:1");
        DcbAppendResult first = eventStore.append(List.of(duplicate));

        assertThatThrownBy(() -> eventStore.append(List.of(duplicate)))
                .isExactlyInstanceOf(DuplicateCloudEventException.class);

        // The shared DcbAppendResult contract only guarantees ordering across appends, but the in-memory store
        // advances its position counter only after an append commits, so a rejected append consumes no position
        // and the next successful append gets exactly the following position.
        DcbAppendResult next = eventStore.append(List.of(taggedEvent("NameChanged", "name:2")));
        assertThat(next.firstSequencePosition()).isEqualTo(first.lastSequencePosition() + 1);
        assertThat(next.lastSequencePosition()).isEqualTo(next.firstSequencePosition());
    }

    @Test
    void exists_and_count_report_matching_dcb_events() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1")));
        eventStore.append(List.of(taggedEvent("NameChanged", "name:1")));
        eventStore.append(List.of(taggedEvent("OrderPlaced", "order:1")));

        assertThat(eventStore.exists(tags(tag("name:1")))).isTrue();
        assertThat(eventStore.exists(tags(tag("absent:1")))).isFalse();
        assertThat(eventStore.count(tags(tag("name:1")))).isEqualTo(2);
        assertThat(eventStore.count(tags(tag("order:1")))).isEqualTo(1);
        assertThat(eventStore.count(all())).isEqualTo(3);
    }

    @Test
    void read_honors_up_to_sequence_position_upper_bound() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1")));
        eventStore.append(List.of(taggedEvent("NameChanged", "name:1")));
        eventStore.append(List.of(taggedEvent("OrderPlaced", "name:1")));

        DcbEventStream upToTwo = eventStore.read(tags(tag("name:1")), DcbReadOptions.upToPosition(2));

        assertThat(upToTwo.events()).extracting(CloudEvent::getType).containsExactly("NameDefined", "NameChanged");
        // lastSequencePosition is always the store head, not the upper bound used for this read.
        assertThat(upToTwo.lastSequencePosition()).isEqualTo(3);
    }

    @Test
    void any_of_matches_the_union_of_its_items() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1")));
        eventStore.append(List.of(taggedEvent("OrderPlaced", "order:1")));
        eventStore.append(List.of(taggedEvent("Unrelated", "other:1")));

        DcbCriteria criteria = anyOf(
                DcbCriteria.types(List.of("NameDefined")),
                DcbCriteria.tags(List.of(tag("order:1"))));

        assertThat(eventStore.read(criteria).events())
                .extracting(CloudEvent::getType)
                .containsExactly("NameDefined", "OrderPlaced");
    }

    @Test
    void dcb_all_only_matches_dcb_written_events_not_stream_written_events() {
        InMemoryEventStore eventStore = new InMemoryEventStore().withStreamPosition();
        eventStore.write("stream:1", WriteCondition.streamVersionEq(0), List.of(event("OrderPlaced")));
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1")));

        assertThat(eventStore.read(all()).events())
                .extracting(CloudEvent::getType)
                .containsExactly("NameDefined");
    }

    @Test
    void dcb_type_only_criterion_does_not_match_a_stream_written_event_of_that_type() {
        InMemoryEventStore eventStore = new InMemoryEventStore().withStreamPosition();
        eventStore.write("stream:1", WriteCondition.streamVersionEq(0), List.of(event("OrderPlaced")));
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1")));

        assertThat(eventStore.read(types(List.of("OrderPlaced"))).events()).isEmpty();
    }

    @Test
    void exists_and_count_are_false_and_zero_for_a_store_with_only_stream_written_events() {
        InMemoryEventStore eventStore = new InMemoryEventStore().withStreamPosition();
        eventStore.write("stream:1", WriteCondition.streamVersionEq(0), List.of(event("OrderPlaced")));

        assertThat(eventStore.exists(all())).isFalse();
        assertThat(eventStore.count(all())).isZero();
    }

    private static Tag tag(String canonical) {
        return Tag.parse(canonical);
    }

    @Test
    void stream_write_rejects_a_dcb_tagged_event() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        CloudEvent dcbTaggedEvent = taggedEvent("NameDefined", "name:1");

        assertThatThrownBy(() -> eventStore.write("name:1", WriteCondition.anyStreamVersion(), List.of(dcbTaggedEvent)))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("A DCB-tagged event cannot be written through the stream write(...) API, use the DCB append(...) API instead.");

        assertThat(eventStore.all()).isEmpty();
    }

    private static CloudEvent taggedEvent(String type, String... tags) {
        return DcbCloudEvents.withTags(event(type), Arrays.stream(tags).map(Tag::parse).toList());
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
