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
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.cloudevents.OccurrentExtensionGetter;
import org.occurrent.eventstore.api.DuplicateCloudEventException;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;
import org.occurrent.eventstore.api.dcb.Tag;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.eventstore.api.SortBy.SortDirection.ASCENDING;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.all;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;

@DisplayNameGeneration(ReplaceUnderscores.class)
class InMemoryEventStoreDcbTest {

    private static final URI SOURCE = URI.create("urn:test");

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
    void delete_all_resets_dcb_sequence() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1")));

        eventStore.deleteAll();

        assertThat(eventStore.read(all()).lastSequencePosition()).isZero();
        assertThat(eventStore.append(List.of(taggedEvent("NameDefined", "name:1"))).firstSequencePosition()).isEqualTo(1);
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

    private static Tag tag(String canonical) {
        return Tag.parse(canonical);
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
