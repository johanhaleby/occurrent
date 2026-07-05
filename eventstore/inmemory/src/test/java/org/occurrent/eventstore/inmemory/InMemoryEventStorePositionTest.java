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

package org.occurrent.eventstore.inmemory;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.Tag;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.getPosition;
import static org.occurrent.filter.Filter.all;

/**
 * Tests for the global {@code position} carried on stream-written events (in addition to DCB-appended events), and
 * for {@link InMemoryEventStore}'s {@link org.occurrent.eventstore.api.blocking.PositionOrderedReader} implementation.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class InMemoryEventStorePositionTest {

    private static final URI SOURCE = URI.create("urn:test");

    @Test
    void stream_written_events_get_a_monotonic_position_shared_with_dcb_events() {
        InMemoryEventStore eventStore = new InMemoryEventStore().withStreamPosition();

        eventStore.append(List.of(taggedEvent("DcbEvent1", "t:1")));                                     // position 1
        eventStore.write("stream1", WriteCondition.anyStreamVersion(), Stream.of(event("StreamEvent1"), event("StreamEvent2"))); // positions 2,3
        eventStore.append(List.of(taggedEvent("DcbEvent2", "t:1")));                                     // position 4

        List<CloudEvent> streamEvents = eventStore.read("stream1").events().toList();
        assertThat(streamEvents).extracting(InMemoryEventStorePositionTest::position).containsExactly(2L, 3L);

        assertThat(eventStore.currentPosition()).isEqualTo(4L);
    }

    @Test
    void position_ordered_reader_returns_events_in_position_order_within_the_requested_range() {
        InMemoryEventStore eventStore = new InMemoryEventStore().withStreamPosition();

        eventStore.write("stream1", WriteCondition.anyStreamVersion(), Stream.of(event("A")));  // position 1
        eventStore.append(List.of(taggedEvent("B", "t:1")));                                      // position 2
        eventStore.write("stream1", WriteCondition.anyStreamVersion(), Stream.of(event("C")));  // position 3
        eventStore.write("stream2", WriteCondition.anyStreamVersion(), Stream.of(event("D")));  // position 4

        List<CloudEvent> all = eventStore.readInPositionOrder(all(), PositionRange.fromBeginning()).toList();
        assertThat(all).extracting(CloudEvent::getType).containsExactly("A", "B", "C", "D");

        List<CloudEvent> afterFirst = eventStore.readInPositionOrder(all(), PositionRange.afterPosition(1)).toList();
        assertThat(afterFirst).extracting(CloudEvent::getType).containsExactly("B", "C", "D");

        List<CloudEvent> between = eventStore.readInPositionOrder(all(), PositionRange.between(1, 3)).toList();
        assertThat(between).extracting(CloudEvent::getType).containsExactly("B", "C");
    }

    @Test
    void opt_out_store_writes_no_position_on_stream_events_and_rejects_position_apis() {
        InMemoryEventStore eventStore = new InMemoryEventStore().withoutStreamPosition();

        assertThat(eventStore.writesPosition()).isFalse();

        eventStore.write("stream1", WriteCondition.anyStreamVersion(), Stream.of(event("StreamEvent1")));
        CloudEvent writtenEvent = eventStore.read("stream1").events().findFirst().orElseThrow();
        assertThat(position(writtenEvent)).isZero();

        assertThatThrownBy(() -> eventStore.currentPosition())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> eventStore.readInPositionOrder(all(), PositionRange.fromBeginning()))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private static long position(CloudEvent event) {
        return getPosition(event);
    }

    private static CloudEvent taggedEvent(String type, String... tags) {
        return DcbCloudEvents.withTags(event(type), java.util.Arrays.stream(tags).map(Tag::parse).toList());
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
