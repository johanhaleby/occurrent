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

package org.occurrent.tck.eventstore.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.occurrent.filter.Filter;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.tck.ConformanceEvents.event;

/**
 * The global sequence position contract, stated in ADR 84. A position is positive, unique, and strictly increasing,
 * and is comparable across separate appends without being contiguous. A rejected write reserves and abandons a
 * position block, so gaps between two writes are expected, not a defect.
 * <p>
 * {@code DcbAppendResult} additionally documents that the very first position a store ever hands out is 1, but this
 * suite does not assert that. A fixture cannot guarantee its store's underlying position counter has never been used
 * before this test, only that the store contains no events. Nothing here asserts a literal position value or
 * contiguity. Every bound this suite reads a range with is derived from a position it read back off a written event,
 * never from a literal such as 1 or 2.
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the stream position contract")
public abstract class StreamPositionConformance extends EventStoreConformance {

    private static final String DEFINED = "NameDefined";

    @Override
    protected final Set<EventStoreCapability> requiredCapabilities() {
        return Set.of(EventStoreCapability.STREAM);
    }

    @Test
    void a_written_event_carries_a_positive_position() {
        eventStore().write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("A", DEFINED)));

        CloudEvent written = singleEventOf("stream:1");
        assertThat(OccurrentCloudEventExtension.getPosition(written))
                .as("A written event's position must be positive")
                .isPositive();
    }

    @Test
    void positions_strictly_increase_across_successive_writes_to_the_same_stream() {
        eventStore().write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("A", DEFINED)));
        eventStore().write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("B", DEFINED)));
        eventStore().write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("C", DEFINED)));

        List<CloudEvent> events = eventStore().read("stream:1").events().toList();
        long positionOfA = OccurrentCloudEventExtension.getPosition(events.get(0));
        long positionOfB = OccurrentCloudEventExtension.getPosition(events.get(1));
        long positionOfC = OccurrentCloudEventExtension.getPosition(events.get(2));

        assertThat(positionOfB)
                .as("The second write to stream:1 must get a strictly higher position than the first")
                .isGreaterThan(positionOfA);
        assertThat(positionOfC)
                .as("The third write to stream:1 must get a strictly higher position than the second")
                .isGreaterThan(positionOfB);
    }

    @Test
    void positions_are_globally_unique_and_strictly_increase_across_writes_to_different_streams() {
        eventStore().write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("A", DEFINED)));
        eventStore().write("stream:2", WriteCondition.anyStreamVersion(), List.of(event("B", DEFINED)));
        eventStore().write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("C", DEFINED)));

        long positionOfA = OccurrentCloudEventExtension.getPosition(singleEventOf("stream:1", "A"));
        long positionOfB = OccurrentCloudEventExtension.getPosition(singleEventOf("stream:2", "B"));
        long positionOfC = OccurrentCloudEventExtension.getPosition(singleEventOf("stream:1", "C"));

        assertThat(Set.of(positionOfA, positionOfB, positionOfC))
                .as("Positions handed out to writes across different streams must be globally unique")
                .hasSize(3);
        assertThat(positionOfB)
                .as("A write to stream:2 must get a strictly higher position than an earlier write to stream:1: "
                        + "the position counter is global, not per stream")
                .isGreaterThan(positionOfA);
        assertThat(positionOfC)
                .as("A later write to stream:1 must get a strictly higher position than an intervening write to "
                        + "stream:2")
                .isGreaterThan(positionOfB);
    }

    @Test
    void current_position_is_zero_on_an_empty_store() {
        assertThat(positionOrderedReader().currentPosition())
                .as("currentPosition() must be 0 on a store with no positioned events")
                .isZero();
    }

    @Test
    void current_position_is_at_least_the_highest_position_written_after_a_sequential_write() {
        eventStore().write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("A", DEFINED)));
        eventStore().write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("B", DEFINED)));

        long positionOfB = OccurrentCloudEventExtension.getPosition(singleEventOf("stream:1", "B"));

        // currentPosition() is a high-watermark, not "the last visible event's position": it may run ahead of what a
        // reader can see under concurrency. With no concurrency here, it is still only guaranteed to be at least the
        // highest position written, never asserted equal to it.
        assertThat(positionOrderedReader().currentPosition())
                .as("currentPosition() must be at least the highest position written so far")
                .isGreaterThanOrEqualTo(positionOfB);
    }

    @Test
    void read_in_position_order_returns_events_in_ascending_position_order() {
        eventStore().write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("A", DEFINED)));
        eventStore().write("stream:2", WriteCondition.anyStreamVersion(), List.of(event("B", DEFINED)));
        eventStore().write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("C", DEFINED)));

        List<CloudEvent> events;
        try (var stream = positionOrderedReader().readInPositionOrder(Filter.all(), PositionRange.fromBeginning())) {
            events = stream.toList();
        }

        List<Long> positions = events.stream().map(OccurrentCloudEventExtension::getPosition).toList();
        assertThat(positions)
                .as("readInPositionOrder must return events in ascending position order")
                .isSorted();
        assertThat(events).extracting(CloudEvent::getId)
                .as("readInPositionOrder must return every event written, once each")
                .containsExactlyInAnyOrder("A", "B", "C");
    }

    @Test
    void read_in_position_order_honours_from_beginning_after_position_up_to_position_and_between() {
        eventStore().write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("A", DEFINED)));
        eventStore().write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("B", DEFINED)));
        eventStore().write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("C", DEFINED)));
        eventStore().write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("D", DEFINED)));

        List<CloudEvent> written = eventStore().read("stream:1").events().toList();
        long positionOfA = OccurrentCloudEventExtension.getPosition(written.get(0));
        long positionOfB = OccurrentCloudEventExtension.getPosition(written.get(1));
        long positionOfC = OccurrentCloudEventExtension.getPosition(written.get(2));
        long positionOfD = OccurrentCloudEventExtension.getPosition(written.get(3));

        assertThat(idsInPositionOrder(Filter.all(), PositionRange.fromBeginning()))
                .as("fromBeginning() must include every event written")
                .containsExactly("A", "B", "C", "D");

        assertThat(idsInPositionOrder(Filter.all(), PositionRange.afterPosition(positionOfB)))
                .as("afterPosition(..) must exclude the boundary event and everything before it")
                .containsExactly("C", "D");

        assertThat(idsInPositionOrder(Filter.all(), PositionRange.upToPosition(positionOfC)))
                .as("upToPosition(..) must include the boundary event and everything before it")
                .containsExactly("A", "B", "C");

        assertThat(idsInPositionOrder(Filter.all(), PositionRange.between(positionOfA, positionOfD)))
                .as("between(..) must exclude the lower boundary and include the upper boundary")
                .containsExactly("B", "C", "D");
    }

    @Test
    void read_in_position_order_applies_a_supplied_filter() {
        eventStore().write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("Included", "Included")));
        eventStore().write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("Excluded", "Excluded")));

        assertThat(idsInPositionOrder(Filter.type("Included"), PositionRange.fromBeginning()))
                .as("readInPositionOrder must apply the supplied filter, not return every event in range")
                .containsExactly("Included");
    }

    @Test
    void read_in_position_order_clamps_to_the_current_high_watermark() {
        eventStore().write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("A", DEFINED)));
        eventStore().write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("B", DEFINED)));

        long farPastTheEnd = positionOrderedReader().currentPosition() + 1_000_000;

        assertThat(idsInPositionOrder(Filter.all(), PositionRange.upToPosition(farPastTheEnd)))
                .as("An upper bound far past the end must clamp to the current high-watermark rather than error "
                        + "or return something else")
                .containsExactly("A", "B");
    }

    private List<String> idsInPositionOrder(Filter filter, PositionRange range) {
        try (var stream = positionOrderedReader().readInPositionOrder(filter, range)) {
            return stream.map(CloudEvent::getId).toList();
        }
    }

    private CloudEvent singleEventOf(String streamId) {
        EventStream<CloudEvent> stream = eventStore().read(streamId);
        return stream.events().findFirst().orElseThrow();
    }

    private CloudEvent singleEventOf(String streamId, String eventId) {
        return eventStore().read(streamId).events()
                .filter(cloudEvent -> cloudEvent.getId().equals(eventId))
                .findFirst()
                .orElseThrow();
    }
}
