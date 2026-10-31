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

package org.occurrent.eventstore.api.dcb;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;

import java.net.URI;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbEventStreamTest {

    @Test
    void has_no_event_below_a_position_when_the_read_returned_no_events() {
        DcbEventStream read = readReturning(List.of(), 10);

        assertThat(read.hasEventBelow(10)).isFalse();
    }

    @Test
    void has_an_event_below_a_position_when_one_event_is_at_a_lower_position() {
        DcbEventStream read = readReturning(List.of(eventAt(41), eventAt(43)), 43);

        assertThat(read.hasEventBelow(42)).isTrue();
    }

    @Test
    void has_no_event_below_a_position_when_every_event_is_at_or_above_it() {
        DcbEventStream read = readReturning(List.of(eventAt(42), eventAt(43)), 43);

        assertThat(read.hasEventBelow(42)).isFalse();
    }

    @Test
    void an_event_without_a_position_counts_as_below_any_position_above_zero() {
        DcbEventStream read = readReturning(List.of(eventWithoutPosition()), 5);

        assertThat(read.hasEventBelow(1)).isTrue();
    }

    @Test
    void an_event_without_a_position_is_not_below_position_zero() {
        DcbEventStream read = readReturning(List.of(eventWithoutPosition()), 5);

        assertThat(read.hasEventBelow(0)).isFalse();
    }

    @Test
    void the_highest_event_position_is_the_highest_among_the_returned_events_not_the_head_of_the_store() {
        DcbEventStream read = readReturning(List.of(eventAt(43), eventAt(41), eventAt(42)), 99);

        assertThat(read.highestEventPosition()).isEqualTo(43);
    }

    @Test
    void the_highest_event_position_is_zero_when_the_read_returned_no_events() {
        DcbEventStream read = readReturning(List.of(), 99);

        assertThat(read.highestEventPosition()).isZero();
    }

    @Test
    void the_highest_event_position_is_zero_when_no_returned_event_has_a_position() {
        DcbEventStream read = readReturning(List.of(eventWithoutPosition(), eventWithoutPosition()), 99);

        assertThat(read.highestEventPosition()).isZero();
    }

    private static DcbEventStream readReturning(List<CloudEvent> events, long lastSequencePosition) {
        return new DcbEventStream(events, lastSequencePosition, DcbConsistencyToken.of(DcbCriteria.all(), lastSequencePosition));
    }

    private static CloudEvent eventAt(long position) {
        return OccurrentCloudEventExtension.withPosition(eventWithoutPosition(), position);
    }

    private static CloudEvent eventWithoutPosition() {
        return CloudEventBuilder.v1()
                .withId("id")
                .withSource(URI.create("urn:test"))
                .withType("type")
                .build();
    }
}
