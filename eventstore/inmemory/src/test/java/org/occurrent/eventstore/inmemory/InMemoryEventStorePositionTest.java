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
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.getPosition;

/**
 * Test for the global {@code position} shared between stream-written and DCB-appended events on
 * {@link InMemoryEventStore}. Ordering, disabled-position, and position-ordered-reader behaviour are covered
 * centrally by {@code StreamPositionConformance} and {@code StreamPositionDisabledConformance} in the TCK; this
 * class keeps only the one assertion those suites cannot make without DCB fixtures (issue #485).
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class InMemoryEventStorePositionTest {

    private static final URI SOURCE = URI.create("urn:test");

    @Test
    void stream_written_events_get_a_monotonic_position_shared_with_dcb_events() {
        InMemoryEventStore eventStore = new InMemoryEventStore().withStreamPosition();

        eventStore.append(List.of(taggedEvent("DcbEvent1", "t:1")));
        eventStore.write("stream1", WriteCondition.anyStreamVersion(), List.of(event("StreamEvent1"), event("StreamEvent2")));
        eventStore.append(List.of(taggedEvent("DcbEvent2", "t:1")));

        List<CloudEvent> dcbEvents = eventStore.read(DcbCriteria.tags(Tag.parse("t:1"))).events();
        List<CloudEvent> streamEvents = eventStore.read("stream1").events().toList();
        assertThat(streamEvents).hasSize(2);

        long dcbEvent1Position = position(dcbEvents.get(0));
        long streamEvent1Position = position(streamEvents.get(0));
        long streamEvent2Position = position(streamEvents.get(1));
        long dcbEvent2Position = position(dcbEvents.get(1));

        // Positions are shared with DCB: the two DCB appends bracket the stream write in the single global sequence.
        // A retried write may reserve (and abandon) an earlier block under contention, so positions can have gaps
        // (same as DCB, ADR 0021, DcbAppendResult); only strict monotonic ordering across the interleaved writes is
        // guaranteed, never a literal or contiguous value.
        assertThat(dcbEvent1Position).isPositive();
        assertThat(streamEvent1Position).isGreaterThan(dcbEvent1Position);
        assertThat(streamEvent2Position).isGreaterThan(streamEvent1Position);
        assertThat(dcbEvent2Position).isGreaterThan(streamEvent2Position);
        assertThat(eventStore.currentPosition()).isGreaterThanOrEqualTo(dcbEvent2Position);
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
