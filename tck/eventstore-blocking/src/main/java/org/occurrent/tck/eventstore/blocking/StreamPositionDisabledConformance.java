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
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.tck.ConformanceEvents.event;

/**
 * The documented behaviour of a store built with its global position turned off: {@link EventStoreFixture#storeWithoutPosition()}.
 * <p>
 * Not every implementation can build one. {@link EventStoreFixture#storeWithoutPosition()} defaults to
 * {@link java.util.Optional#empty()} for exactly that reason, and this suite treats an empty answer as a test failure
 * rather than something to skip past: see {@link #storeWithoutPosition()} below. The TCK bans {@code Assumptions}
 * everywhere else for the same reason, and an {@code Optional} accessor is not an exemption from it.
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the position-disabled contract")
public abstract class StreamPositionDisabledConformance extends EventStoreConformance {

    private static final String DEFINED = "NameDefined";

    @Override
    protected final Set<EventStoreCapability> requiredCapabilities() {
        return Set.of(EventStoreCapability.STREAM);
    }

    @Test
    void the_position_ordered_reader_reports_that_it_writes_no_position() {
        PositionOrderedReader reader = storeWithoutPosition().positionOrderedReader();

        assertThat(reader.writesPosition())
                .as("writesPosition() must be false for a store built with position turned off")
                .isFalse();
    }

    @Test
    void a_written_event_carries_no_position() {
        EventStore store = storeWithoutPosition().eventStore();
        store.write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("A", DEFINED)));

        CloudEvent written = store.read("stream:1").events().findFirst().orElseThrow();

        assertThat(OccurrentCloudEventExtension.getPosition(written))
                .as("getPosition(..) must answer 0 for an event written with position turned off")
                .isZero();
        assertThat(written.getExtensionNames())
                .as("the raw position extension must be entirely absent from the event, not merely unreadable")
                .doesNotContain(OccurrentCloudEventExtension.POSITION);
    }

    @Test
    void current_position_refuses_to_answer_and_says_why() {
        PositionOrderedReader reader = storeWithoutPosition().positionOrderedReader();

        assertThatThrownBy(reader::currentPosition)
                .isExactlyInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("does not write a position");
    }

    @Test
    void read_in_position_order_refuses_to_answer_and_says_why() {
        PositionOrderedReader reader = storeWithoutPosition().positionOrderedReader();

        assertThatThrownBy(() -> {
            try (var stream = reader.readInPositionOrder(Filter.all(), PositionRange.fromBeginning())) {
                stream.toList();
            }
        })
                .isExactlyInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("does not write a position");
    }

    /**
     * The store under test, built with its global position turned off.
     * <p>
     * {@link EventStoreFixture#storeWithoutPosition()} answers {@link java.util.Optional#empty()} when an
     * implementation cannot build one, which is a legitimate answer for a fixture to give. It is not, however, a
     * legitimate reason for this suite to pass or to skip: every test above needs the store to make its assertion,
     * so an empty answer fails loudly here, naming the fixture that owes an override, exactly as
     * {@link EventStoreFixture}'s own {@code notOverridden(..)} fails loudly for a capability declared but not wired
     * up. A store that declines this behaviour is expected to say so by never reaching this failure in the first
     * place, i.e. by supplying a {@link StoreWithoutPosition}, since all four stores shipping with Occurrent do.
     */
    private StoreWithoutPosition storeWithoutPosition() {
        return fixture().storeWithoutPosition().orElseThrow(() -> new AssertionError(
                fixture().getClass().getName() + " does not override storeWithoutPosition(), so " + getClass().getName()
                        + " has no store to assert the position-disabled contract against. Override "
                        + "storeWithoutPosition() to build a STREAM-only store with its position turned off."));
    }
}
