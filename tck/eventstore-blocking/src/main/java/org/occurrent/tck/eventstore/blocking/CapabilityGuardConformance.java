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
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.StreamReadFilter;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.filter.Filter;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.tck.ConformanceEvents.*;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.*;

/**
 * What a store does when asked for a capability it was not built with.
 * <p>
 * A capability is a construction-time argument, and a store implements every interface either way, so there is always
 * an object to call and nothing on it announces that the call will not work. The contract is that such a call refuses
 * with an {@link UnsupportedOperationException} naming the missing capability. Refusing matters more than it looks:
 * quietly answering "no events" to a DCB read on a store that never enabled DCB is indistinguishable from an empty
 * store, so a decider would append against a boundary it never actually checked.
 * <p>
 * The suite needs two extra stores, one per direction, which the fixture builds through
 * {@link EventStoreFixture#storeWithoutDcb()} and {@link EventStoreFixture#storeWithoutStream()}. Extending this suite
 * is the promise that both exist, so an empty answer from either fails rather than skipping.
 * <p>
 * Each group also asserts that its restricted store still serves the capability it <em>was</em> built with. Without
 * that, a fixture handing back a store that refuses everything, or one closed before the test ran, would pass every
 * assertion here.
 * <p>
 * A store built with both capabilities is not covered here. Nothing refuses in that configuration, so what is worth
 * asserting is that the two halves coexist without seeing each other's events, which is
 * {@link DcbStreamInteropConformance}.
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the capability guards")
public abstract class CapabilityGuardConformance extends EventStoreConformance {

    private static final String DEFINED = "NameDefined";
    private static final String NAME_1 = "name:1";
    private static final String STREAM_1 = "stream:1";

    /**
     * Both capabilities, because this suite asserts refusal in both directions and only an implementation that can do
     * both is able to build a store with each one alone.
     */
    @Override
    protected final Set<EventStoreCapability> requiredCapabilities() {
        return Set.of(EventStoreCapability.STREAM, EventStoreCapability.DCB);
    }

    @Nested
    @DisplayName("a store built without the DCB capability")
    class WithoutDcb {

        @Test
        void every_dcb_read_refuses() {
            DcbEventStore store = storeWithoutDcb().dcbEventStore();
            DcbCriteria criteria = DcbCriteria.tags(tag(NAME_1));

            assertAll(
                    () -> assertRefuses(EventStoreCapability.DCB, () -> store.read(criteria)),
                    () -> assertRefuses(EventStoreCapability.DCB, () -> store.read(criteria, DcbReadOptions.fromBeginning())),
                    () -> assertRefuses(EventStoreCapability.DCB, () -> store.exists(criteria)),
                    () -> assertRefuses(EventStoreCapability.DCB, () -> store.exists(criteria, DcbReadOptions.fromBeginning())),
                    () -> assertRefuses(EventStoreCapability.DCB, () -> store.count(criteria)),
                    () -> assertRefuses(EventStoreCapability.DCB, () -> store.count(criteria, DcbReadOptions.fromBeginning()))
            );
        }

        @Test
        void every_dcb_append_refuses() {
            DcbEventStore store = storeWithoutDcb().dcbEventStore();
            List<CloudEvent> events = List.of(taggedEvent(DEFINED, NAME_1));

            assertAll(
                    () -> assertRefuses(EventStoreCapability.DCB, () -> store.append(events)),
                    () -> assertRefuses(EventStoreCapability.DCB, () -> store.append(events, failIfEventsMatch(DcbCriteria.tags(tag(NAME_1)))))
            );
        }

        @Test
        void the_stream_capability_it_was_built_with_still_works() {
            // Proves the refusals above are scoped to DCB rather than this being a store that refuses everything.
            EventStore store = storeWithoutDcb().eventStore();
            store.write(STREAM_1, WriteCondition.anyStreamVersion(), List.of(event("A", DEFINED)));

            assertThat(idsOf(store.read(STREAM_1).events())).containsExactly("A");
        }
    }

    @Nested
    @DisplayName("a store built without the STREAM capability")
    class WithoutStream {

        @Test
        void every_stream_read_refuses() {
            StoreWithoutStream store = storeWithoutStream();

            assertAll(
                    () -> assertRefuses(EventStoreCapability.STREAM, () -> store.eventStore().read(STREAM_1)),
                    () -> assertRefuses(EventStoreCapability.STREAM, () -> store.eventStore().read(STREAM_1, 0, 10)),
                    () -> assertRefuses(EventStoreCapability.STREAM, () -> store.filteredReader().read(STREAM_1, StreamReadFilter.type(DEFINED), 0, 10)),
                    () -> assertRefuses(EventStoreCapability.STREAM, () -> store.eventStore().exists(STREAM_1))
            );
        }

        @Test
        void every_stream_write_refuses() {
            StoreWithoutStream store = storeWithoutStream();
            List<CloudEvent> events = List.of(event("A", DEFINED));

            assertAll(
                    () -> assertRefuses(EventStoreCapability.STREAM, () -> store.eventStore().write(STREAM_1, events)),
                    () -> assertRefuses(EventStoreCapability.STREAM, () -> store.eventStore().write(STREAM_1, WriteCondition.anyStreamVersion(), events))
            );
        }

        @Test
        void every_query_refuses() {
            StoreWithoutStream store = storeWithoutStream();

            assertAll(
                    () -> assertRefuses(EventStoreCapability.STREAM, () -> consume(store.queries().query(Filter.all(), 0, 10, SortBy.unsorted()))),
                    () -> assertRefuses(EventStoreCapability.STREAM, () -> store.queries().count(Filter.all())),
                    () -> assertRefuses(EventStoreCapability.STREAM, () -> store.queries().exists(Filter.all()))
            );
        }

        @Test
        void every_operation_refuses() {
            StoreWithoutStream store = storeWithoutStream();

            assertAll(
                    () -> assertRefuses(EventStoreCapability.STREAM, () -> store.operations().deleteEventStream(STREAM_1)),
                    () -> assertRefuses(EventStoreCapability.STREAM, () -> store.operations().deleteEvent("A", SOURCE)),
                    () -> assertRefuses(EventStoreCapability.STREAM, () -> store.operations().delete(Filter.all())),
                    () -> assertRefuses(EventStoreCapability.STREAM, () -> store.operations().updateEvent("A", SOURCE, cloudEvent -> cloudEvent))
            );
        }

        @Test
        void the_dcb_capability_it_was_built_with_still_works() {
            // Proves the refusals above are scoped to STREAM rather than this being a store that refuses everything.
            DcbEventStore store = storeWithoutStream().dcbEventStore();
            store.append(List.of(taggedEvent(DEFINED, NAME_1)));

            assertThat(typesOf(store.read(DcbCriteria.tags(tag(NAME_1))).events())).containsExactly(DEFINED);
        }
    }

    /**
     * Asserts that a call refuses because {@code capability} is not enabled.
     * <p>
     * The type is pinned and the message is only required to name the capability. The stores shipping with Occurrent
     * word it as "DCB capability is not enabled for this MongoEventStore", which carries the implementing class and so
     * cannot be cross-store law, the same line {@code DuplicateCloudEventException.getDetails()} sits on. A subclass of
     * {@link UnsupportedOperationException} passes, since choosing a more specific type is not a contract violation.
     */
    private static void assertRefuses(EventStoreCapability capability, ThrowingCallable call) {
        assertThatThrownBy(call)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(capability.name());
    }

    /**
     * Drains a query stream, so a store that returns a lazy one refuses here rather than going unchecked.
     * {@code BlockingEventStoreOverReactive} returns exactly that, since a {@code Flux.error(..)} reaches the caller
     * only once something consumes it.
     */
    private static void consume(Stream<CloudEvent> events) {
        try (events) {
            events.forEach(event -> {
            });
        }
    }

    private StoreWithoutDcb storeWithoutDcb() {
        return fixture().storeWithoutDcb().orElseThrow(() -> new AssertionError(
                fixture().getClass().getName() + " does not override storeWithoutDcb(), so " + getClass().getName()
                        + " has no store to assert the DCB guards against. Override storeWithoutDcb() to build a store "
                        + "with the STREAM capability alone."));
    }

    private StoreWithoutStream storeWithoutStream() {
        return fixture().storeWithoutStream().orElseThrow(() -> new AssertionError(
                fixture().getClass().getName() + " does not override storeWithoutStream(), so " + getClass().getName()
                        + " has no store to assert the STREAM guards against. Override storeWithoutStream() to build a "
                        + "store with the DCB capability alone."));
    }
}
