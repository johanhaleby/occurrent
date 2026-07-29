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
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.filter.Filter;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.tck.ConformanceEvents.SOURCE;
import static org.occurrent.tck.ConformanceEvents.event;
import static org.occurrent.tck.ConformanceEvents.idsOf;

/**
 * The {@link org.occurrent.eventstore.api.blocking.EventStoreOperations} contract: deleting a whole stream, deleting a
 * single event by id and source, deleting everything a {@link Filter} matches, and updating one event in place.
 * <p>
 * Extend it from a test class per store, as described on {@link EventStoreConformance}:
 * <pre>{@code
 * class PostgresqlEventStoreOperationsTest extends EventStoreOperationsConformance {
 *     @Override
 *     protected EventStoreFixture createFixture() { ... }
 * }
 * }</pre>
 * <p>
 * These are the operations an event-sourced system is not supposed to need, which is exactly why they need pinning: a
 * store that silently does nothing on {@code deleteEvent}, or that leaves a stream version pointing past the events it
 * still holds, breaks callers in ways an append-only test never reaches.
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the event store operations contract")
public abstract class EventStoreOperationsConformance extends EventStoreConformance {

    private static final String STREAM_ID = "name";
    private static final String OTHER_STREAM_ID = "other-name";

    private static final String DEFINED = "NameDefined";
    private static final String CHANGED = "NameWasChanged";
    private static final String ARCHIVED = "NameArchived";

    @Override
    protected final Set<EventStoreCapability> requiredCapabilities() {
        return Set.of(EventStoreCapability.STREAM);
    }

    @Nested
    @DisplayName("deleting a stream")
    class DeletingAStream {

        @Test
        void removes_every_event_in_the_stream() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED), event("b", CHANGED)));

            operations().deleteEventStream(STREAM_ID);

            assertAll(
                    () -> assertThat(eventStore().read(STREAM_ID).eventList()).isEmpty(),
                    () -> assertThat(eventStore().exists(STREAM_ID)).isFalse(),
                    () -> assertThat(queries().count()).isZero()
            );
        }

        @Test
        void leaves_every_other_stream_alone() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED)));
            eventStore().write(OTHER_STREAM_ID, List.of(event("b", DEFINED)));

            operations().deleteEventStream(STREAM_ID);

            assertAll(
                    () -> assertThat(eventStore().exists(OTHER_STREAM_ID)).isTrue(),
                    () -> assertThat(idsOf(eventStore().read(OTHER_STREAM_ID))).containsExactly("b")
            );
        }

        @Test
        void deleting_a_stream_that_was_never_written_is_not_an_error() {
            operations().deleteEventStream("never-written");

            assertThat(eventStore().exists("never-written")).isFalse();
        }

        @Test
        void the_stream_can_be_written_again_from_version_one_afterwards() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED), event("b", CHANGED)));
            operations().deleteEventStream(STREAM_ID);

            eventStore().write(STREAM_ID, List.of(event("c", DEFINED)));

            assertAll(
                    () -> assertThat(idsOf(eventStore().read(STREAM_ID))).containsExactly("c"),
                    () -> assertThat(eventStore().read(STREAM_ID).version()).isEqualTo(1)
            );
        }
    }

    @Nested
    @DisplayName("deleting a single event")
    class DeletingASingleEvent {

        @Test
        void removes_only_the_addressed_event() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED), event("b", CHANGED), event("c", ARCHIVED)));

            operations().deleteEvent("b", SOURCE);

            assertThat(idsOf(eventStore().read(STREAM_ID))).containsExactly("a", "c");
        }

        @Test
        void the_stream_still_exists_when_events_remain() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED), event("b", CHANGED)));

            operations().deleteEvent("b", SOURCE);

            assertThat(eventStore().exists(STREAM_ID)).isTrue();
        }

        @Test
        void deleting_an_event_that_does_not_exist_is_not_an_error() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED)));

            operations().deleteEvent("never-written", SOURCE);

            assertThat(idsOf(eventStore().read(STREAM_ID))).containsExactly("a");
        }

        @Test
        void a_matching_id_under_another_source_is_left_alone() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED)));

            operations().deleteEvent("a", URI.create("urn:occurrent:somewhere-else"));

            assertThat(idsOf(eventStore().read(STREAM_ID))).containsExactly("a");
        }
    }

    @Nested
    @DisplayName("deleting by filter")
    class DeletingByFilter {

        @Test
        void removes_every_event_the_filter_matches_across_streams() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED), event("b", CHANGED)));
            eventStore().write(OTHER_STREAM_ID, List.of(event("c", CHANGED)));

            operations().delete(Filter.type(CHANGED));

            assertThat(idsOf(queries().all())).containsExactly("a");
        }

        @Test
        void removes_nothing_when_the_filter_matches_nothing() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED)));

            operations().delete(Filter.type("NeverWritten"));

            assertThat(idsOf(queries().all())).containsExactly("a");
        }

        @Test
        void can_empty_the_store_entirely() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED)));
            eventStore().write(OTHER_STREAM_ID, List.of(event("b", CHANGED)));

            operations().delete(Filter.all());

            boolean existenceIsCleared = fixture().deleteByFilterClearsStreamExistence();
            assertAll(
                    () -> assertThat(queries().count()).isZero(),
                    () -> assertThat(eventStore().exists(STREAM_ID)).isEqualTo(!existenceIsCleared),
                    () -> assertThat(eventStore().exists(OTHER_STREAM_ID)).isEqualTo(!existenceIsCleared)
            );
        }

        @Test
        void can_delete_a_single_stream_through_a_stream_id_filter() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED)));
            eventStore().write(OTHER_STREAM_ID, List.of(event("b", CHANGED)));

            operations().delete(Filter.streamId(STREAM_ID));

            boolean existenceIsCleared = fixture().deleteByFilterClearsStreamExistence();
            assertAll(
                    () -> assertThat(eventStore().read(STREAM_ID).eventList()).isEmpty(),
                    () -> assertThat(eventStore().exists(STREAM_ID)).isEqualTo(!existenceIsCleared),
                    () -> assertThat(idsOf(queries().all())).containsExactly("b")
            );
        }
    }

    @Nested
    @DisplayName("updating an event")
    class UpdatingAnEvent {

        @Test
        void gives_back_the_updated_event_and_stores_it() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED), event("b", CHANGED)));

            Optional<CloudEvent> updated = operations().updateEvent("b", SOURCE,
                    original -> CloudEventBuilder.v1(original).withSubject("rewritten").build());

            assertAll(
                    () -> assertThat(updated).isPresent(),
                    () -> assertThat(updated.orElseThrow().getSubject()).isEqualTo("rewritten"),
                    () -> assertThat(queries().query(Filter.id("b")).findFirst().orElseThrow().getSubject())
                            .isEqualTo("rewritten")
            );
        }

        @Test
        void leaves_every_other_event_untouched() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED, "first"), event("b", CHANGED, "second")));

            operations().updateEvent("b", SOURCE,
                    original -> CloudEventBuilder.v1(original).withSubject("rewritten").build());

            assertThat(queries().query(Filter.id("a")).findFirst().orElseThrow().getSubject()).isEqualTo("first");
        }

        @Test
        void gives_back_nothing_when_the_event_does_not_exist() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED)));

            Optional<CloudEvent> updated = operations().updateEvent("never-written", SOURCE,
                    original -> CloudEventBuilder.v1(original).withSubject("rewritten").build());

            assertThat(updated).isEmpty();
        }

        @Test
        void does_not_change_how_many_events_the_stream_holds() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED), event("b", CHANGED)));

            operations().updateEvent("b", SOURCE,
                    original -> CloudEventBuilder.v1(original).withSubject("rewritten").build());

            assertAll(
                    () -> assertThat(idsOf(eventStore().read(STREAM_ID))).containsExactly("a", "b"),
                    () -> assertThat(eventStore().read(STREAM_ID).version()).isEqualTo(2)
            );
        }
    }
}
