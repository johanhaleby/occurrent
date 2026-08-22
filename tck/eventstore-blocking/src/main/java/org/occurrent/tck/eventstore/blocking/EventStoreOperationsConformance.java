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
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.cloudevents.OccurrentExtensionGetter;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.filter.Filter;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.tck.ConformanceEvents.*;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.taggedEventWithId;

/**
 * The {@link org.occurrent.eventstore.api.blocking.EventStoreOperations} contract covers deleting a whole stream,
 * deleting a single event by id and source, deleting everything a {@link Filter} matches, and updating one event in
 * place.
 * <p>
 * Extend it from a test class per store, as described on {@link EventStoreConformance}:
 * <pre>{@code
 * class PostgresqlEventStoreOperationsTest extends EventStoreOperationsConformance {
 *     @Override
 *     protected EventStoreFixture createFixture() { ... }
 * }
 * }</pre>
 * <p>
 * These are the operations an event-sourced system is not supposed to need, which is exactly why they need their own
 * tests. A store that silently does nothing on {@code deleteEvent}, or that leaves a stream version pointing past the
 * events it still holds, breaks callers in ways an append-only test never reaches.
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

        @Test
        void skip_still_counts_stream_positions_after_an_earlier_event_is_deleted() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED), event("b", CHANGED), event("c", ARCHIVED)));

            operations().deleteEvent("a", SOURCE);

            EventStream<CloudEvent> stream = eventStore().read(STREAM_ID, 1, 10);
            assertThat(idsOf(stream.eventList())).containsExactly("b", "c");
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

            assertAll(
                    () -> assertThat(queries().count()).isZero(),
                    () -> assertThat(eventStore().exists(STREAM_ID)).isFalse(),
                    () -> assertThat(eventStore().exists(OTHER_STREAM_ID)).isFalse()
            );
        }

        @Test
        void can_delete_a_single_stream_through_a_stream_id_filter() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED)));
            eventStore().write(OTHER_STREAM_ID, List.of(event("b", CHANGED)));

            operations().delete(Filter.streamId(STREAM_ID));

            assertAll(
                    () -> assertThat(eventStore().read(STREAM_ID).eventList()).isEmpty(),
                    () -> assertThat(eventStore().exists(STREAM_ID)).isFalse(),
                    () -> assertThat(idsOf(queries().all())).containsExactly("b")
            );
        }
    }

    /**
     * The position assertions below assume {@code eventStore()} writes a global position. Every store shipping
     * with Occurrent does by default. A store built with position turned off is covered separately by
     * {@link StreamPositionDisabledConformance}.
     */
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
        void rejects_an_update_function_that_returns_null() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED), event("b", CHANGED)));

            Throwable thrown = catchThrowable(() -> operations().updateEvent("b", SOURCE, original -> null));

            assertAll(
                    // The wording is part of the contract, not a cosmetic detail, so every store owes the same one.
                    () -> assertThat(thrown)
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Cloud event update function is not allowed to return null"),
                    // Removing the event instead of refusing would be the dangerous reading of a null return.
                    () -> assertThat(idsOf(eventStore().read(STREAM_ID))).containsExactly("a", "b")
            );
        }

        @Test
        void gives_back_the_event_when_the_update_changes_nothing() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED), event("b", CHANGED)));

            Optional<CloudEvent> updated = operations().updateEvent("b", SOURCE, original -> original);

            // An empty result means "no such event" everywhere else on this interface, so an unchanged event must not
            // report itself as missing.
            assertAll(
                    () -> assertThat(updated).isPresent(),
                    () -> assertThat(updated.orElseThrow().getId()).isEqualTo("b"),
                    () -> assertThat(idsOf(eventStore().read(STREAM_ID))).containsExactly("a", "b")
            );
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

        @Test
        void keeps_the_events_own_append_id_even_when_the_update_function_returns_a_fresh_event() {
            WriteResult result = eventStore().write(STREAM_ID, List.of(event("a", DEFINED), event("b", CHANGED)));
            String appendId = result.appendId().orElseThrow().toString();

            // A fresh event built from scratch, not derived from "original", has none of its extensions. The
            // store owns the append id the same way it owns streamId and streamVersion, so it must reapply it
            // rather than let a replacement event drop it.
            operations().updateEvent("b", SOURCE, original -> event("b", "NameRewritten"));

            CloudEvent updated = queries().query(Filter.id("b")).findFirst().orElseThrow();
            assertThat(extension(updated, OccurrentCloudEventExtension.APPEND_ID)).isEqualTo(appendId);
        }

        @Test
        void keeps_the_events_own_stream_identity_even_when_the_update_function_returns_a_fresh_event() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED), event("b", CHANGED)));

            // A fresh event built from scratch carries no streamId or streamVersion of its own. Both are store-owned
            // the same way the append id already is, so the update must not lose them or move the event to a stream
            // it does not belong to. Dropping them is worse than stale metadata, since calculateStreamVersion reads
            // the last event's own streamversion extension and throws when it is missing, making the whole stream
            // unreadable rather than merely wrong.
            CloudEvent updated = operations().updateEvent("b", SOURCE, original -> event("b", "NameRewritten")).orElseThrow();

            CloudEvent stored = queries().query(Filter.id("b")).findFirst().orElseThrow();
            assertAll(
                    () -> assertThat(extension(updated, OccurrentCloudEventExtension.STREAM_ID)).isEqualTo(STREAM_ID),
                    () -> assertThat(OccurrentExtensionGetter.getStreamVersion(updated)).isEqualTo(2L),
                    () -> assertThat(extension(stored, OccurrentCloudEventExtension.STREAM_ID)).isEqualTo(STREAM_ID),
                    () -> assertThat(OccurrentExtensionGetter.getStreamVersion(stored)).isEqualTo(2L),
                    () -> assertThat(idsOf(eventStore().read(STREAM_ID))).containsExactly("a", "b"),
                    () -> assertThat(eventStore().read(STREAM_ID).version()).isEqualTo(2)
            );
        }

        @Test
        void rejects_a_forged_stream_identity_and_keeps_the_original() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED), event("b", CHANGED)));

            // The updater forges a different streamId and streamVersion onto a fresh event. Both are store-owned, so
            // the update must neither move the event into the forged stream nor let a read of the real stream answer
            // the forged version, since that version is exactly what stream-level optimistic concurrency is written
            // against.
            operations().updateEvent("b", SOURCE, original -> CloudEventBuilder.v1(event("b", "NameRewritten"))
                    .withExtension(OccurrentCloudEventExtension.STREAM_ID, "forged-stream")
                    .withExtension(OccurrentCloudEventExtension.STREAM_VERSION, 999L)
                    .build());

            assertAll(
                    () -> assertThat(eventStore().read(STREAM_ID).version()).isEqualTo(2),
                    () -> assertThat(idsOf(eventStore().read(STREAM_ID))).containsExactly("a", "b"),
                    () -> assertThat(eventStore().exists("forged-stream")).isFalse()
            );
        }

        @Test
        void keeps_the_events_own_position_even_when_the_update_function_returns_a_fresh_event() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED), event("b", CHANGED)));
            CloudEvent original = queries().query(Filter.id("b")).findFirst().orElseThrow();
            long originalPosition = OccurrentCloudEventExtension.getPosition(original);

            // A fresh event built from scratch carries no position of its own. Position is store-owned the same way
            // the append id and stream identity already are, so the update must not let a replacement event drop the
            // position an earlier write stamped.
            CloudEvent updated = operations().updateEvent("b", SOURCE, original2 -> event("b", "NameRewritten")).orElseThrow();

            CloudEvent stored = queries().query(Filter.id("b")).findFirst().orElseThrow();
            assertAll(
                    () -> assertThat(OccurrentCloudEventExtension.getPosition(updated)).isEqualTo(originalPosition),
                    () -> assertThat(OccurrentCloudEventExtension.getPosition(stored)).isEqualTo(originalPosition)
            );
        }

        @Test
        void rejects_a_forged_position_and_keeps_the_original() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED), event("b", CHANGED)));
            CloudEvent original = queries().query(Filter.id("b")).findFirst().orElseThrow();
            long originalPosition = OccurrentCloudEventExtension.getPosition(original);

            // Position is store-owned, so an updater forging one of its own must not let it through.
            CloudEvent updated = operations().updateEvent("b", SOURCE,
                    original2 -> OccurrentCloudEventExtension.withPosition(event("b", "NameRewritten"), originalPosition + 999)).orElseThrow();

            CloudEvent stored = queries().query(Filter.id("b")).findFirst().orElseThrow();
            assertAll(
                    () -> assertThat(OccurrentCloudEventExtension.getPosition(updated)).isEqualTo(originalPosition),
                    () -> assertThat(OccurrentCloudEventExtension.getPosition(stored)).isEqualTo(originalPosition)
            );
        }

        @Test
        void keeps_no_dcb_tags_on_a_plain_stream_event_even_when_the_update_function_forges_some() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED)));

            // DCB tags are store-owned the same way streamId, streamVersion, the append id and position already are,
            // so an updater forging one onto a plain stream event that never carried any must not let it through.
            CloudEvent updated = operations().updateEvent("a", SOURCE,
                    original -> taggedEventWithId("a", "NameRewritten", "forged:1")).orElseThrow();

            CloudEvent stored = queries().query(Filter.id("a")).findFirst().orElseThrow();
            assertAll(
                    () -> assertThat(DcbCloudEvents.isDcbEvent(updated)).isFalse(),
                    () -> assertThat(DcbCloudEvents.isDcbEvent(stored)).isFalse()
            );
        }
    }
}
