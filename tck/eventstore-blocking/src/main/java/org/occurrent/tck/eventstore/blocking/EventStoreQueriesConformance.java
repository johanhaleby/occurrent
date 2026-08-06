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
import io.cloudevents.CloudEventData;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.filter.Filter;
import org.occurrent.tck.ConformanceEvents;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.condition.Condition.*;
import static org.occurrent.eventstore.api.SortBy.SortDirection.ASCENDING;
import static org.occurrent.eventstore.api.SortBy.SortDirection.DESCENDING;
import static org.occurrent.tck.ConformanceEvents.*;

/**
 * The {@link org.occurrent.eventstore.api.blocking.EventStoreQueries} contract: reading across streams with a
 * {@link Filter}, a {@link SortBy}, and paging, plus {@code count} and {@code exists}.
 * <p>
 * Extend it from a test class per store, as described on {@link EventStoreConformance}:
 * <pre>{@code
 * class PostgresqlEventStoreQueriesTest extends EventStoreQueriesConformance {
 *     @Override
 *     protected EventStoreFixture createFixture() { ... }
 * }
 * }</pre>
 * <p>
 * Most of what is here filters and sorts on CloudEvent attributes the specification defines, so a store that keeps
 * events in a shape this TCK knows nothing about still has to answer. Three places are documented to differ, each
 * declared by the fixture and asserted both ways rather than skipped: composing a natural sort step with a field
 * sort ({@link EventStoreFixture#composesNaturalSortWithFieldSorts()}), whether natural order is insertion order
 * ({@link EventStoreFixture#naturalOrderIsInsertionOrder()}), and whether the store can filter on a field inside the
 * {@code data} payload ({@link EventStoreFixture#supportsDataFilter()}).
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the event store queries contract")
public abstract class EventStoreQueriesConformance extends EventStoreConformance {

    private static final String STREAM_ID = "name";
    private static final String OTHER_STREAM_ID = "other-name";

    private static final String DEFINED = "NameDefined";
    private static final String CHANGED = "NameWasChanged";
    private static final String ARCHIVED = "NameArchived";

    private static final URI DATA_SCHEMA = URI.create("urn:occurrent:tck:schema");

    @Override
    protected final Set<EventStoreCapability> requiredCapabilities() {
        return Set.of(EventStoreCapability.STREAM);
    }

    @Nested
    @DisplayName("counting")
    class Counting {

        @Test
        void counts_nothing_in_an_empty_store() {
            assertAll(
                    () -> assertThat(queries().count()).isZero(),
                    () -> assertThat(queries().count(Filter.all())).isZero()
            );
        }

        @Test
        void counts_every_event_across_every_stream() {
            eventStore().write(STREAM_ID, List.of(event(DEFINED), event(CHANGED)));
            eventStore().write(OTHER_STREAM_ID, List.of(event(DEFINED)));

            assertThat(queries().count()).isEqualTo(3);
        }

        @Test
        void counts_only_the_events_a_filter_matches() {
            eventStore().write(STREAM_ID, List.of(event(DEFINED), event(CHANGED), event(CHANGED)));

            assertAll(
                    () -> assertThat(queries().count(Filter.type(DEFINED))).isEqualTo(1),
                    () -> assertThat(queries().count(Filter.type(CHANGED))).isEqualTo(2),
                    () -> assertThat(queries().count(Filter.type(ARCHIVED))).isZero()
            );
        }

        @Test
        void counts_ignore_paging_because_count_takes_no_skip_or_limit() {
            eventStore().write(STREAM_ID, List.of(event(DEFINED), event(CHANGED), event(ARCHIVED)));

            assertThat(queries().count(Filter.all())).isEqualTo(3);
        }
    }

    @Nested
    @DisplayName("existence")
    class Existence {

        @Test
        void nothing_exists_in_an_empty_store() {
            assertThat(queries().exists(Filter.all())).isFalse();
        }

        @Test
        void reports_whether_anything_matches_the_filter() {
            eventStore().write(STREAM_ID, List.of(event(DEFINED)));

            assertAll(
                    () -> assertThat(queries().exists(Filter.all())).isTrue(),
                    () -> assertThat(queries().exists(Filter.type(DEFINED))).isTrue(),
                    () -> assertThat(queries().exists(Filter.type(CHANGED))).isFalse()
            );
        }
    }

    @Nested
    @DisplayName("filtering on a cloud event attribute")
    class FilteringOnAnAttribute {

        @Test
        void filters_on_id() {
            CloudEvent wanted = event("wanted", DEFINED);
            eventStore().write(STREAM_ID, List.of(wanted, event("other", CHANGED)));

            assertThat(idsOf(queries().query(Filter.id("wanted")))).containsExactly("wanted");
        }

        @Test
        void filters_on_type() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED), event("b", CHANGED)));

            assertThat(idsOf(queries().query(Filter.type(CHANGED)))).containsExactly("b");
        }

        @Test
        void filters_on_subject() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED, "first"), event("b", DEFINED, "second")));

            assertThat(idsOf(queries().query(Filter.subject("second")))).containsExactly("b");
        }

        @Test
        void filters_on_source() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED)));

            assertAll(
                    () -> assertThat(idsOf(queries().query(Filter.source(SOURCE)))).containsExactly("a"),
                    () -> assertThat(idsOf(queries().query(Filter.source(URI.create("urn:nope"))))).isEmpty()
            );
        }

        @Test
        void filters_on_a_time_range() {
            OffsetDateTime later = TIME.plusHours(1);
            eventStore().write(STREAM_ID, List.of(eventAt("a", DEFINED, TIME), eventAt("b", CHANGED, later)));

            assertAll(
                    () -> assertThat(idsOf(queries().query(Filter.time(lt(later))))).containsExactly("a"),
                    () -> assertThat(idsOf(queries().query(Filter.time(gte(later))))).containsExactly("b")
            );
        }

        @Test
        void matches_an_exact_time() {
            // TIME has zero seconds and zero nanos on purpose. A store that renders the stored value and the filter
            // value differently misses exactly this case and nothing else, which is how it went unnoticed for so long.
            eventStore().write(STREAM_ID, List.of(eventAt("a", DEFINED, TIME), eventAt("b", CHANGED, TIME.plusHours(1))));

            assertThat(idsOf(queries().query(Filter.time(TIME)))).containsExactly("a");
        }

        @Test
        void filters_on_data_schema() {
            eventStore().write(STREAM_ID, List.of(
                    ConformanceEvents.eventWithDataSchema("a", DEFINED, DATA_SCHEMA),
                    event("b", CHANGED)));

            assertAll(
                    () -> assertThat(idsOf(queries().query(Filter.dataSchema(DATA_SCHEMA)))).containsExactly("a"),
                    () -> assertThat(idsOf(queries().query(Filter.dataSchema(URI.create("urn:other"))))).isEmpty()
            );
        }

        @Test
        void filters_on_data_content_type() {
            eventStore().write(STREAM_ID, List.of(
                    event("a", DEFINED),
                    ConformanceEvents.eventWithDataContentType("b", CHANGED, "text/plain", "text".getBytes(UTF_8))));

            assertAll(
                    () -> assertThat(idsOf(queries().query(Filter.dataContentType("text/plain")))).containsExactly("b"),
                    () -> assertThat(idsOf(queries().query(Filter.dataContentType(ConformanceEvents.CONTENT_TYPE))))
                            .containsExactly("a")
            );
        }

        @Test
        void filters_on_the_stream_id_extension_a_store_stamps_on_write() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED)));
            eventStore().write(OTHER_STREAM_ID, List.of(event("b", DEFINED)));

            assertThat(idsOf(queries().query(Filter.streamId(OTHER_STREAM_ID)))).containsExactly("b");
        }

        @Test
        void filters_on_the_stream_version_extension_a_store_stamps_on_write() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED), event("b", CHANGED)));

            assertThat(idsOf(queries().query(Filter.streamVersion(2L)))).containsExactly("b");
        }

        @Test
        void filters_on_a_specific_cloud_event_by_id_and_source() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED), event("b", CHANGED)));

            assertThat(idsOf(queries().query(Filter.cloudEvent("b", SOURCE)))).containsExactly("b");
        }

        @Test
        void matches_everything_with_the_all_filter() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED), event("b", CHANGED)));

            assertThat(idsOf(queries().query(Filter.all()))).containsExactlyInAnyOrder("a", "b");
        }
    }

    @Nested
    @DisplayName("filtering on the data payload")
    class FilteringOnData {

        /**
         * The payload shapes the tests below query. Written once so every test sees the same store, and named for what
         * makes each one interesting rather than for its content.
         */
        private void writePayloads() {
            eventStore().write(STREAM_ID, List.of(
                    ConformanceEvents.eventWithJsonData("flat", DEFINED, "{\"name\":\"alice\"}"),
                    ConformanceEvents.eventWithJsonData("nested", DEFINED, "{\"person\":{\"city\":\"Malmo\"},\"name\":\"bob\"}"),
                    ConformanceEvents.eventWithJsonData("whole", DEFINED, "{\"amount\":42,\"name\":\"carol\"}"),
                    ConformanceEvents.eventWithJsonData("fraction", DEFINED, "{\"amount\":42.5,\"name\":\"dave\"}"),
                    ConformanceEvents.eventWithJsonData("listed", DEFINED, "{\"tags\":[\"red\",\"blue\"],\"name\":\"erin\"}"),
                    ConformanceEvents.eventWithJsonData("rootArray", DEFINED, "[1,2,3]")));
        }

        /**
         * Runs the assertions when the store declares it can read a payload, and otherwise asserts that it refuses.
         * Returning early after asserting the refusal is not a skip: the store answered for itself either way.
         */
        private boolean declinesDataFilters(Filter probe) {
            // Call this after writing, never before. A query over an empty store never reaches the filter, so a store
            // that would refuse the filter returns nothing instead of throwing.

            if (fixture().supportsDataFilter()) {
                return false;
            }
            assertThat(catchThrowable(() -> idsOf(queries().query(probe))))
                    .as("a store that declares it cannot filter on the data payload must reject Filter.data(..)")
                    .isExactlyInstanceOf(IllegalArgumentException.class);
            return true;
        }

        @Test
        void filters_on_a_nested_field_through_a_dotted_path() {
            Filter byCity = Filter.data("person.city", eq("Malmo"));
            writePayloads();
            if (declinesDataFilters(byCity)) {
                return;
            }

            assertAll(
                    () -> assertThat(idsOf(queries().query(byCity)))
                            .as("a dotted path must reach a field inside a nested object")
                            .containsExactly("nested"),
                    () -> assertThat(idsOf(queries().query(Filter.data("person.city", eq("Lund")))))
                            .as("a dotted path that reaches the field but not the value must match nothing")
                            .isEmpty()
            );
        }

        @Test
        void reads_a_number_as_a_number_rather_than_as_its_text() {
            Filter byNumber = Filter.data("amount", eq(42));
            writePayloads();
            if (declinesDataFilters(byNumber)) {
                return;
            }

            assertAll(
                    () -> assertThat(idsOf(queries().query(byNumber)))
                            .as("a numeric operand must match a numeric field")
                            .containsExactly("whole"),
                    () -> assertThat(idsOf(queries().query(Filter.data("amount", eq("42")))))
                            .as("a text operand must not match a numeric field, because the two are different values "
                                    + "rather than two spellings of one")
                            .isEmpty()
            );
        }

        @Test
        void compares_numbers_by_value_but_refuses_to_compare_across_types() {
            Filter above = Filter.data("amount", gt(10));
            writePayloads();
            if (declinesDataFilters(above)) {
                return;
            }

            assertAll(
                    () -> assertThat(idsOf(queries().query(above)))
                            .as("a range operator must compare a whole number and a fraction by value")
                            .containsExactlyInAnyOrder("whole", "fraction"),
                    () -> assertThat(idsOf(queries().query(Filter.data("amount", lt(100)))))
                            .as("and must do so in the other direction too")
                            .containsExactlyInAnyOrder("whole", "fraction"),
                    () -> assertThat(idsOf(queries().query(Filter.data("amount", gt("10")))))
                            .as("a range operator given an operand of a different type than the stored value must "
                                    + "match nothing, rather than failing")
                            .isEmpty()
            );
        }

        @Test
        void matches_an_element_inside_an_array_field() {
            Filter tagged = Filter.data("tags", eq("red"));
            writePayloads();
            if (declinesDataFilters(tagged)) {
                return;
            }

            assertAll(
                    () -> assertThat(idsOf(queries().query(tagged)))
                            .as("comparing an array field to a value must match when the array holds that value")
                            .containsExactly("listed"),
                    () -> assertThat(idsOf(queries().query(Filter.data("tags", eq("green")))))
                            .as("and must not match when it does not")
                            .isEmpty()
            );
        }

        @Test
        void matches_nothing_when_the_path_leads_nowhere() {
            Filter nowhere = Filter.data("nosuchfield", eq("x"));
            writePayloads();
            if (declinesDataFilters(nowhere)) {
                return;
            }

            assertAll(
                    () -> assertThat(idsOf(queries().query(nowhere)))
                            .as("a field no payload has must match nothing")
                            .isEmpty(),
                    () -> assertThat(idsOf(queries().query(Filter.data("name.deeper", eq("x")))))
                            .as("a path that continues past a value that is not an object must match nothing")
                            .isEmpty(),
                    () -> assertThat(idsOf(queries().query(Filter.data("0", eq(1)))))
                            .as("a payload whose root is an array rather than an object is not reachable by field, "
                                    + "so nothing matches")
                            .isEmpty()
            );
        }

        @Test
        void filters_on_a_field_inside_the_data_payload_according_to_what_the_fixture_declares() {
            // ConformanceEvents builds a body of {"name":"<subject>"}, so filtering data.name for a subject picks out
            // exactly the event written with that subject.
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED, "first"), event("b", CHANGED, "second")));

            Filter secondByData = Filter.data("name", eq("second"));

            if (fixture().supportsDataFilter()) {
                CloudEvent matched;
                try (Stream<CloudEvent> matches = queries().query(secondByData)) {
                    matched = matches.findFirst().orElseThrow();
                }

                assertAll(
                        () -> assertThat(matched.getId())
                                .as("Filter.data(..) must pick out only the event whose payload field matches")
                                .isEqualTo("b"),
                        () -> assertThat(withoutWhitespace(matched.getData()))
                                .as("the payload content must survive filtering, even if a store reformats it "
                                        + "while parsing it to reach inside")
                                .isEqualTo(withoutWhitespace(ConformanceEvents.dataFor("second")))
                );
            } else {
                Throwable thrown = catchThrowable(() -> idsOf(queries().query(secondByData)));

                assertThat(thrown)
                        .as("a store that declares it cannot filter on the data payload must reject Filter.data(..) "
                                + "rather than silently ignoring it or scanning every payload unindexed")
                        .isExactlyInstanceOf(IllegalArgumentException.class);
            }
        }

        /**
         * The payload with every space, tab and newline removed.
         * <p>
         * A store is not required to hand the payload back byte for byte. The MongoDB stores parse it into a BSON
         * document so that {@code Filter.data(..)} can reach inside it, and re-serialising that document reformats
         * the JSON, which shows up as a space after each colon. What the contract requires is that the data
         * survives, so the comparison ignores formatting.
         */
        private String withoutWhitespace(@Nullable CloudEventData data) {
            return withoutWhitespace(requireNonNull(data, "event matched by Filter.data(..) must carry data").toBytes());
        }

        private String withoutWhitespace(byte[] data) {
            return new String(data, UTF_8).replaceAll("\\s", "");
        }
    }

    @Nested
    @DisplayName("filtering with a condition")
    class FilteringWithACondition {

        @Test
        void eq_matches_only_an_exact_value() {
            writeThreeSubjects();

            assertThat(idsOf(queries().query(Filter.subject(eq("second"))))).containsExactly("b");
        }

        @Test
        void ne_matches_everything_else() {
            writeThreeSubjects();

            assertThat(idsOf(queries().query(Filter.subject(ne("second"))))).containsExactlyInAnyOrder("a", "c");
        }

        @Test
        void in_matches_any_of_the_supplied_values() {
            writeThreeSubjects();

            assertThat(idsOf(queries().query(Filter.subject(in("first", "third")))))
                    .containsExactlyInAnyOrder("a", "c");
        }

        @Test
        void lt_and_lte_bound_from_above() {
            writeThreeStreamVersions();

            assertAll(
                    () -> assertThat(idsOf(queries().query(Filter.streamVersion(lt(3L))))).containsExactlyInAnyOrder("a", "b"),
                    () -> assertThat(idsOf(queries().query(Filter.streamVersion(lte(2L))))).containsExactlyInAnyOrder("a", "b")
            );
        }

        @Test
        void gt_and_gte_bound_from_below() {
            writeThreeStreamVersions();

            assertAll(
                    () -> assertThat(idsOf(queries().query(Filter.streamVersion(gt(2L))))).containsExactly("c"),
                    () -> assertThat(idsOf(queries().query(Filter.streamVersion(gte(2L))))).containsExactlyInAnyOrder("b", "c")
            );
        }

        @Test
        void and_requires_every_condition_to_hold() {
            writeThreeStreamVersions();

            assertThat(idsOf(queries().query(Filter.streamVersion(and(gte(2L), lt(3L)))))).containsExactly("b");
        }

        @Test
        void not_inverts_a_condition() {
            writeThreeSubjects();

            assertThat(idsOf(queries().query(Filter.subject(not(eq("second"))))))
                    .containsExactlyInAnyOrder("a", "c");
        }

        @Test
        void and_composes_two_filters_on_different_attributes() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED, "first"), event("b", CHANGED, "first")));
            eventStore().write(OTHER_STREAM_ID, List.of(event("c", CHANGED, "first")));

            Filter changedInTheFirstStream = Filter.type(CHANGED).and(Filter.streamId(STREAM_ID));

            assertThat(idsOf(queries().query(changedInTheFirstStream))).containsExactly("b");
        }

        @Test
        void or_composes_two_filters_on_different_attributes() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED), event("b", CHANGED), event("c", ARCHIVED)));

            Filter definedOrArchived = Filter.type(DEFINED).or(Filter.type(ARCHIVED));

            assertThat(idsOf(queries().query(definedOrArchived))).containsExactlyInAnyOrder("a", "c");
        }

        private void writeThreeSubjects() {
            eventStore().write(STREAM_ID, List.of(
                    event("a", DEFINED, "first"),
                    event("b", CHANGED, "second"),
                    event("c", ARCHIVED, "third")));
        }

        private void writeThreeStreamVersions() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED), event("b", CHANGED), event("c", ARCHIVED)));
        }
    }

    @Nested
    @DisplayName("sorting")
    class Sorting {

        @Test
        void sorts_by_a_field_ascending_and_descending() {
            writeThreeAtDistinctTimes();

            assertAll(
                    () -> assertThat(idsOf(queries().all(SortBy.time(ASCENDING)))).containsExactly("a", "b", "c"),
                    () -> assertThat(idsOf(queries().all(SortBy.time(DESCENDING)))).containsExactly("c", "b", "a")
            );
        }

        @Test
        void sorts_by_stream_version_in_both_directions() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED), event("b", CHANGED), event("c", ARCHIVED)));

            assertAll(
                    () -> assertThat(idsOf(queries().all(SortBy.streamVersion(ASCENDING)))).containsExactly("a", "b", "c"),
                    () -> assertThat(idsOf(queries().all(SortBy.streamVersion(DESCENDING)))).containsExactly("c", "b", "a")
            );
        }

        @Test
        void sorts_by_natural_order_according_to_what_the_fixture_declares() {
            // Times are deliberately skewed and interleaved across two streams: b sorts after a by time, but c goes
            // back in time yet is appended to stream_id last. A store that groups by stream, or that sorts by time
            // instead of insertion order, gives a different answer than the one asserted below.
            eventStore().write(STREAM_ID, List.of(eventAt("a", DEFINED, TIME)));
            eventStore().write(OTHER_STREAM_ID, List.of(eventAt("b", CHANGED, TIME.plusSeconds(5))));
            eventStore().write(STREAM_ID, List.of(eventAt("c", ARCHIVED, TIME.minusSeconds(3))));

            if (fixture().naturalOrderIsInsertionOrder()) {
                assertAll(
                        () -> assertThat(idsOf(queries().all(SortBy.natural(ASCENDING))))
                                .as("a store that declares natural order as insertion order must return events in "
                                        + "the order they were written, across every stream")
                                .containsExactly("a", "b", "c"),
                        () -> assertThat(idsOf(queries().all(SortBy.natural(DESCENDING))))
                                .as("descending natural order must be the exact reverse of insertion order")
                                .containsExactly("c", "b", "a")
                );
            } else {
                // SortBy.natural documents natural order as typically insertion order, and explicitly as possibly
                // undefined for some datastores, so a fixture that does not declare the stronger guarantee only owes
                // every event exactly once in some total order. The suites that need a guaranteed order sort on a
                // field or use position ordering instead.
                assertAll(
                        () -> assertThat(idsOf(queries().all(SortBy.natural(ASCENDING))))
                                .containsExactlyInAnyOrder("a", "b", "c"),
                        () -> assertThat(idsOf(queries().all(SortBy.natural(DESCENDING))))
                                .containsExactlyInAnyOrder("a", "b", "c")
                );
            }
        }

        @Test
        void sorts_by_a_second_field_when_the_first_ties() {
            OffsetDateTime shared = TIME;
            eventStore().write(STREAM_ID, List.of(
                    eventAt("a", DEFINED, shared),
                    eventAt("b", CHANGED, shared),
                    eventAt("c", ARCHIVED, shared.plusHours(1))));

            SortBy timeThenVersion = SortBy.time(ASCENDING).then(SortBy.streamVersion(DESCENDING));

            assertThat(idsOf(queries().all(timeThenVersion))).containsExactly("b", "a", "c");
        }

        @Test
        void returns_events_unsorted_without_failing() {
            writeThreeAtDistinctTimes();

            // Unsorted means the store picks, so the only contract is that nothing is lost or duplicated.
            assertThat(idsOf(queries().all(SortBy.unsorted()))).containsExactlyInAnyOrder("a", "b", "c");
        }

        @Test
        void composing_a_natural_step_with_a_field_sort_follows_what_the_fixture_declares() {
            // "a" and "b" deliberately share a time. A natural step is only observable as a tiebreaker, so three
            // distinct times would pass for the wrong reason: the field sort alone would already be a total order.
            eventStore().write(STREAM_ID, List.of(
                    eventAt("a", DEFINED, TIME),
                    eventAt("b", CHANGED, TIME),
                    eventAt("c", ARCHIVED, TIME.plusHours(1))));
            SortBy timeThenNatural = SortBy.time(DESCENDING).thenNatural(ASCENDING);

            if (fixture().composesNaturalSortWithFieldSorts()) {
                // Latest time first, then the tied pair in insertion order.
                assertThat(idsOf(queries().all(timeThenNatural))).containsExactly("c", "a", "b");
            } else {
                Throwable thrown = catchThrowable(() -> idsOf(queries().all(timeThenNatural)));

                assertThat(thrown)
                        .describedAs("a store that declares it cannot compose a natural step must reject the compound "
                                + "sort rather than silently ignoring the natural step")
                        .isInstanceOf(IllegalArgumentException.class);
            }
        }

        private void writeThreeAtDistinctTimes() {
            eventStore().write(STREAM_ID, List.of(
                    eventAt("a", DEFINED, TIME),
                    eventAt("b", CHANGED, TIME.plusHours(1)),
                    eventAt("c", ARCHIVED, TIME.plusHours(2))));
        }
    }

    @Nested
    @DisplayName("paging")
    class Paging {

        @Test
        void skips_and_limits_a_sorted_query() {
            writeFive();
            SortBy byTime = SortBy.time(ASCENDING);

            assertAll(
                    () -> assertThat(idsOf(queries().all(0, 2, byTime))).containsExactly("a", "b"),
                    () -> assertThat(idsOf(queries().all(2, 2, byTime))).containsExactly("c", "d"),
                    () -> assertThat(idsOf(queries().all(4, 2, byTime))).containsExactly("e")
            );
        }

        @Test
        void skipping_past_the_end_returns_nothing() {
            writeFive();

            assertThat(idsOf(queries().all(10, 5, SortBy.time(ASCENDING)))).isEmpty();
        }

        @Test
        void pages_a_filtered_query() {
            writeFive();

            List<String> firstTwoChanged = idsOf(queries().query(Filter.type(CHANGED), 0, 2, SortBy.time(ASCENDING)));

            assertThat(firstTwoChanged).containsExactly("b", "c");
        }

        @Test
        void a_limit_larger_than_the_result_returns_everything_that_matched() {
            writeFive();

            assertThat(idsOf(queries().all(0, 100, SortBy.time(ASCENDING))))
                    .containsExactly("a", "b", "c", "d", "e");
        }

        private void writeFive() {
            eventStore().write(STREAM_ID, List.of(
                    eventAt("a", DEFINED, TIME),
                    eventAt("b", CHANGED, TIME.plusHours(1)),
                    eventAt("c", CHANGED, TIME.plusHours(2)),
                    eventAt("d", CHANGED, TIME.plusHours(3)),
                    eventAt("e", ARCHIVED, TIME.plusHours(4))));
        }
    }

    @Nested
    @DisplayName("what a query gives back")
    class WhatAQueryGivesBack {

        @Test
        void carries_the_occurrent_stream_extensions_a_store_stamped_on_write() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED)));

            CloudEvent read = queries().query(Filter.id("a")).findFirst().orElseThrow();

            assertAll(
                    () -> assertThat(read.getExtension(OccurrentCloudEventExtension.STREAM_ID))
                            .hasToString(STREAM_ID),
                    () -> assertThat(read.getExtension(OccurrentCloudEventExtension.STREAM_VERSION))
                            .hasToString("1")
            );
        }

        @Test
        void reads_across_every_stream_rather_than_one() {
            eventStore().write(STREAM_ID, List.of(event("a", DEFINED)));
            eventStore().write(OTHER_STREAM_ID, List.of(event("b", DEFINED)));

            assertThat(idsOf(queries().all())).containsExactlyInAnyOrder("a", "b");
        }
    }
}
