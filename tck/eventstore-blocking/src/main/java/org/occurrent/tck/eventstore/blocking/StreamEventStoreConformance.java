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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.*;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.occurrent.tck.ConformanceEvents;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.condition.Condition.*;
import static org.occurrent.eventstore.api.WriteCondition.streamVersion;
import static org.occurrent.eventstore.api.WriteCondition.streamVersionEq;
import static org.occurrent.tck.ConformanceEvents.*;

/**
 * The stream half of the event-store contract. It covers reading and writing a stream, paging a read, stream
 * existence, {@link WriteResult}, duplicate detection, reading through a {@link StreamReadFilter}, and the whole
 * {@link WriteCondition} family with its exact failure messages.
 * <p>
 * An implementation runs this by extending it and supplying a fixture:
 * <pre>{@code
 * class PostgresqlEventStoreTest extends StreamEventStoreConformance {
 *     @Override
 *     protected EventStoreFixture createFixture() {
 *         return new PostgresqlEventStoreFixture();
 *     }
 * }
 * }</pre>
 * <p>
 * Two things this suite deliberately does not assert, so that neither gets normalised into a vague assertion:
 * <ul>
 *     <li><strong>The cause and message of a {@link DuplicateCloudEventException}.</strong> The MongoDB stores wrap a
 *     driver exception and append its raw text to {@code getDetails()}, which lands in the message, while the
 *     in-memory store detects the duplicate itself and has neither. So the suite asserts the exception type,
 *     {@code getId()}, {@code getSource()} and that nothing was written, which is what a caller can act on, and
 *     leaves {@code getDetails()} to each store.</li>
 *     <li><strong>Read skew.</strong> Whether a concurrent writer can be observed mid-write is an isolation property
 *     of the underlying storage, not of this contract, and the existing per-store tests make materially different
 *     claims about it. Those stay where they are.</li>
 *     <li><strong>That a {@link StreamReadFilter} may not constrain the Occurrent stream extensions.</strong> That is
 *     rejected when the filter is built, by {@code StreamReadFilter.extension(..)} itself, so a store never sees such
 *     a filter and asserting it here would pass against a store that does nothing at all. It belongs to the filter's
 *     own tests in {@code occurrent-eventstore-api-common}, where it already lives.</li>
 * </ul>
 * <p>
 * Two behaviours are left out because the stores that ship with Occurrent disagree about them, and this suite is not
 * the place to pick a winner. Whether an event id already used in another stream counts as a duplicate (the MongoDB
 * stores have a collection-wide unique index on id and source, the in-memory store only looks inside the stream), and
 * whether a stream that only an empty write touched reports as existing (the in-memory store says yes, the MongoDB
 * stores write nothing so they say no). Both are tracked under issue 396 and get asserted here once they are settled.
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
public abstract class StreamEventStoreConformance extends EventStoreConformance {

    private static final String STREAM_ID = "name";
    private static final String OTHER_STREAM_ID = "other-name";

    private static final String DEFINED = "NameDefined";
    private static final String CHANGED = "NameWasChanged";

    @Override
    protected Set<EventStoreCapability> requiredCapabilities() {
        return Set.of(EventStoreCapability.STREAM);
    }

    @Test
    void reads_back_a_single_event_written_to_an_empty_stream() {
        CloudEvent event = event("event-1", DEFINED);

        eventStore().write(STREAM_ID, List.of(event));

        EventStream<CloudEvent> stream = eventStore().read(STREAM_ID);
        assertAll(
                () -> assertThat(stream.id()).isEqualTo(STREAM_ID),
                () -> assertThat(stream.version()).isEqualTo(1L),
                () -> assertThat(idsOf(stream.eventList())).containsExactly("event-1")
        );
    }

    @Test
    void reads_back_several_events_written_in_one_call_in_the_order_they_were_written() {
        eventStore().write(STREAM_ID, List.of(event("event-1", DEFINED), event("event-2", CHANGED)));

        EventStream<CloudEvent> stream = eventStore().read(STREAM_ID);
        assertAll(
                () -> assertThat(stream.version()).isEqualTo(2L),
                () -> assertThat(idsOf(stream.eventList())).containsExactly("event-1", "event-2")
        );
    }

    @Test
    void reads_back_events_written_on_separate_occasions_in_the_order_they_were_written() {
        eventStore().write(STREAM_ID, streamVersionEq(0L), List.of(event("event-1", DEFINED)));
        eventStore().write(STREAM_ID, streamVersionEq(1L), List.of(event("event-2", CHANGED)));
        eventStore().write(STREAM_ID, streamVersionEq(2L), List.of(event("event-3", CHANGED)));

        EventStream<CloudEvent> stream = eventStore().read(STREAM_ID);
        assertAll(
                () -> assertThat(stream.version()).isEqualTo(3L),
                () -> assertThat(idsOf(stream.eventList())).containsExactly("event-1", "event-2", "event-3")
        );
    }

    @Test
    void writing_no_events_leaves_the_stream_untouched() {
        eventStore().write(STREAM_ID, List.of());

        EventStream<CloudEvent> stream = eventStore().read(STREAM_ID);
        assertAll(
                () -> assertThat(stream.isEmpty()).isTrue(),
                () -> assertThat(stream.version()).isEqualTo(0L),
                () -> assertThat(stream.eventList()).isEmpty()
        );
    }

    @Test
    void gives_back_every_cloud_event_attribute_and_the_payload_content_unchanged() {
        CloudEvent written = ConformanceEvents.event("event-1", DEFINED, "a-subject");

        eventStore().write(STREAM_ID, List.of(written));

        CloudEvent read = eventStore().read(STREAM_ID).eventList().get(0);
        assertAll(
                () -> assertThat(read.getId()).isEqualTo(written.getId()),
                () -> assertThat(read.getSource()).isEqualTo(written.getSource()),
                () -> assertThat(read.getType()).isEqualTo(written.getType()),
                () -> assertThat(read.getSubject()).isEqualTo(written.getSubject()),
                () -> assertThat(read.getTime()).isEqualTo(written.getTime()),
                () -> assertThat(read.getDataContentType()).isEqualTo(written.getDataContentType()),
                () -> assertThat(read.getData()).isNotNull(),
                () -> assertThat(withoutWhitespace(requireNonNull(read.getData()).toBytes()))
                        .isEqualTo(withoutWhitespace(ConformanceEvents.dataFor("a-subject")))
        );
    }

    /**
     * The payload with every space, tab and newline removed.
     * <p>
     * A store is not required to hand the payload back byte for byte. The MongoDB stores parse it into a BSON document
     * so that {@code Filter.data(..)} can reach inside it, and re-serialising that document reformats the JSON, which
     * shows up as a space after each colon. What the contract does require is that the data survives, so the
     * comparison ignores formatting.
     * <p>
     * Stripping all whitespace is exact rather than approximate only because {@link ConformanceEvents} builds payloads
     * whose values contain no whitespace of their own. A suite that needs a payload with a space inside a value has to
     * bring a JSON parser instead of reusing this.
     */
    private static String withoutWhitespace(byte[] data) {
        return new String(data, StandardCharsets.UTF_8).replaceAll("\\s", "");
    }

    @Test
    void events_written_to_different_streams_do_not_leak_into_each_other() {
        eventStore().write(STREAM_ID, List.of(event("event-1", DEFINED)));
        eventStore().write(OTHER_STREAM_ID, List.of(event("event-2", DEFINED)));

        assertAll(
                () -> assertThat(idsOf(eventStore().read(STREAM_ID).eventList())).containsExactly("event-1"),
                () -> assertThat(idsOf(eventStore().read(OTHER_STREAM_ID).eventList())).containsExactly("event-2")
        );
    }

    @Test
    void adds_the_stream_id_extension_to_each_written_event() {
        eventStore().write(STREAM_ID, List.of(event("event-1", DEFINED), event("event-2", CHANGED)));

        assertThat(eventStore().read(STREAM_ID).eventList())
                .allSatisfy(event -> assertThat(event.getExtension(OccurrentCloudEventExtension.STREAM_ID))
                        .hasToString(STREAM_ID));
    }

    @Test
    void adds_an_increasing_stream_version_extension_to_each_written_event() {
        eventStore().write(STREAM_ID, List.of(event("event-1", DEFINED), event("event-2", CHANGED)));

        assertThat(eventStore().read(STREAM_ID).eventList())
                .extracting(event -> event.getExtension(OccurrentCloudEventExtension.STREAM_VERSION))
                .extracting(Object::toString)
                .containsExactly("1", "2");
    }

    @Nested
    @DisplayName("paging a stream read")
    class Paging {

        @Test
        void skips_and_limits_the_events_returned_without_changing_the_reported_version() {
            eventStore().write(STREAM_ID, List.of(event("event-1", DEFINED), event("event-2", CHANGED), event("event-3", CHANGED)));

            EventStream<CloudEvent> stream = eventStore().read(STREAM_ID, 1, 1);
            assertAll(
                    () -> assertThat(stream.version()).isEqualTo(3L),
                    () -> assertThat(idsOf(stream.eventList())).containsExactly("event-2")
            );
        }

        @Test
        void returns_the_remaining_events_when_the_limit_exceeds_what_is_left() {
            eventStore().write(STREAM_ID, List.of(event("event-1", DEFINED), event("event-2", CHANGED), event("event-3", CHANGED)));

            EventStream<CloudEvent> stream = eventStore().read(STREAM_ID, 2, 100);

            assertThat(idsOf(stream.eventList())).containsExactly("event-3");
        }

        @Test
        void returns_nothing_when_the_skip_is_past_the_end_of_the_stream() {
            eventStore().write(STREAM_ID, List.of(event("event-1", DEFINED)));

            EventStream<CloudEvent> stream = eventStore().read(STREAM_ID, 10, 10);
            assertAll(
                    () -> assertThat(stream.version()).isEqualTo(1L),
                    () -> assertThat(stream.eventList()).isEmpty()
            );
        }
    }

    @Nested
    @DisplayName("stream existence")
    class StreamExistence {

        @Test
        void a_stream_with_events_exists() {
            eventStore().write(STREAM_ID, List.of(event("event-1", DEFINED)));

            assertThat(eventStore().exists(STREAM_ID)).isTrue();
        }

        @Test
        void a_stream_nothing_was_written_to_does_not_exist() {
            assertThat(eventStore().exists(STREAM_ID)).isFalse();
        }

    }

    @Nested
    @DisplayName("write result")
    class WriteResults {

        @Test
        void reports_the_new_version_when_events_are_written_to_an_empty_stream() {
            WriteResult result = eventStore().write(STREAM_ID, List.of(event("event-1", DEFINED), event("event-2", CHANGED)));

            assertAll(
                    () -> assertThat(result.streamId()).isEqualTo(STREAM_ID),
                    () -> assertThat(result.oldStreamVersion()).isEqualTo(0L),
                    () -> assertThat(result.newStreamVersion()).isEqualTo(2L)
            );
        }

        @Test
        void reports_both_versions_when_events_are_written_to_an_existing_stream() {
            eventStore().write(STREAM_ID, List.of(event("event-1", DEFINED)));

            WriteResult result = eventStore().write(STREAM_ID, List.of(event("event-2", CHANGED), event("event-3", CHANGED)));

            assertAll(
                    () -> assertThat(result.streamId()).isEqualTo(STREAM_ID),
                    () -> assertThat(result.oldStreamVersion()).isEqualTo(1L),
                    () -> assertThat(result.newStreamVersion()).isEqualTo(3L)
            );
        }

        @Test
        void reports_version_zero_when_no_events_are_written_to_an_empty_stream() {
            WriteResult result = eventStore().write(STREAM_ID, List.of());

            assertAll(
                    () -> assertThat(result.streamId()).isEqualTo(STREAM_ID),
                    () -> assertThat(result.oldStreamVersion()).isEqualTo(0L),
                    () -> assertThat(result.newStreamVersion()).isEqualTo(0L)
            );
        }

        @Test
        void reports_the_unchanged_version_when_no_events_are_written_to_an_existing_stream() {
            eventStore().write(STREAM_ID, List.of(event("event-1", DEFINED), event("event-2", CHANGED)));

            WriteResult result = eventStore().write(STREAM_ID, List.of());

            assertAll(
                    () -> assertThat(result.streamId()).isEqualTo(STREAM_ID),
                    () -> assertThat(result.oldStreamVersion()).isEqualTo(2L),
                    () -> assertThat(result.newStreamVersion()).isEqualTo(2L)
            );
        }
    }

    @Nested
    @DisplayName("duplicates")
    class Duplicates {

        @Test
        void writing_a_batch_containing_the_same_event_twice_writes_nothing() {
            CloudEvent event = event("event-1", DEFINED);

            Throwable thrown = catchThrowable(() -> eventStore().write(STREAM_ID, List.of(event, event)));

            assertAll(
                    () -> assertThat(thrown).isExactlyInstanceOf(DuplicateCloudEventException.class),
                    () -> assertThat(duplicateOf(thrown).getId()).isEqualTo("event-1"),
                    () -> assertThat(duplicateOf(thrown).getSource()).isEqualTo(SOURCE),
                    () -> assertThat(eventStore().read(STREAM_ID).eventList()).isEmpty(),
                    () -> assertThat(eventStore().read(STREAM_ID).version()).isEqualTo(0L)
            );
        }

        @Test
        void writing_an_event_that_is_already_persisted_writes_nothing_and_leaves_the_version_alone() {
            eventStore().write(STREAM_ID, List.of(event("event-1", DEFINED)));

            Throwable thrown = catchThrowable(() ->
                    eventStore().write(STREAM_ID, List.of(event("event-2", CHANGED), event("event-1", DEFINED))));

            assertAll(
                    () -> assertThat(thrown).isExactlyInstanceOf(DuplicateCloudEventException.class),
                    () -> assertThat(duplicateOf(thrown).getId()).isEqualTo("event-1"),
                    () -> assertThat(duplicateOf(thrown).getSource()).isEqualTo(SOURCE),
                    () -> assertThat(idsOf(eventStore().read(STREAM_ID).eventList())).containsExactly("event-1"),
                    () -> assertThat(eventStore().read(STREAM_ID).version()).isEqualTo(1L)
            );
        }

        private DuplicateCloudEventException duplicateOf(Throwable thrown) {
            assertThat(thrown).isInstanceOf(DuplicateCloudEventException.class);
            return (DuplicateCloudEventException) thrown;
        }
    }

    @Nested
    @DisplayName("reading through a stream read filter")
    class StreamReadFilters {

        @Test
        void returns_every_event_when_the_filter_matches_all_of_their_types() {
            writeThreeEvents();

            EventStream<CloudEvent> stream = filteredReader().read(STREAM_ID, StreamReadFilter.type(in(DEFINED, CHANGED)));

            assertAll(
                    () -> assertThat(stream.version()).isEqualTo(3L),
                    () -> assertThat(idsOf(stream.eventList())).containsExactly("event-1", "event-2", "event-3")
            );
        }

        @Test
        void returns_only_the_events_of_the_filtered_type_while_still_reporting_the_full_stream_version() {
            writeThreeEvents();

            EventStream<CloudEvent> stream = filteredReader().read(STREAM_ID, StreamReadFilter.type(CHANGED));

            assertAll(
                    () -> assertThat(stream.version()).isEqualTo(3L),
                    () -> assertThat(idsOf(stream.eventList())).containsExactly("event-2", "event-3")
            );
        }

        @Test
        void returns_nothing_when_the_filter_matches_no_event_but_still_reports_the_full_stream_version() {
            writeThreeEvents();

            EventStream<CloudEvent> stream = filteredReader().read(STREAM_ID, StreamReadFilter.type("NoSuchType"));

            assertAll(
                    () -> assertThat(stream.version()).isEqualTo(3L),
                    () -> assertThat(stream.eventList()).isEmpty()
            );
        }

        @Test
        void the_version_from_a_filtered_read_is_the_full_stream_version_so_a_conditional_write_on_it_succeeds() {
            writeThreeEvents();

            EventStream<CloudEvent> stream = filteredReader().read(STREAM_ID, StreamReadFilter.type(CHANGED));
            WriteResult result = eventStore().write(STREAM_ID, streamVersionEq(stream.version()), List.of(event("event-4", CHANGED)));

            assertAll(
                    () -> assertThat(result.oldStreamVersion()).isEqualTo(3L),
                    () -> assertThat(result.newStreamVersion()).isEqualTo(4L)
            );
        }

        @Test
        void pages_a_filtered_read_without_changing_the_reported_version() {
            writeThreeEvents();

            EventStream<CloudEvent> stream = filteredReader().read(STREAM_ID, StreamReadFilter.type(CHANGED), 1, 1);

            assertAll(
                    () -> assertThat(stream.version()).isEqualTo(3L),
                    () -> assertThat(idsOf(stream.eventList())).containsExactly("event-3")
            );
        }

        private void writeThreeEvents() {
            eventStore().write(STREAM_ID, List.of(event("event-1", DEFINED), event("event-2", CHANGED), event("event-3", CHANGED)));
        }
    }

    /**
     * Every case here starts from a stream at version 1, so the actual version in each failure message is 1. The
     * messages are asserted verbatim because all four stores that ship with Occurrent already produce exactly these,
     * which makes the wording part of the contract rather than an implementation detail.
     */
    @Nested
    @DisplayName("write conditions")
    class WriteConditions {

        @Nested
        @DisplayName("any")
        class Any {

            @Test
            void writes_events_whatever_the_stream_version_is() {
                givenAStreamAtVersionOne();

                WriteResult result = eventStore().write(STREAM_ID, WriteCondition.anyStreamVersion(), List.of(event("event-2", CHANGED)));

                assertThat(result.newStreamVersion()).isEqualTo(2L);
            }

            @Test
            void writes_events_to_a_stream_that_does_not_exist_yet() {
                WriteResult result = eventStore().write(STREAM_ID, WriteCondition.anyStreamVersion(), List.of(event("event-1", DEFINED)));

                assertThat(result.newStreamVersion()).isEqualTo(1L);
            }
        }

        @Nested
        @DisplayName("eq")
        class Eq {

            @Test
            void writes_events_when_the_stream_version_is_equal_to_the_expected_version() {
                givenAStreamAtVersionOne();

                thenTheConditionalWriteSucceeds(streamVersionEq(1L));
            }

            @Test
            void fails_when_the_stream_version_is_not_equal_to_the_expected_version() {
                givenAStreamAtVersionOne();

                thenTheConditionalWriteFails(streamVersion(eq(10L)), "to be equal to 10");
            }
        }

        @Nested
        @DisplayName("in")
        class In {

            @Test
            void writes_events_when_the_stream_version_is_one_of_the_expected_versions() {
                givenAStreamAtVersionOne();

                thenTheConditionalWriteSucceeds(streamVersion(in(1L, 10L)));
            }

            @Test
            void fails_when_the_stream_version_is_none_of_the_expected_versions() {
                givenAStreamAtVersionOne();

                thenTheConditionalWriteFails(streamVersion(in(10L, 12L)), "in any of (10,12)");
            }
        }

        @Nested
        @DisplayName("ne")
        class Ne {

            @Test
            void writes_events_when_the_stream_version_differs_from_the_expected_version() {
                givenAStreamAtVersionOne();

                thenTheConditionalWriteSucceeds(streamVersion(ne(20L)));
            }

            @Test
            void fails_when_the_stream_version_equals_the_version_it_must_differ_from() {
                givenAStreamAtVersionOne();

                thenTheConditionalWriteFails(streamVersion(ne(1L)), "to not be equal to 1");
            }
        }

        @Nested
        @DisplayName("lt")
        class Lt {

            @Test
            void writes_events_when_the_stream_version_is_less_than_the_expected_version() {
                givenAStreamAtVersionOne();

                thenTheConditionalWriteSucceeds(streamVersion(lt(10L)));
            }

            @Test
            void fails_when_the_stream_version_is_greater_than_the_expected_version() {
                givenAStreamAtVersionOne();

                thenTheConditionalWriteFails(streamVersion(lt(0L)), "to be less than 0");
            }

            @Test
            void fails_when_the_stream_version_is_equal_to_the_expected_version() {
                givenAStreamAtVersionOne();

                thenTheConditionalWriteFails(streamVersion(lt(1L)), "to be less than 1");
            }
        }

        @Nested
        @DisplayName("gt")
        class Gt {

            @Test
            void writes_events_when_the_stream_version_is_greater_than_the_expected_version() {
                givenAStreamAtVersionOne();

                thenTheConditionalWriteSucceeds(streamVersion(gt(0L)));
            }

            @Test
            void fails_when_the_stream_version_is_less_than_the_expected_version() {
                givenAStreamAtVersionOne();

                thenTheConditionalWriteFails(streamVersion(gt(100L)), "to be greater than 100");
            }

            @Test
            void fails_when_the_stream_version_is_equal_to_the_expected_version() {
                givenAStreamAtVersionOne();

                thenTheConditionalWriteFails(streamVersion(gt(1L)), "to be greater than 1");
            }
        }

        @Nested
        @DisplayName("lte")
        class Lte {

            @Test
            void writes_events_when_the_stream_version_is_less_than_the_expected_version() {
                givenAStreamAtVersionOne();

                thenTheConditionalWriteSucceeds(streamVersion(lte(10L)));
            }

            @Test
            void writes_events_when_the_stream_version_is_equal_to_the_expected_version() {
                givenAStreamAtVersionOne();

                thenTheConditionalWriteSucceeds(streamVersion(lte(1L)));
            }

            @Test
            void fails_when_the_stream_version_is_greater_than_the_expected_version() {
                givenAStreamAtVersionOne();

                thenTheConditionalWriteFails(streamVersion(lte(0L)), "to be less than or equal to 0");
            }
        }

        @Nested
        @DisplayName("gte")
        class Gte {

            @Test
            void writes_events_when_the_stream_version_is_greater_than_the_expected_version() {
                givenAStreamAtVersionOne();

                thenTheConditionalWriteSucceeds(streamVersion(gte(0L)));
            }

            @Test
            void writes_events_when_the_stream_version_is_equal_to_the_expected_version() {
                givenAStreamAtVersionOne();

                // Deliberately gte(1L), the actual version. Every store's own test used gte(0L) here, which is the
                // "greater than" case again, so the equality branch of gte was never exercised anywhere.
                thenTheConditionalWriteSucceeds(streamVersion(gte(1L)));
            }

            @Test
            void fails_when_the_stream_version_is_less_than_the_expected_version() {
                givenAStreamAtVersionOne();

                thenTheConditionalWriteFails(streamVersion(gte(100L)), "to be greater than or equal to 100");
            }
        }

        @Nested
        @DisplayName("and")
        class And {

            @Test
            void writes_events_when_every_condition_is_fulfilled() {
                givenAStreamAtVersionOne();

                thenTheConditionalWriteSucceeds(streamVersion(and(gte(0L), lt(100L), ne(4L))));
            }

            @Test
            void fails_when_one_of_the_conditions_is_not_fulfilled() {
                givenAStreamAtVersionOne();

                thenTheConditionalWriteFails(streamVersion(and(gte(0L), lt(100L), ne(1L))),
                        "to be greater than or equal to 0 and to be less than 100 and to not be equal to 1");
            }
        }

        @Nested
        @DisplayName("or")
        class Or {

            @Test
            void writes_events_when_at_least_one_condition_is_fulfilled() {
                givenAStreamAtVersionOne();

                thenTheConditionalWriteSucceeds(streamVersion(or(gte(0L), lt(1L))));
            }

            @Test
            void fails_when_none_of_the_conditions_is_fulfilled() {
                givenAStreamAtVersionOne();

                thenTheConditionalWriteFails(streamVersion(or(gte(100L), lt(1L))),
                        "to be greater than or equal to 100 or to be less than 1");
            }
        }

        @Nested
        @DisplayName("not")
        class Not {

            @Test
            void writes_events_when_the_negated_condition_is_not_fulfilled() {
                givenAStreamAtVersionOne();

                thenTheConditionalWriteSucceeds(streamVersion(not(eq(20L))));
            }

            @Test
            void fails_when_the_negated_condition_is_fulfilled() {
                givenAStreamAtVersionOne();

                thenTheConditionalWriteFails(streamVersion(not(eq(1L))), "not to be equal to 1");
            }
        }

        private void givenAStreamAtVersionOne() {
            eventStore().write(STREAM_ID, List.of(event("event-1", DEFINED)));
        }

        private void thenTheConditionalWriteSucceeds(WriteCondition condition) {
            WriteResult result = eventStore().write(STREAM_ID, condition, List.of(event("event-2", CHANGED)));

            assertAll(
                    () -> assertThat(result.oldStreamVersion()).isEqualTo(1L),
                    () -> assertThat(result.newStreamVersion()).isEqualTo(2L),
                    () -> assertThat(idsOf(eventStore().read(STREAM_ID).eventList())).containsExactly("event-1", "event-2")
            );
        }

        private void thenTheConditionalWriteFails(WriteCondition condition, String expectedVersionDescription) {
            Throwable thrown = catchThrowable(() ->
                    eventStore().write(STREAM_ID, condition, List.of(event("event-2", CHANGED))));

            assertAll(
                    () -> assertThat(thrown)
                            .isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                            .hasMessage("WriteCondition was not fulfilled. Expected version "
                                    + expectedVersionDescription + " but was 1."),
                    () -> assertThat(idsOf(eventStore().read(STREAM_ID).eventList())).containsExactly("event-1"),
                    () -> assertThat(eventStore().read(STREAM_ID).version()).isEqualTo(1L)
            );
        }
    }
}
