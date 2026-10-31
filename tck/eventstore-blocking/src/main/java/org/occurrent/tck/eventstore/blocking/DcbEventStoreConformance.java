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
import org.occurrent.eventstore.api.DuplicateCloudEventException;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.dcb.*;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.wholeStoreLock;
import static org.occurrent.tck.ConformanceEvents.extension;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.*;

/**
 * The Dynamic Consistency Boundary contract covers which events a criteria selects, what the read options do to the
 * selection, and when an append condition conflicts.
 * <p>
 * Occurrent's stores answer a token-qualified append condition in genuinely different ways, so the fixture
 * declares which through {@link EventStoreFixture#appendConditionModel()} and this suite asserts the outcome that
 * model owes. The in-memory store runs the {@code EXACT_CRITERIA} branches and the MongoDB stores run the
 * {@code MARKERS_THEN_EVENTS} ones. No store in this repository declares {@code TAG_MARKER}, so its branches are
 * written but not run. See {@link DcbAppendConditionModel} for the two questions a declaration answers.
 * <p>
 * What this suite deliberately does <strong>not</strong> assert, each for a reason the contract states:
 * <ul>
 *     <li><strong>A consistency token's {@code version()}.</strong> The store that created a token defines it and it
 *     means nothing outside that store, so the suite never reads it. Two tokens from the same store are compared for
 *     equality where the contract promises they are equal, which is a different thing from interpreting one. The
 *     suite does read {@code criteria()}, because the contract promises a token holds the criteria it was read
 *     with.</li>
 *     <li><strong>{@code lastSequencePosition} as a concurrency boundary.</strong> It is the store's DCB head, not the
 *     highest matched position, and it reports the head even for a read that matched nothing. A store that assigns
 *     positions before its events commit can report a head ahead of what a reader can see, which is exactly why
 *     {@link DcbEventStream#consistencyToken()} exists.</li>
 *     <li><strong>Position contiguity across appends, or any literal position value.</strong> A block is contiguous
 *     within one append and nothing more. A rejected append can reserve and abandon a block, leaving a permanent gap
 *     (ADR 84). Every bound read here is derived from a position handed back by an append, never written as a
 *     literal.</li>
 *     <li><strong>Which storage stream a DCB event landed in.</strong> Placement is a storage choice a store derives
 *     from tags, and {@link org.occurrent.eventstore.api.dcb.DcbStreamIdGenerator} says so. It is not part of the DCB
 *     contract, and a caller reasons in tags rather than stream ids.</li>
 *     <li><strong>How {@code exists} and {@code count} are implemented.</strong> Both document a full read as their
 *     default and invite an implementation to do better, so asserting call counts or efficiency would test the default
 *     rather than the contract.</li>
 * </ul>
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the DCB event store contract")
public abstract class DcbEventStoreConformance extends EventStoreConformance {

    private static final String DEFINED = "NameDefined";
    private static final String CHANGED = "NameChanged";
    private static final String SNAPSHOT = "NameSnapshot";
    private static final String IMPORTED = "NameImported";
    private static final String NAME_1 = "name:1";
    private static final String NAME_2 = "name:2";

    @Override
    protected final Set<EventStoreCapability> requiredCapabilities() {
        return Set.of(EventStoreCapability.DCB);
    }

    @Nested
    @DisplayName("reading by criteria")
    class ReadingByCriteria {

        @Test
        void reads_nothing_and_reports_a_head_of_zero_on_an_empty_store() {
            DcbEventStream read = dcbEventStore().read(DcbCriteria.all());

            assertThat(read.events()).as("A read of an empty store must return no events").isEmpty();
            assertThat(read.lastSequencePosition())
                    .as("lastSequencePosition must be 0, and only 0, while the store holds no DCB events")
                    .isZero();
        }

        @Test
        void reads_every_dcb_event_when_the_criteria_matches_all() {
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            dcbEventStore().append(List.of(taggedEvent(CHANGED, NAME_2)));

            assertThat(typesOf(dcbEventStore().read(DcbCriteria.all()).events()))
                    .as("MatchAll must select every DCB event regardless of its tags")
                    .containsExactly(DEFINED, CHANGED);
        }

        @Test
        void reads_events_whose_type_is_any_of_the_criteria_types() {
            dcbEventStore().append(List.of(
                    taggedEvent(DEFINED, NAME_1),
                    taggedEvent(CHANGED, NAME_1),
                    taggedEvent(SNAPSHOT, NAME_1)));

            assertThat(typesOf(dcbEventStore().read(DcbCriteria.types(DEFINED, SNAPSHOT)).events()))
                    .as("Types are matched as any-of, so an event of either named type must be selected and no other")
                    .containsExactly(DEFINED, SNAPSHOT);
        }

        @Test
        void reads_only_events_carrying_all_of_the_criteria_tags() {
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            dcbEventStore().append(List.of(taggedEvent(CHANGED, NAME_1, NAME_2)));
            dcbEventStore().append(List.of(taggedEvent(SNAPSHOT, NAME_2)));

            assertThat(typesOf(dcbEventStore().read(DcbCriteria.tags(tag(NAME_1), tag(NAME_2))).events()))
                    .as("Tags are matched as all-of, so only the event carrying both tags must be selected")
                    .containsExactly(CHANGED);
        }

        @Test
        void does_not_read_an_event_whose_type_the_criteria_excludes() {
            dcbEventStore().append(List.of(
                    taggedEvent(DEFINED, NAME_1),
                    taggedEvent(SNAPSHOT, NAME_1),
                    taggedEvent(CHANGED, NAME_1)));

            DcbCriterion criteria = DcbCriteria.tags(tag(NAME_1)).excludingTypes(SNAPSHOT);

            assertThat(typesOf(dcbEventStore().read(criteria).events()))
                    .as("Excluded types are matched as none-of, so the excluded event must be removed from a "
                            + "selection its tags would otherwise put it in")
                    .containsExactly(DEFINED, CHANGED);
        }

        @Test
        void reads_an_event_matching_any_alternative_when_alternatives_are_combined() {
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            dcbEventStore().append(List.of(taggedEvent(CHANGED, NAME_2)));
            dcbEventStore().append(List.of(taggedEvent(SNAPSHOT, "name:3")));

            DcbCriteria criteria = DcbCriteria.anyOf(DcbCriteria.tags(tag(NAME_1)), DcbCriteria.tags(tag(NAME_2)));

            assertThat(typesOf(dcbEventStore().read(criteria).events()))
                    .as("Alternatives are OR-ed, so an event matching either alternative must be selected")
                    .containsExactly(DEFINED, CHANGED);
        }

        @Test
        void reads_only_events_matching_both_the_type_and_the_tags_of_one_alternative() {
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_2)));
            dcbEventStore().append(List.of(taggedEvent(CHANGED, NAME_1)));

            DcbCriteria criteria = DcbCriteria.type(DEFINED).tags(tag(NAME_1));

            assertThat(typesOf(dcbEventStore().read(criteria).events()))
                    .as("Types and tags inside one alternative are combined, so an event must satisfy both to be "
                            + "selected, not either")
                    .containsExactly(DEFINED);
        }

        @Test
        void applies_excluded_types_per_alternative_rather_than_to_the_whole_criteria() {
            dcbEventStore().append(List.of(taggedEvent(SNAPSHOT, NAME_1)));
            dcbEventStore().append(List.of(taggedEvent(SNAPSHOT, NAME_2)));

            // Only the first alternative excludes SNAPSHOT. The second says nothing about it, so the event tagged
            // name:2 stays selected.
            DcbCriteria criteria = DcbCriteria.anyOf(
                    DcbCriteria.tags(tag(NAME_1)).excludingTypes(SNAPSHOT),
                    DcbCriteria.tags(tag(NAME_2)));

            assertThat(typesOf(dcbEventStore().read(criteria).events()))
                    .as("An exclusion belongs to the alternative that carries it, so it must not remove an event "
                            + "another alternative selects")
                    .containsExactly(SNAPSHOT);
            assertThat(tagsOn(dcbEventStore().read(criteria).events().getFirst()))
                    .as("The surviving event must be the one tagged by the alternative that does not exclude it")
                    .contains(tag(NAME_2));
        }

        @Test
        void does_not_inspect_the_payload_when_matching_tags() {
            dcbEventStore().append(List.of(
                    taggedEventWithJsonData(DEFINED, "{\"tags\":[\"" + NAME_1 + "\"]}", NAME_2)));

            assertThat(dcbEventStore().read(DcbCriteria.tags(tag(NAME_1))).events())
                    .as("DCB tags are metadata, so a payload that happens to spell a tag must not make an event "
                            + "match a boundary it was not appended to")
                    .isEmpty();
            assertThat(typesOf(dcbEventStore().read(DcbCriteria.tags(tag(NAME_2))).events()))
                    .as("The event's real tag must still select it")
                    .containsExactly(DEFINED);
        }

        @Test
        void reads_an_event_matching_either_a_type_only_or_a_tag_only_alternative() {
            DcbAppendResult first = dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_2)));
            dcbEventStore().append(List.of(taggedEvent(SNAPSHOT, NAME_1)));
            dcbEventStore().append(List.of(taggedEvent(CHANGED, NAME_2)));
            dcbEventStore().append(List.of(taggedEvent(IMPORTED, "name:3")));

            // One alternative constrains only a type, the other only a tag, which is a different shape from ORing
            // two alternatives of the same kind. Combined with a lower bound, so the window and the union have to
            // both be applied rather than one of them quietly winning.
            DcbCriteria criteria = DcbCriteria.anyOf(DcbCriteria.type(CHANGED), DcbCriteria.tags(tag(NAME_1)));

            assertThat(typesOf(dcbEventStore()
                    .read(criteria, DcbReadOptions.afterPosition(first.lastSequencePosition())).events()))
                    .as("Alternatives constraining different things must still be OR-ed, and the position window "
                            + "must still apply to the union")
                    .containsExactly(SNAPSHOT, CHANGED);
        }

        @Test
        void combines_types_tags_and_excluded_types_in_one_alternative() {
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            dcbEventStore().append(List.of(taggedEvent(CHANGED, NAME_1)));
            dcbEventStore().append(List.of(taggedEvent(SNAPSHOT, NAME_1)));
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_2)));

            // All three constraints at once. The exclusion cannot name a listed type, because DcbCriterion rejects
            // that at construction, so alongside a type list an exclusion can only ever name a type the list already
            // leaves out. It is therefore redundant here by construction, and that is exactly what makes it worth
            // asserting: a store treating excludedTypes as anything other than a filter, for example as a second
            // positive list, would return the wrong events for a criterion that should behave as if it were absent.
            DcbCriteria criteria = DcbCriteria.types(DEFINED, CHANGED)
                    .tags(tag(NAME_1))
                    .excludingTypes(SNAPSHOT);

            assertThat(typesOf(dcbEventStore().read(criteria).events()))
                    .as("A criterion carrying types, tags and an exclusion must apply all three, and an exclusion "
                            + "that the type list already rules out must change nothing")
                    .containsExactly(DEFINED, CHANGED);
        }

        @Test
        void reads_a_dcb_event_carrying_no_tags_through_a_type_scoped_criteria() {
            dcbEventStore().append(List.of(untaggedDcbEvent(DEFINED)));

            assertThat(typesOf(dcbEventStore().read(DcbCriteria.type(DEFINED)).events()))
                    .as("A DCB event with an empty tag set is a real event and a type-scoped criteria must reach it")
                    .containsExactly(DEFINED);
            assertThat(dcbEventStore().read(DcbCriteria.tags(tag(NAME_1))).events())
                    .as("An untagged DCB event carries no tags, so no tag-scoped criteria may select it")
                    .isEmpty();
        }

        @Test
        void returns_the_selected_events_in_ascending_position_order() {
            DcbAppendResult first = dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            DcbAppendResult second = dcbEventStore().append(List.of(taggedEvent(CHANGED, NAME_1)));
            DcbAppendResult third = dcbEventStore().append(List.of(taggedEvent(IMPORTED, NAME_1)));

            List<CloudEvent> events = dcbEventStore().read(DcbCriteria.tags(tag(NAME_1))).events();

            assertThat(typesOf(events))
                    .as("A read must list events ascending by DCB position, which here is append order")
                    .containsExactly(DEFINED, CHANGED, IMPORTED);
            assertThat(positionsOf(events))
                    .as("The positions of the returned events must ascend, and must be the positions the appends "
                            + "reported")
                    .containsExactly(first.firstSequencePosition(), second.firstSequencePosition(),
                            third.firstSequencePosition());
        }

        @Test
        void observes_the_store_head_even_when_the_criteria_matches_nothing() {
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            DcbAppendResult last = dcbEventStore().append(List.of(taggedEvent(CHANGED, NAME_1)));

            DcbEventStream matchesNothing = dcbEventStore().read(DcbCriteria.tags(tag("name:absent")));

            assertThat(matchesNothing.events()).as("A criteria matching nothing must select no events").isEmpty();
            assertThat(matchesNothing.lastSequencePosition())
                    .as("lastSequencePosition is the store's DCB head, not the highest matched position, so a read "
                            + "that matched nothing must still report at least the position last appended")
                    .isGreaterThanOrEqualTo(last.lastSequencePosition());
        }

        @Test
        void observes_the_store_head_rather_than_the_highest_position_it_matched() {
            DcbAppendResult matched = dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            DcbAppendResult last = dcbEventStore().append(List.of(taggedEvent(CHANGED, NAME_2)));

            DcbEventStream read = dcbEventStore().read(DcbCriteria.tags(tag(NAME_1)));

            assertThat(typesOf(read.events()))
                    .as("Only the event in the read boundary may come back").containsExactly(DEFINED);
            assertThat(read.lastSequencePosition())
                    .as("A read matching some but not all events must report the store head, which is past the "
                            + "highest position it matched. Reporting the highest match instead is the mistake that "
                            + "makes the head look like a per-query cursor")
                    .isGreaterThan(matched.lastSequencePosition())
                    .isGreaterThanOrEqualTo(last.lastSequencePosition());
        }
    }

    @Nested
    @DisplayName("read options")
    class ReadOptions {

        @Test
        void reads_only_events_after_the_lower_bound() {
            DcbAppendResult first = dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            dcbEventStore().append(List.of(taggedEvent(CHANGED, NAME_1)));

            List<CloudEvent> events = dcbEventStore()
                    .read(DcbCriteria.tags(tag(NAME_1)), DcbReadOptions.afterPosition(first.lastSequencePosition()))
                    .events();

            assertThat(typesOf(events))
                    .as("The lower bound is exclusive, so the event at that position must be left out")
                    .containsExactly(CHANGED);
        }

        @Test
        void reads_only_events_up_to_and_including_the_upper_bound() {
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            DcbAppendResult second = dcbEventStore().append(List.of(taggedEvent(CHANGED, NAME_1)));
            dcbEventStore().append(List.of(taggedEvent(IMPORTED, NAME_1)));

            List<CloudEvent> events = dcbEventStore()
                    .read(DcbCriteria.tags(tag(NAME_1)), DcbReadOptions.upToPosition(second.lastSequencePosition()))
                    .events();

            assertThat(typesOf(events))
                    .as("The upper bound is inclusive, so the event at that position must be included")
                    .containsExactly(DEFINED, CHANGED);
        }

        @Test
        void reads_only_events_inside_a_position_window() {
            DcbAppendResult first = dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            DcbAppendResult second = dcbEventStore().append(List.of(taggedEvent(CHANGED, NAME_1)));
            dcbEventStore().append(List.of(taggedEvent(IMPORTED, NAME_1)));

            List<CloudEvent> events = dcbEventStore()
                    .read(DcbCriteria.tags(tag(NAME_1)),
                            DcbReadOptions.between(first.lastSequencePosition(), second.lastSequencePosition()))
                    .events();

            assertThat(typesOf(events))
                    .as("A window excludes its lower bound and includes its upper bound")
                    .containsExactly(CHANGED);
        }

        @Test
        void a_forward_limit_keeps_the_oldest_matches() {
            appendThreeEventsTaggedName1();

            List<CloudEvent> events = dcbEventStore()
                    .read(DcbCriteria.tags(tag(NAME_1)), DcbReadOptions.fromBeginning().forwards().limit(2))
                    .events();

            assertThat(typesOf(events))
                    .as("Reading forwards selects from the lowest-position end, so a limit of 2 keeps the 2 oldest "
                            + "matches, in ascending order")
                    .containsExactly(DEFINED, CHANGED);
        }

        @Test
        void a_backward_limit_keeps_the_newest_matches_and_still_returns_them_ascending() {
            appendThreeEventsTaggedName1();

            List<CloudEvent> events = dcbEventStore()
                    .read(DcbCriteria.tags(tag(NAME_1)), DcbReadOptions.fromBeginning().backwards().limit(2))
                    .events();

            assertThat(typesOf(events))
                    .as("Reading backwards selects from the highest-position end, but direction never changes the "
                            + "returned order: the 2 newest matches must come back ascending, not reversed")
                    .containsExactly(CHANGED, IMPORTED);
        }

        @Test
        void skip_counts_from_the_selected_end() {
            appendThreeEventsTaggedName1();

            assertThat(typesOf(dcbEventStore()
                    .read(DcbCriteria.tags(tag(NAME_1)), DcbReadOptions.fromBeginning().forwards().skip(1)).events()))
                    .as("Skipping forwards drops the oldest match")
                    .containsExactly(CHANGED, IMPORTED);
            assertThat(typesOf(dcbEventStore()
                    .read(DcbCriteria.tags(tag(NAME_1)), DcbReadOptions.fromBeginning().backwards().skip(1)).events()))
                    .as("Skipping backwards drops the newest match, and the rest still come back ascending")
                    .containsExactly(DEFINED, CHANGED);
        }

        @Test
        void skip_and_limit_compose_from_the_selected_end() {
            appendThreeEventsTaggedName1();

            List<CloudEvent> events = dcbEventStore()
                    .read(DcbCriteria.tags(tag(NAME_1)), DcbReadOptions.fromBeginning().backwards().skip(1).limit(1))
                    .events();

            assertThat(typesOf(events))
                    .as("Skipping the newest match and then keeping 1 must leave the middle match")
                    .containsExactly(CHANGED);
        }

        @Test
        void a_limit_beyond_the_match_count_returns_every_match() {
            appendThreeEventsTaggedName1();

            assertThat(typesOf(dcbEventStore()
                    .read(DcbCriteria.tags(tag(NAME_1)), DcbReadOptions.fromBeginning().limit(10)).events()))
                    .as("A limit is a cap, not a demand, so asking for more matches than exist must return the ones "
                            + "that do")
                    .containsExactly(DEFINED, CHANGED, IMPORTED);
        }

        @Test
        void a_skip_beyond_the_match_count_returns_no_events_and_still_observes_the_head() {
            appendThreeEventsTaggedName1();

            DcbEventStream read = dcbEventStore()
                    .read(DcbCriteria.tags(tag(NAME_1)), DcbReadOptions.fromBeginning().skip(10));

            assertThat(read.events())
                    .as("Skipping past the whole matching set must return nothing rather than wrapping around or "
                            + "failing")
                    .isEmpty();
            assertThat(read.lastSequencePosition())
                    .as("The head is a property of the store, not of the page, so skipping past the matches must not "
                            + "reset it")
                    .isPositive();
        }

        @Test
        void skip_and_limit_apply_inside_a_position_window() {
            DcbAppendResult first = dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            dcbEventStore().append(List.of(taggedEvent(CHANGED, NAME_1)));
            dcbEventStore().append(List.of(taggedEvent(IMPORTED, NAME_1)));
            DcbAppendResult fourth = dcbEventStore().append(List.of(taggedEvent(SNAPSHOT, NAME_1)));

            // The window leaves CHANGED, IMPORTED and SNAPSHOT. Skipping the oldest of those leaves 2, capped to 1.
            DcbReadOptions options = DcbReadOptions
                    .between(first.lastSequencePosition(), fourth.lastSequencePosition())
                    .skip(1)
                    .limit(1);

            assertThat(typesOf(dcbEventStore().read(DcbCriteria.tags(tag(NAME_1)), options).events()))
                    .as("The position window selects the matching set first, and skip and limit then page within it, "
                            + "so neither may reach an event the window excluded")
                    .containsExactly(IMPORTED);

            DcbReadOptions fromTheOtherEnd = DcbReadOptions
                    .between(first.lastSequencePosition(), fourth.lastSequencePosition())
                    .backwards()
                    .skip(1)
                    .limit(1);

            assertThat(typesOf(dcbEventStore().read(DcbCriteria.tags(tag(NAME_1)), fromTheOtherEnd).events()))
                    .as("Paging from the newest end must page within the same window, so the newest match inside it "
                            + "is skipped and the one before it returned, never an event outside the window")
                    .containsExactly(IMPORTED);
        }

        @Test
        void a_backward_limit_beyond_the_match_count_returns_every_match() {
            appendThreeEventsTaggedName1();

            assertThat(typesOf(dcbEventStore()
                    .read(DcbCriteria.tags(tag(NAME_1)), DcbReadOptions.fromBeginning().backwards().limit(10)).events()))
                    .as("Asking for more matches than exist must behave the same from either end")
                    .containsExactly(DEFINED, CHANGED, IMPORTED);
        }

        @Test
        void a_skip_equal_to_the_match_count_returns_no_events() {
            appendThreeEventsTaggedName1();

            assertThat(dcbEventStore()
                    .read(DcbCriteria.tags(tag(NAME_1)), DcbReadOptions.fromBeginning().skip(3)).events())
                    .as("Skipping exactly as many matches as exist must leave nothing, which is the boundary case "
                            + "either side of it is easy to get right by accident")
                    .isEmpty();
        }

        @Test
        void a_bounded_read_still_reports_the_true_store_head() {
            DcbAppendResult first = dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            DcbAppendResult last = dcbEventStore().append(List.of(taggedEvent(CHANGED, NAME_1)));

            DcbEventStream bounded = dcbEventStore()
                    .read(DcbCriteria.tags(tag(NAME_1)), DcbReadOptions.upToPosition(first.lastSequencePosition()));

            assertThat(typesOf(bounded.events()))
                    .as("The window must still bound which events come back").containsExactly(DEFINED);
            assertThat(bounded.lastSequencePosition())
                    .as("The head is the store's, not the window's, so bounding a read to an earlier position must "
                            + "not make the head look older than it is")
                    .isGreaterThanOrEqualTo(last.lastSequencePosition());
        }

        @Test
        void direction_skip_and_limit_do_not_change_the_consistency_token() {
            appendThreeEventsTaggedName1();
            DcbCriteria criteria = DcbCriteria.tags(tag(NAME_1));

            DcbConsistencyToken whole = dcbEventStore().read(criteria).consistencyToken();
            DcbConsistencyToken selected = dcbEventStore()
                    .read(criteria, DcbReadOptions.fromBeginning().backwards().skip(1).limit(1))
                    .consistencyToken();

            // Comparing two tokens from the same store for equality is not the same as interpreting one. The token
            // reflects the whole matching set the read observed, so narrowing what comes back must not narrow the
            // boundary a later append is checked against.
            assertThat(selected)
                    .as("A read that returns one of three matches must observe the same consistency boundary as a "
                            + "read that returns all three")
                    .isEqualTo(whole);
        }

        /**
         * Three matches with a non-matching event between each pair, so the matches do not occupy consecutive
         * positions. A store that paged over stored positions rather than over the matched set would pass every
         * assertion below if the matches were contiguous, which is the whole reason for the interleaving.
         */
        private void appendThreeEventsTaggedName1() {
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            dcbEventStore().append(List.of(taggedEvent(SNAPSHOT, NAME_2)));
            dcbEventStore().append(List.of(taggedEvent(CHANGED, NAME_1)));
            dcbEventStore().append(List.of(taggedEvent(SNAPSHOT, NAME_2)));
            dcbEventStore().append(List.of(taggedEvent(IMPORTED, NAME_1)));
        }
    }

    @Nested
    @DisplayName("exists and count")
    class ExistsAndCount {

        @Test
        void report_nothing_on_an_empty_store() {
            assertThat(dcbEventStore().exists(DcbCriteria.all()))
                    .as("Nothing exists in a store with no DCB events").isFalse();
            assertThat(dcbEventStore().count(DcbCriteria.all()))
                    .as("Nothing is counted in a store with no DCB events").isZero();
        }

        @Test
        void report_whether_the_criteria_matches() {
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1), taggedEvent(CHANGED, NAME_1)));

            assertThat(dcbEventStore().exists(DcbCriteria.tags(tag(NAME_1))))
                    .as("A matched boundary must exist").isTrue();
            assertThat(dcbEventStore().count(DcbCriteria.tags(tag(NAME_1))))
                    .as("count must report how many events the criteria selects").isEqualTo(2);
            assertThat(dcbEventStore().exists(DcbCriteria.tags(tag("name:absent"))))
                    .as("An unmatched boundary must not exist").isFalse();
            assertThat(dcbEventStore().count(DcbCriteria.tags(tag("name:absent"))))
                    .as("An unmatched boundary must count zero").isZero();
        }

        @Test
        void respect_the_position_window() {
            DcbAppendResult first = dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            DcbAppendResult second = dcbEventStore().append(List.of(taggedEvent(CHANGED, NAME_1)));
            DcbCriteria criteria = DcbCriteria.tags(tag(NAME_1));

            assertThat(dcbEventStore().count(criteria, DcbReadOptions.afterPosition(first.lastSequencePosition())))
                    .as("count must only count inside the position window").isEqualTo(1);
            assertThat(dcbEventStore().count(criteria, DcbReadOptions.upToPosition(first.lastSequencePosition())))
                    .as("count must respect an upper bound as well as a lower one").isEqualTo(1);
            assertThat(dcbEventStore().count(criteria,
                    DcbReadOptions.between(first.lastSequencePosition(), second.lastSequencePosition())))
                    .as("count must respect a window bounded at both ends").isEqualTo(1);
            assertThat(dcbEventStore().exists(criteria, DcbReadOptions.upToPosition(first.lastSequencePosition())))
                    .as("exists must answer for the position window it was given").isTrue();
            assertThat(dcbEventStore().exists(criteria, DcbReadOptions.afterPosition(second.lastSequencePosition())))
                    .as("A window holding no match must make exists false, which is the half a store answering "
                            + "exists without applying the window would get wrong")
                    .isFalse();
        }

        @Test
        void ignore_direction_skip_and_limit() {
            dcbEventStore().append(List.of(
                    taggedEvent(DEFINED, NAME_1),
                    taggedEvent(CHANGED, NAME_1),
                    taggedEvent(IMPORTED, NAME_1)));
            DcbCriteria criteria = DcbCriteria.tags(tag(NAME_1));
            DcbReadOptions narrowed = DcbReadOptions.fromBeginning().backwards().skip(2).limit(1);

            // Paging options are ignored while the position window is not, so the two have to be exercised together.
            // A store that discarded the whole DcbReadOptions rather than only its paging part would pass a test
            // that only ever asked with fromBeginning().
            long headAfterTwo = dcbEventStore().read(criteria, DcbReadOptions.fromBeginning().limit(2))
                    .events().stream().mapToLong(OccurrentCloudEventExtension::getPosition).max().orElseThrow();
            assertThat(dcbEventStore().count(criteria, DcbReadOptions.upToPosition(headAfterTwo).backwards().skip(2).limit(1)))
                    .as("count must ignore direction, skip and limit while still applying the position window")
                    .isEqualTo(2);

            assertThat(dcbEventStore().count(criteria, narrowed))
                    .as("count is documented to ignore direction, skip and limit, so it must count the whole "
                            + "matching set inside the position window rather than the 1 event a read would return")
                    .isEqualTo(3);
            assertThat(dcbEventStore().exists(criteria, narrowed))
                    .as("exists is documented to ignore direction, skip and limit").isTrue();
        }

        @Test
        void a_skip_past_the_matching_set_does_not_make_the_boundary_stop_existing() {
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            DcbCriteria criteria = DcbCriteria.tags(tag(NAME_1));

            // The trap this pins: an implementation that answers exists() by running the read it would run for a
            // caller sees an empty result here and reports false, which would make an append condition built on a
            // skipped read silently unguarded.
            assertThat(dcbEventStore().exists(criteria, DcbReadOptions.fromBeginning().skip(5)))
                    .as("A skip that would return no events must not change the answer to exists")
                    .isTrue();
        }
    }

    @Nested
    @DisplayName("append results")
    class AppendResults {

        @Test
        void an_append_is_assigned_one_contiguous_block_of_positions() {
            DcbAppendResult result = dcbEventStore().append(List.of(
                    taggedEvent(DEFINED, NAME_1),
                    taggedEvent(CHANGED, NAME_1),
                    taggedEvent(IMPORTED, NAME_1)));

            assertThat(result.eventCount()).as("Every appended event must be counted").isEqualTo(3);
            assertThat(result.lastSequencePosition() - result.firstSequencePosition() + 1)
                    .as("A single append is assigned a contiguous block, so the block must be exactly as wide as the "
                            + "number of events appended")
                    .isEqualTo(3);
        }

        @Test
        void positions_strictly_increase_across_separate_appends() {
            DcbAppendResult first = dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            DcbAppendResult second = dcbEventStore().append(List.of(taggedEvent(CHANGED, NAME_2)));

            // Strictly greater, never contiguous: nothing here says second.first == first.last + 1, because a store
            // that reserves position blocks outside the write transaction can leave a permanent gap between them.
            assertThat(second.firstSequencePosition())
                    .as("A later append must be assigned strictly higher positions than an earlier one, across "
                            + "different consistency boundaries as well as the same one")
                    .isGreaterThan(first.lastSequencePosition());
        }

        @Test
        void an_appended_event_reads_back_inside_the_block_the_append_reported() {
            DcbAppendResult result = dcbEventStore().append(List.of(
                    taggedEvent(DEFINED, NAME_1),
                    taggedEvent(CHANGED, NAME_1)));

            assertThat(positionsOf(dcbEventStore().read(DcbCriteria.tags(tag(NAME_1))).events()))
                    .as("The positions the events carry must be the block the append reported, so a caller can act "
                            + "on the result without reading the events back")
                    .allSatisfy(position -> assertThat(position)
                            .isBetween(result.firstSequencePosition(), result.lastSequencePosition()));
        }

        @Test
        void an_appended_event_reads_back_with_the_tags_it_was_appended_with() {
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1, NAME_2)));

            CloudEvent read = dcbEventStore().read(DcbCriteria.tags(tag(NAME_1))).events().getFirst();

            assertThat(tagsOn(read))
                    .as("A store may add tags of its own to place an event, but it must not lose the tags the caller "
                            + "appended, since a later read is selected by exactly those")
                    .containsAll(tagsOf(NAME_1, NAME_2));
        }

        @Test
        void rejects_an_event_whose_id_and_source_already_exist() {
            CloudEvent event = taggedEventWithId("the-same-id", DEFINED, NAME_1);
            dcbEventStore().append(List.of(event));

            assertThatThrownBy(() -> dcbEventStore().append(List.of(event)))
                    .as("CloudEvents requires id and source to identify an event uniquely, and a DCB append is held "
                            + "to that like any other write")
                    .isExactlyInstanceOf(DuplicateCloudEventException.class);
        }

        @Test
        void the_store_head_reaches_the_position_the_append_reported() {
            DcbAppendResult result = dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));

            assertThat(dcbEventStore().read(DcbCriteria.all()).lastSequencePosition())
                    .as("A read after an append must observe a head at least as high as the position the append "
                            + "reported, since the head is a monotonic global cursor over the same sequence")
                    .isGreaterThanOrEqualTo(result.lastSequencePosition());
        }

        @Test
        void every_event_of_one_append_has_the_same_append_id_the_append_returned() {
            DcbAppendResult result = dcbEventStore().append(List.of(
                    taggedEvent(DEFINED, NAME_1),
                    taggedEvent(CHANGED, NAME_1)));

            assertThat(result.appendId())
                    .as("A successful DCB append always persists at least one event, so it must report an append id")
                    .isPresent();
            String appendId = result.appendId().get().toString();
            assertThat(dcbEventStore().read(DcbCriteria.tags(tag(NAME_1))).events())
                    .extracting(event -> extension(event, OccurrentCloudEventExtension.APPEND_ID))
                    .as("Every event the append persisted must have the exact id the append reported")
                    .containsExactly(appendId, appendId);
        }

        @Test
        void distinct_appends_carry_distinct_append_ids() {
            DcbAppendResult first = dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            DcbAppendResult second = dcbEventStore().append(List.of(taggedEvent(CHANGED, NAME_2)));

            assertThat(first.appendId())
                    .as("Two separate appends must not be assigned the same append id")
                    .isNotEqualTo(second.appendId());
        }
    }

    @Nested
    @DisplayName("append conditions")
    class AppendConditions {

        @Test
        void a_condition_with_no_token_succeeds_when_nothing_matches() {
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_2)));

            DcbAppendResult result = dcbEventStore().append(
                    List.of(taggedEvent(CHANGED, NAME_1)),
                    failIfEventsMatch(DcbCriteria.tags(tag(NAME_1))));

            assertThat(result.eventCount())
                    .as("A guard on an empty boundary must not be tripped by an event in a different boundary")
                    .isEqualTo(1);
        }

        @Test
        void a_condition_with_no_token_conflicts_when_something_matches() {
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));

            assertThatThrownBy(() -> dcbEventStore().append(
                    List.of(taggedEvent(CHANGED, NAME_1)),
                    failIfEventsMatch(DcbCriteria.tags(tag(NAME_1)))))
                    .as("Without a token the condition means \"nothing matching may exist\", so an existing match "
                            + "must reject the append")
                    .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        }

        @Test
        void a_token_qualified_condition_succeeds_when_nothing_matched_since_the_read() {
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            DcbCriteria criteria = DcbCriteria.tags(tag(NAME_1));
            DcbConsistencyToken token = dcbEventStore().read(criteria).consistencyToken();

            DcbAppendResult result = dcbEventStore().append(
                    List.of(taggedEvent(CHANGED, NAME_1)), failIfEventsMatch(token));

            assertThat(result.eventCount())
                    .as("A token narrows the condition to what was committed after the read, so the event the read "
                            + "itself saw must not reject the append")
                    .isEqualTo(1);
        }

        @Test
        void a_token_qualified_condition_conflicts_on_a_match_committed_after_the_read() {
            DcbCriteria criteria = DcbCriteria.tags(tag(NAME_1));
            DcbConsistencyToken token = dcbEventStore().read(criteria).consistencyToken();

            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));

            assertThatThrownBy(() -> dcbEventStore().append(
                    List.of(taggedEvent(CHANGED, NAME_1)), failIfEventsMatch(token)))
                    .as("An event matching the boundary committed after the read must reject the append")
                    .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        }

        @Test
        void a_token_qualified_condition_ignores_an_append_to_a_different_boundary() {
            DcbCriteria criteria = DcbCriteria.tags(tag(NAME_1));
            DcbConsistencyToken token = dcbEventStore().read(criteria).consistencyToken();

            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_2)));

            DcbAppendResult result = dcbEventStore().append(
                    List.of(taggedEvent(CHANGED, NAME_1)), failIfEventsMatch(token));

            assertThat(result.eventCount())
                    .as("A consistency boundary is scoped by its criteria, so a concurrent append to an unrelated "
                            + "boundary must not reject this one. This is the whole point of DCB over a stream lock")
                    .isEqualTo(1);
        }

        @Test
        void a_token_from_a_read_that_matched_nothing_still_guards_the_boundary() {
            DcbCriteria criteria = DcbCriteria.tags(tag(NAME_1));
            DcbConsistencyToken token = dcbEventStore().read(criteria).consistencyToken();

            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));

            assertThatThrownBy(() -> dcbEventStore().append(
                    List.of(taggedEvent(CHANGED, NAME_1)), failIfEventsMatch(token)))
                    .as("Reading an empty boundary is the ordinary first step of a decide-then-append, so its token "
                            + "must guard just as well as one from a read that matched")
                    .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        }

        @Test
        void a_token_holds_the_criteria_of_its_read_and_conflicts_on_a_match_of_any_alternative_committed_after_the_read() {
            DcbCriteria criteria = DcbCriteria.anyOf(DcbCriteria.tags(tag(NAME_1)), DcbCriteria.tags(tag(NAME_2)));
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            DcbConsistencyToken token = dcbEventStore().read(criteria).consistencyToken();

            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_2)));

            assertAll(
                    () -> assertThat(token.criteria())
                            .as("The token holds the criteria the read was given, both alternatives")
                            .isEqualTo(criteria),
                    () -> assertThatThrownBy(() -> dcbEventStore().append(
                            List.of(taggedEvent(CHANGED, NAME_1)), failIfEventsMatch(token)))
                            .as("An event of the second alternative committed after the read must reject the append")
                            .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class));
        }

        @Test
        void a_token_holds_the_criteria_of_its_read_whichever_read_options_the_read_was_given() {
            DcbCriteria criteria = DcbCriteria.anyOf(DcbCriteria.tags(tag(NAME_1)), DcbCriteria.tags(tag(NAME_2)));
            DcbAppendResult first = dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            dcbEventStore().append(List.of(taggedEvent(CHANGED, NAME_2)));
            dcbEventStore().append(List.of(taggedEvent(IMPORTED, NAME_1)));

            assertAll(
                    () -> assertThat(dcbEventStore()
                            .read(criteria, DcbReadOptions.afterPosition(first.lastSequencePosition()))
                            .consistencyToken().criteria())
                            .as("A position-bound read must hold the criteria it was given, both alternatives, "
                                    + "because a caller that read after a position builds its condition from the "
                                    + "token alone")
                            .isEqualTo(criteria),
                    () -> assertThat(dcbEventStore()
                            .read(criteria, DcbReadOptions.fromBeginning().limit(1))
                            .consistencyToken().criteria())
                            .as("A limited read must hold the criteria it was given, both alternatives")
                            .isEqualTo(criteria),
                    () -> assertThat(dcbEventStore()
                            .read(criteria, DcbReadOptions.fromBeginning().skip(1))
                            .consistencyToken().criteria())
                            .as("A read that skips matches must hold the criteria it was given, both alternatives")
                            .isEqualTo(criteria),
                    () -> assertThat(dcbEventStore()
                            .read(criteria, DcbReadOptions.fromBeginning().backwards())
                            .consistencyToken().criteria())
                            .as("A backwards read must hold the criteria it was given, both alternatives")
                            .isEqualTo(criteria));
        }

        @Test
        void a_token_qualified_condition_conflicts_on_a_non_excluded_type_committed_after_the_read() {
            DcbCriterion criteria = DcbCriteria.tags(tag(NAME_1)).excludingTypes(SNAPSHOT);
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            DcbConsistencyToken token = dcbEventStore().read(criteria).consistencyToken();

            dcbEventStore().append(List.of(taggedEvent(CHANGED, NAME_1)));

            assertThatThrownBy(() -> dcbEventStore().append(
                    List.of(taggedEvent(IMPORTED, NAME_1)), failIfEventsMatch(token)))
                    .as("An exclusion narrows the boundary, it does not disarm it: an event of a type the criteria "
                            + "does not exclude must still reject the append")
                    .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        }

        @Test
        void an_unconditional_append_is_visible_to_a_later_token_qualified_condition() {
            DcbCriteria criteria = DcbCriteria.tags(tag(NAME_1));
            DcbConsistencyToken token = dcbEventStore().read(criteria).consistencyToken();

            // Appended with no condition of its own, which is the case a store answering conditions from per-boundary
            // bookkeeping could plausibly miss: nothing asked it to record anything.
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));

            assertThatThrownBy(() -> dcbEventStore().append(
                    List.of(taggedEvent(CHANGED, NAME_1)), failIfEventsMatch(token)))
                    .as("An unconditional append is still an append, so a later condition on the boundary it wrote "
                            + "into must see it")
                    .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        }

        @Test
        void the_conflict_carries_the_condition_that_was_evaluated_and_the_shared_message() {
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            DcbAppendCondition condition = failIfEventsMatch(DcbCriteria.tags(tag(NAME_1)));

            assertThatThrownBy(() -> dcbEventStore().append(List.of(taggedEvent(CHANGED, NAME_1)), condition))
                    .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class)
                    // The message is a fixed literal on the exception's standard constructor with nothing
                    // interpolated, so it is cross-store law rather than something each store words for itself.
                    .hasMessage("Append condition was not fulfilled.")
                    .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories
                            .type(DcbAppendConditionNotFulfilledException.class))
                    .satisfies(thrown -> assertThat(thrown.appendCondition())
                            .as("The exception must hand back the condition it evaluated, so a caller can retry "
                                    + "against the same boundary without having kept it")
                            .isEqualTo(condition));
        }

        @Test
        void a_rejected_append_writes_none_of_its_events() {
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            DcbCriteria criteria = DcbCriteria.tags(tag(NAME_1));

            assertThatThrownBy(() -> dcbEventStore().append(
                    List.of(taggedEvent(CHANGED, NAME_1), taggedEvent(IMPORTED, NAME_1)),
                    failIfEventsMatch(criteria)))
                    .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);

            assertThat(typesOf(dcbEventStore().read(criteria).events()))
                    .as("A rejected append must be all or nothing, so neither of its events may be visible")
                    .containsExactly(DEFINED);
        }

        @Test
        void a_condition_with_other_criteria_than_the_token_was_read_with_is_refused() {
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            DcbConsistencyToken tokenReadWithName1 = dcbEventStore().read(DcbCriteria.tags(tag(NAME_1))).consistencyToken();

            assertThatThrownBy(() -> dcbEventStore().append(
                    List.of(taggedEvent(CHANGED, NAME_2)),
                    failIfEventsMatch(DcbCriteria.tags(tag(NAME_2)), tokenReadWithName1)))
                    .as("A token belongs to the criteria it was read with, so building a condition on other criteria "
                            + "throws before append is called")
                    .isExactlyInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void a_whole_store_lock_with_a_token_read_with_other_criteria_than_all_is_refused() {
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            DcbConsistencyToken tokenReadWithName1 = dcbEventStore().read(DcbCriteria.tags(tag(NAME_1))).consistencyToken();

            assertThatThrownBy(() -> dcbEventStore().append(
                    List.of(taggedEvent(CHANGED, NAME_2)), wholeStoreLock(tokenReadWithName1)))
                    .as("A token belongs to the criteria it was read with, so building a whole-store lock from a "
                            + "token read with a tag instead of all() throws before append is called")
                    .isExactlyInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void a_condition_taking_its_criteria_from_the_token_succeeds_when_nothing_matched_since_the_read() {
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            DcbConsistencyToken token = dcbEventStore().read(DcbCriteria.tags(tag(NAME_1))).consistencyToken();

            DcbAppendResult result = dcbEventStore().append(
                    List.of(taggedEvent(CHANGED, NAME_1)), failIfEventsMatch(token));

            assertThat(result.eventCount())
                    .as("The one-argument form uses the criteria the token was read with, so the control for the "
                            + "refusals above must be accepted when nothing matching was committed after the read")
                    .isEqualTo(1);
        }

        @Test
        void a_condition_on_the_alternatives_of_the_token_in_another_order_is_the_same_boundary() {
            assertConditionOnSameAlternativesIsTheSameBoundary(
                    DcbCriteria.anyOf(DcbCriteria.tags(tag(NAME_1)), DcbCriteria.tags(tag(NAME_2))),
                    DcbCriteria.anyOf(DcbCriteria.tags(tag(NAME_2)), DcbCriteria.tags(tag(NAME_1))));
        }

        @Test
        void a_condition_on_the_alternatives_of_the_token_without_its_duplicate_is_the_same_boundary() {
            assertConditionOnSameAlternativesIsTheSameBoundary(
                    DcbCriteria.anyOf(DcbCriteria.tags(tag(NAME_1)), DcbCriteria.tags(tag(NAME_1)), DcbCriteria.tags(tag(NAME_2))),
                    DcbCriteria.anyOf(DcbCriteria.tags(tag(NAME_1)), DcbCriteria.tags(tag(NAME_2))));
        }

        private void assertConditionOnSameAlternativesIsTheSameBoundary(DcbCriteria readWith, DcbCriteria conditionOn) {
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1), taggedEvent(DEFINED, NAME_2)));

            DcbConsistencyToken untouched = dcbEventStore().read(readWith).consistencyToken();
            assertThatCode(() -> dcbEventStore().append(
                    List.of(taggedEvent(SNAPSHOT, "unrelated")), failIfEventsMatch(conditionOn, untouched)))
                    .as("A condition on the same alternatives as the token was read with, only reordered or without "
                            + "a duplicate, must be accepted when nothing matched since the read")
                    .doesNotThrowAnyException();

            DcbConsistencyToken firstMatched = dcbEventStore().read(readWith).consistencyToken();
            dcbEventStore().append(List.of(taggedEvent(CHANGED, NAME_1)));
            assertThatThrownBy(() -> dcbEventStore().append(
                    List.of(taggedEvent(SNAPSHOT, "unrelated")), failIfEventsMatch(conditionOn, firstMatched)))
                    .as("An event of the first alternative committed after the read must refuse a condition on the "
                            + "same alternatives, so the condition guards the boundary the token was read with")
                    .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);

            DcbConsistencyToken secondMatched = dcbEventStore().read(readWith).consistencyToken();
            dcbEventStore().append(List.of(taggedEvent(CHANGED, NAME_2)));
            assertThatThrownBy(() -> dcbEventStore().append(
                    List.of(taggedEvent(SNAPSHOT, "unrelated")), failIfEventsMatch(conditionOn, secondMatched)))
                    .as("An event of the second alternative committed after the read must refuse a condition on the "
                            + "same alternatives, so the condition guards the boundary the token was read with")
                    .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        }

        @Test
        void a_condition_on_a_type_and_a_tag_ignores_an_event_of_that_type_with_another_tag_committed_after_the_read() {
            String type = "TypeAndTagAccepted";
            String tagA = "type-and-tag-accepted:a";
            String tagB = "type-and-tag-accepted:b";
            DcbCriteria criteria = DcbCriteria.type(type).tags(tag(tagA));
            DcbConsistencyToken token = dcbEventStore().read(criteria).consistencyToken();

            dcbEventStore().append(List.of(taggedEvent(type, tagB)));

            assertThatCode(() -> dcbEventStore().append(
                    List.of(taggedEvent(type, tagA)), failIfEventsMatch(token)))
                    .as("The event of the same type tagged only with b does not match type and tag a, so it must not "
                            + "reject an append conditioned on them. Sharing a type with the criteria is not "
                            + "sharing the boundary")
                    .doesNotThrowAnyException();

            assertThat(dcbEventStore().read(criteria).events().size())
                    .as("The accepted append must have written its event, and the event tagged b is not selected")
                    .isEqualTo(1);
        }

        @Test
        void a_condition_on_a_type_and_a_tag_conflicts_on_a_match_committed_after_the_read() {
            String type = "TypeAndTagControl";
            String tagA = "type-and-tag-control:a";
            DcbCriteria criteria = DcbCriteria.type(type).tags(tag(tagA));
            DcbConsistencyToken token = dcbEventStore().read(criteria).consistencyToken();

            dcbEventStore().append(List.of(taggedEvent(type, tagA)));

            assertThatThrownBy(() -> dcbEventStore().append(
                    List.of(taggedEvent(type, tagA)), failIfEventsMatch(token)))
                    .as("The control of the accepted case: the same type and the tag the criteria hold do match, "
                            + "so a store that checks no marker for this item at all would accept this append")
                    .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        }

        @Test
        void a_condition_on_a_type_and_a_tag_does_not_invalidate_a_token_read_on_the_type_alone_when_it_appends_an_event_of_another_type() {
            String heldType = "TypeAndTagHeld";
            String otherType = "TypeAndTagOther";
            String tagA = "type-and-tag-held:a";
            String tagB = "type-and-tag-held:b";
            DcbConsistencyToken held = dcbEventStore().read(DcbCriteria.type(heldType)).consistencyToken();
            DcbCriteria typeAndTag = DcbCriteria.type(heldType).tags(tag(tagA));
            DcbConsistencyToken tokenOnTypeAndTag = dcbEventStore().read(typeAndTag).consistencyToken();

            assertThatCode(() -> dcbEventStore().append(
                    List.of(taggedEvent(otherType, tagA)), failIfEventsMatch(tokenOnTypeAndTag)))
                    .as("Nothing of the held type exists, so the condition on that type and the tag a is fulfilled")
                    .doesNotThrowAnyException();
            assertThat(dcbEventStore().read(DcbCriteria.type(heldType)).events())
                    .as("The append above wrote an event of another type, so still no event of the held type exists "
                            + "for the held token to be stale against")
                    .isEmpty();

            assertThatCode(() -> dcbEventStore().append(
                    List.of(taggedEvent(heldType, tagB)), failIfEventsMatch(held)))
                    .as("An append that was conditioned on a type and a tag guards that boundary and nothing wider. "
                            + "It committed no event of the held type, so the token read on the type alone is "
                            + "still current and must not be refused")
                    .doesNotThrowAnyException();

            assertThat(dcbEventStore().read(DcbCriteria.type(heldType)).events().size())
                    .as("The accepted append must have written its event of the held type")
                    .isEqualTo(1);
        }

        @Test
        void a_condition_on_a_type_and_a_tag_that_is_refused_appends_nothing() {
            String type = "TypeAndTagGuard";
            String tagA = "type-and-tag-guard:a";
            String tagB = "type-and-tag-guard:b";
            DcbCriteria criteria = DcbCriteria.type(type).tags(tag(tagA));
            DcbConsistencyToken token = dcbEventStore().read(criteria).consistencyToken();

            dcbEventStore().append(List.of(taggedEvent(type, tagB)));
            dcbEventStore().append(List.of(taggedEvent(type, tagA)));
            DcbEventStream before = dcbEventStore().read(criteria);

            assertThatThrownBy(() -> dcbEventStore().append(
                    List.of(taggedEvent(type, tagA)), failIfEventsMatch(token)))
                    .as("An event matching the type and the tag was committed after the read, so the append is refused "
                            + "and the checks below can show that a refused append writes nothing")
                    .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);

            DcbEventStream after = dcbEventStore().read(criteria);
            assertAll(
                    () -> assertThat(after.events().size())
                            .as("The refused append must not have written its event")
                            .isEqualTo(before.events().size()),
                    () -> assertThat(positionsOf(after.events()))
                            .as("The refused append must not have added an event at a position the criteria select")
                            .isEqualTo(positionsOf(before.events())));
        }

        @Test
        void a_condition_on_a_tagged_item_and_an_untagged_item_conflicts_on_an_event_of_the_untagged_type_with_an_unrelated_tag_and_nothing_is_appended() {
            String taggedType = "MixedItemsConflictTagged";
            String untaggedType = "MixedItemsConflictUntagged";
            String tagA = "mixed-items-conflict:a";
            String unrelatedTag = "mixed-items-conflict:z";
            DcbCriteria criteria = DcbCriteria.anyOf(DcbCriteria.type(taggedType).tags(tag(tagA)), DcbCriteria.type(untaggedType));
            DcbConsistencyToken token = dcbEventStore().read(criteria).consistencyToken();

            dcbEventStore().append(List.of(taggedEvent(untaggedType, unrelatedTag)));
            DcbEventStream before = dcbEventStore().read(criteria);

            assertThatThrownBy(() -> dcbEventStore().append(
                    List.of(taggedEvent(taggedType, tagA)), failIfEventsMatch(token)))
                    .as("The untagged item selects every event of its type whatever its tags, so the event of that "
                            + "type committed after the read matches the criteria even though it carries none of the "
                            + "tags of the tagged item. A store that gives the untagged item no boundary because "
                            + "another item has tags would accept this append")
                    .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);

            DcbEventStream after = dcbEventStore().read(criteria);
            assertAll(
                    () -> assertThat(after.events().size())
                            .as("The refused append must not have written its event")
                            .isEqualTo(before.events().size()),
                    () -> assertThat(positionsOf(after.events()))
                            .as("The refused append must not have added an event at a position the criteria select")
                            .isEqualTo(positionsOf(before.events())));
        }

        @Test
        void a_condition_on_a_tagged_item_and_an_untagged_item_ignores_an_event_of_the_tagged_type_with_another_tag_committed_after_the_read() {
            String taggedType = "MixedItemsControlTagged";
            String untaggedType = "MixedItemsControlUntagged";
            String tagA = "mixed-items-control:a";
            String tagB = "mixed-items-control:b";
            DcbCriteria criteria = DcbCriteria.anyOf(DcbCriteria.type(taggedType).tags(tag(tagA)), DcbCriteria.type(untaggedType));
            DcbConsistencyToken token = dcbEventStore().read(criteria).consistencyToken();

            dcbEventStore().append(List.of(taggedEvent(taggedType, tagB)));

            assertThatCode(() -> dcbEventStore().append(
                    List.of(taggedEvent(untaggedType, tagA)), failIfEventsMatch(token)))
                    .as("The event of the tagged type carries only tag b, so it matches neither the tagged item, "
                            + "which needs tag a, nor the untagged item, which needs the other type. The control of "
                            + "the conflict case: a store that refuses every event of a type the criteria name "
                            + "would reject this append")
                    .doesNotThrowAnyException();

            assertThat(dcbEventStore().read(criteria).events().size())
                    .as("The accepted append must have written its event, and the event of the tagged type with tag b is not selected")
                    .isEqualTo(1);
        }

        @Test
        void a_condition_on_a_type_and_two_tags_ignores_an_event_of_that_type_carrying_only_one_of_the_tags_unless_the_model_compares_only_markers() {
            String type = "TwoTagsAccepted";
            String course = "two-tags-accepted-course:c1";
            String student = "two-tags-accepted-student:s1";
            String otherStudent = "two-tags-accepted-student:s2";
            DcbCriteria criteria = DcbCriteria.type(type).tags(tag(course), tag(student));
            DcbConsistencyToken token = dcbEventStore().read(criteria).consistencyToken();

            dcbEventStore().append(List.of(taggedEvent(type, course, otherStudent)));

            List<CloudEvent> events = List.of(taggedEvent(type, course, student));
            DcbAppendCondition condition = failIfEventsMatch(token);
            if (expectedToConflictOnANonMatchingEventSharingATag()) {
                Throwable thrown = catchThrowable(() -> dcbEventStore().append(events, condition));
                int selected = dcbEventStore().read(criteria).events().size();
                if (thrown == null) {
                    assertThat(selected)
                            .as("A store that compares the marker of each tag can accept the append, and then it must "
                                    + "have written its event. The event with the other student is not selected")
                            .isEqualTo(1);
                } else {
                    assertThat(thrown)
                            .as("A store that compares the markers as a sum can refuse the append, because the marker "
                                    + "of the course tag moved. It must refuse with the append condition exception")
                            .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
                    assertThat(selected)
                            .as("The refused append must not have written its event, and the event with the other student is not selected")
                            .isZero();
                }
            } else {
                assertThatCode(() -> dcbEventStore().append(events, condition))
                        .as("The event committed after the read carries the course tag but not the student tag, so it "
                                + "does not match a criterion that needs both. Sharing one of two tags with the criteria "
                                + "is not sharing the boundary")
                        .doesNotThrowAnyException();

                assertThat(dcbEventStore().read(criteria).events().size())
                        .as("The accepted append must have written its event, and the event with the other student is not selected")
                        .isEqualTo(1);
            }
        }

        @Test
        void a_condition_on_a_type_and_two_tags_conflicts_on_an_event_of_that_type_carrying_both_tags_committed_after_the_read() {
            String type = "TwoTagsControl";
            String course = "two-tags-control-course:c1";
            String student = "two-tags-control-student:s1";
            DcbCriteria criteria = DcbCriteria.type(type).tags(tag(course), tag(student));
            DcbConsistencyToken token = dcbEventStore().read(criteria).consistencyToken();

            dcbEventStore().append(List.of(taggedEvent(type, course, student)));

            assertThatThrownBy(() -> dcbEventStore().append(
                    List.of(taggedEvent(type, course, student)), failIfEventsMatch(token)))
                    .as("The control of the accepted case: an event of the type that carries both tags does match, "
                            + "so a store that never refuses an event that shares only some of its markers with the "
                            + "criteria would accept this append")
                    .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        }

        @Test
        void a_condition_on_two_tag_alternatives_conflicts_on_an_event_carrying_only_the_tag_of_the_second_alternative() {
            String tagA = "alternatives-conflict:a";
            String tagB = "alternatives-conflict:b";
            DcbCriteria criteria = DcbCriteria.anyOf(DcbCriteria.tags(tag(tagA)), DcbCriteria.tags(tag(tagB)));
            DcbConsistencyToken token = dcbEventStore().read(criteria).consistencyToken();

            dcbEventStore().append(List.of(taggedEvent(CHANGED, tagB)));
            DcbEventStream before = dcbEventStore().read(criteria);

            assertThatThrownBy(() -> dcbEventStore().append(
                    List.of(taggedEvent(SNAPSHOT, "alternatives-conflict:unrelated")), failIfEventsMatch(token)))
                    .as("The event committed after the read matches the second alternative, and the tag of the first "
                            + "alternative did not move. Each alternative is judged on its own tags, so the unmoved "
                            + "tag of one must not clear the other")
                    .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);

            assertThat(positionsOf(dcbEventStore().read(criteria).events()))
                    .as("The refused append must not have added an event the criteria select")
                    .isEqualTo(positionsOf(before.events()));
        }

        @Test
        void a_condition_on_a_type_alternative_and_a_two_tag_alternative_ignores_an_event_of_another_type_carrying_one_of_the_tags_unless_the_model_compares_only_markers() {
            String type = "AlternativesAcceptedType";
            String otherType = "AlternativesAcceptedOther";
            String tagA = "alternatives-accepted:a";
            String tagB = "alternatives-accepted:b";
            DcbCriteria criteria = DcbCriteria.anyOf(DcbCriteria.type(type), DcbCriteria.tags(tag(tagA), tag(tagB)));
            DcbConsistencyToken token = dcbEventStore().read(criteria).consistencyToken();

            dcbEventStore().append(List.of(taggedEvent(otherType, tagA)));

            List<CloudEvent> events = List.of(taggedEvent(otherType, "alternatives-accepted:unrelated"));
            DcbAppendCondition condition = failIfEventsMatch(token);
            AtomicReference<DcbAppendResult> result = new AtomicReference<>();
            if (expectedToConflictOnANonMatchingEventSharingATag()) {
                Throwable thrown = catchThrowable(() -> result.set(dcbEventStore().append(events, condition)));
                if (thrown == null) {
                    assertThat(result.get().eventCount())
                            .as("A store that compares the marker of each tag can accept the append, and then it must "
                                    + "have written its event")
                            .isEqualTo(1);
                } else {
                    assertThat(thrown)
                            .as("A store that compares the markers as a sum can refuse the append, because the marker "
                                    + "of tag a moved. It must refuse with the append condition exception")
                            .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
                }
            } else {
                assertThatCode(() -> result.set(dcbEventStore().append(events, condition)))
                        .as("The event committed after the read has neither the type of the first alternative nor both "
                                + "tags of the second, so it matches neither. A store that cannot tell this from a match "
                                + "of the type alternative, or of the tag alternative, would refuse")
                        .doesNotThrowAnyException();
                assertThat(result.get().eventCount())
                        .as("The accepted append must have written its event")
                        .isEqualTo(1);
            }

            assertThat(dcbEventStore().read(criteria).events())
                    .as("The event of the append matches neither alternative, so the criteria selects none either way")
                    .isEmpty();
        }

        @Test
        void a_token_read_on_a_tag_no_event_carried_ignores_an_append_that_does_not_carry_it() {
            String tagZ = "unused-tag-accepted:z";
            DcbCriteria criteria = DcbCriteria.tags(tag(tagZ));
            DcbConsistencyToken token = dcbEventStore().read(criteria).consistencyToken();

            dcbEventStore().append(List.of(taggedEvent(DEFINED, "unused-tag-accepted:other")));

            assertThatCode(() -> dcbEventStore().append(
                    List.of(taggedEvent(CHANGED, tagZ)), failIfEventsMatch(token)))
                    .as("The tag the read looked for still has no event, so nothing matching was committed after the "
                            + "read. A store that counts a tag it has never seen as having changed would refuse")
                    .doesNotThrowAnyException();
        }

        @Test
        void a_token_read_on_a_tag_no_event_carried_conflicts_on_an_append_that_carries_it() {
            String tagZ = "unused-tag-control:z";
            DcbCriteria criteria = DcbCriteria.tags(tag(tagZ));
            DcbConsistencyToken token = dcbEventStore().read(criteria).consistencyToken();

            dcbEventStore().append(List.of(taggedEvent(DEFINED, tagZ)));

            assertThatThrownBy(() -> dcbEventStore().append(
                    List.of(taggedEvent(CHANGED, tagZ)), failIfEventsMatch(token)))
                    .as("The control of the accepted case: the first event with the tag is a match committed after "
                            + "the read, so a store that treats a tag it had not seen as unchanged would accept this append")
                    .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        }

        @Test
        void a_whole_store_lock_with_no_token_succeeds_only_while_the_store_holds_no_dcb_event() {
            DcbAppendResult result = dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)), wholeStoreLock());
            assertThat(result.eventCount())
                    .as("A whole-store lock is correct for a bootstrap append, so it must let the first append "
                            + "through")
                    .isEqualTo(1);

            assertThatThrownBy(() -> dcbEventStore().append(List.of(taggedEvent(CHANGED, NAME_2)), wholeStoreLock()))
                    .as("Once any DCB event exists, an untokenized whole-store lock must reject the append whatever "
                            + "boundary the existing event belongs to")
                    .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        }

        @Test
        void a_whole_store_lock_conflicts_on_a_later_whole_store_append() {
            DcbConsistencyToken token = dcbEventStore().read(DcbCriteria.all()).consistencyToken();
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)), wholeStoreLock(token));

            assertThatThrownBy(() -> dcbEventStore().append(
                    List.of(taggedEvent(CHANGED, NAME_2)), wholeStoreLock(token)))
                    .as("A whole-store lock is skew-safe against another whole-store append, which is the case it "
                            + "is documented to be correct for")
                    .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        }

        @Test
        void an_excluded_type_sharing_a_boundary_tag_conflicts_only_under_the_tag_marker_model() {
            DcbCriterion criteria = DcbCriteria.tags(tag(NAME_1)).excludingTypes(SNAPSHOT);
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));
            DcbConsistencyToken token = dcbEventStore().read(criteria).consistencyToken();

            // The excluded-type event is committed after the read. A read excludes it precisely (asserted by
            // ReadingByCriteria), but it carries the positive tag the boundary is keyed on.
            dcbEventStore().append(List.of(taggedEvent(SNAPSHOT, NAME_1)));

            List<CloudEvent> events = List.of(taggedEvent(CHANGED, NAME_1));
            DcbAppendCondition condition = failIfEventsMatch(token);
            if (expectedToConflictOnANonMatchingEventSharingATag()) {
                assertThatThrownBy(() -> dcbEventStore().append(events, condition))
                        .as("Under the tag-marker model the boundary's marker was bumped by the excluded event, so "
                                + "the condition conflicts. A false conflict, and a sound one: it never misses a "
                                + "real conflict, and the application service re-reads the still-excluded boundary "
                                + "and retries")
                        .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
            } else {
                assertThat(dcbEventStore().append(events, condition).eventCount())
                        .as("Under the exact-criteria and the markers-then-events models the exclusion is applied to "
                                + "the event itself, so an excluded event is not a conflict however it is tagged")
                        .isEqualTo(1);
            }
        }

        @Test
        void an_event_of_another_type_sharing_the_tag_of_a_type_and_tag_condition_conflicts_only_under_the_tag_marker_model() {
            String type = "TicketSoldShared";
            String otherType = "ConcertRenamedShared";
            String concert = "shared-tag-concert:k1";
            DcbCriteria criteria = DcbCriteria.type(type).tags(tag(concert));
            DcbConsistencyToken token = dcbEventStore().read(criteria).consistencyToken();

            dcbEventStore().append(List.of(taggedEvent(otherType, concert)));

            List<CloudEvent> events = List.of(taggedEvent(type, concert));
            DcbAppendCondition condition = failIfEventsMatch(token);
            if (expectedToConflictOnANonMatchingEventSharingATag()) {
                assertThatThrownBy(() -> dcbEventStore().append(events, condition))
                        .as("Under the tag-marker model the marker of the concert tag moved, and it knows nothing about "
                                + "types, so the event of the other type conflicts")
                        .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
            } else {
                AtomicReference<DcbAppendResult> result = new AtomicReference<>();
                assertThatCode(() -> result.set(dcbEventStore().append(events, condition)))
                        .as("The event committed after the read has the tag of the criteria but not its type, so it "
                                + "doesn't match. Only a store that tells a matching event from one that shares a tag "
                                + "accepts this append")
                        .doesNotThrowAnyException();
                assertThat(result.get().eventCount())
                        .as("The accepted append must have written its event")
                        .isEqualTo(1);
            }
        }

        @Test
        void a_condition_on_a_type_and_a_tag_conflicts_on_an_event_of_that_type_with_the_tag_committed_after_the_read_although_another_type_with_the_tag_came_first() {
            String type = "TicketSoldControl";
            String otherType = "ConcertRenamedControl";
            String concert = "shared-tag-control-concert:k1";
            DcbCriteria criteria = DcbCriteria.type(type).tags(tag(concert));
            DcbConsistencyToken token = dcbEventStore().read(criteria).consistencyToken();

            dcbEventStore().append(List.of(taggedEvent(otherType, concert)));
            dcbEventStore().append(List.of(taggedEvent(type, concert)));

            assertThatThrownBy(() -> dcbEventStore().append(
                    List.of(taggedEvent(type, concert)), failIfEventsMatch(token)))
                    .as("The control of the case above: the marker of the tag moved for two appends, and one of them "
                            + "is an event of the criteria's type with its tag. A store that lets an append through "
                            + "because the tag was shared with a non-matching event would accept this one too")
                    .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        }

        @Test
        void a_condition_on_two_types_conflicts_on_an_event_of_the_second_type_committed_after_the_read() {
            String typeX = "TwoTypesConflictX";
            String typeY = "TwoTypesConflictY";
            DcbCriteria criteria = DcbCriteria.types(typeX, typeY);
            DcbConsistencyToken token = dcbEventStore().read(criteria).consistencyToken();

            dcbEventStore().append(List.of(taggedEvent(typeY, "two-types-conflict:a")));

            assertThatThrownBy(() -> dcbEventStore().append(
                    List.of(taggedEvent(typeX, "two-types-conflict:b")), failIfEventsMatch(token)))
                    .as("An event of the second type matches the criteria, and the marker of the first type didn't "
                            + "move. A store that looks at the first type alone would accept this append")
                    .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        }

        @Test
        void a_condition_on_two_types_ignores_an_event_of_a_third_type_committed_after_the_read() {
            String typeX = "TwoTypesAcceptedX";
            String typeY = "TwoTypesAcceptedY";
            String typeZ = "TwoTypesAcceptedZ";
            DcbCriteria criteria = DcbCriteria.types(typeX, typeY);
            DcbConsistencyToken token = dcbEventStore().read(criteria).consistencyToken();

            dcbEventStore().append(List.of(taggedEvent(typeZ, "two-types-accepted:a")));

            AtomicReference<DcbAppendResult> result = new AtomicReference<>();
            assertThatCode(() -> result.set(dcbEventStore().append(
                    List.of(taggedEvent(typeX, "two-types-accepted:b")), failIfEventsMatch(token))))
                    .as("The event committed after the read is of neither type, so it doesn't match. The control of "
                            + "the conflict case: a store that refuses every append after any event would reject this one")
                    .doesNotThrowAnyException();
            assertThat(result.get().eventCount())
                    .as("The accepted append must have written its event")
                    .isEqualTo(1);
        }

        @Test
        void a_whole_store_lock_detects_a_later_tag_scoped_append_only_under_the_exact_criteria_model() {
            DcbConsistencyToken token = dcbEventStore().read(DcbCriteria.all()).consistencyToken();

            // A tag-scoped append commits after the whole-store read.
            dcbEventStore().append(List.of(taggedEvent(DEFINED, NAME_1)));

            List<CloudEvent> events = List.of(taggedEvent(CHANGED, NAME_2));
            DcbAppendCondition condition = wholeStoreLock(token);
            if (expectedToMissATagScopedAppendUnderAWholeStoreLock()) {
                assertThat(dcbEventStore().append(events, condition).eventCount())
                        .as("Under the tag-marker and the markers-then-events models a whole-store lock is keyed on a "
                                + "marker only another whole-store append touches, so it does not see a tag-scoped "
                                + "append. This is the "
                                + "limitation wholeStoreLock() documents, and the reason it is correct only for a "
                                + "single writer or a bootstrap append that races only with other bootstrap appends")
                        .isEqualTo(1);
            } else {
                assertThatThrownBy(() -> dcbEventStore().append(events, condition))
                        .as("Under the exact-criteria model MatchAll matches the tag-scoped event like any other, "
                                + "so the whole-store lock conflicts")
                        .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
            }
        }
    }

    /**
     * Whether the declared append-condition model conflicts on an event that shares a tag with the criteria, whether it
     * is of a type the criteria excludes or lacks another tag the criteria requires, although no event matching the
     * criteria was committed. Written as a switch expression so adding a model breaks the build here rather than
     * silently taking one of the existing branches.
     */
    private boolean expectedToConflictOnANonMatchingEventSharingATag() {
        return switch (appendConditionModel()) {
            case EXACT_CRITERIA, MARKERS_THEN_EVENTS -> false;
            case TAG_MARKER -> true;
        };
    }

    /**
     * Whether the declared append-condition model lets a whole-store lock pass after a tag-scoped append committed
     * since its read. A separate question from the one above, since a model can answer exactly on one and coarsely on
     * the other.
     */
    private boolean expectedToMissATagScopedAppendUnderAWholeStoreLock() {
        return switch (appendConditionModel()) {
            case EXACT_CRITERIA -> false;
            case MARKERS_THEN_EVENTS, TAG_MARKER -> true;
        };
    }

    private static List<Long> positionsOf(List<CloudEvent> events) {
        return events.stream().map(OccurrentCloudEventExtension::getPosition).toList();
    }
}
