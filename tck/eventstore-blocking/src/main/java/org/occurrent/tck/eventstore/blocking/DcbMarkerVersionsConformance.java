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
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions.MarkerVersion;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.DcbSnapshotInvalidException;
import org.occurrent.tck.ConformanceEvents;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.tck.ConformanceEvents.extension;
import static org.occurrent.tck.ConformanceEvents.idsOf;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.tag;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.taggedEvent;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.taggedEventWithId;

/**
 * What a DCB caller sees of marker versions, the values a snapshot saves to resume a read without comparing positions.
 * A marker key is {@code tag:<tag>} for a tag and {@code type:<type>} for an event type, and a read reports the
 * versions of the keys of its criteria.
 * <p>
 * The suite states only what a caller sees, which is the same for every store. These things are left out on purpose,
 * because they describe how a store keeps the contract rather than the contract:
 * <ul>
 *     <li><strong>The epoch and unstamped values a store reports.</strong> They are opaque, so they are round-tripped
 *     and never read. The refusal tests change one of them to a value no store reported, which is a fact about the
 *     caller's saved copy and nothing about the store.</li>
 *     <li><strong>The cause of a refusal beyond the check that failed.</strong> A refusal is a
 *     {@link DcbSnapshotInvalidException}, and the tests that look at its message look for the marker key or the
 *     check, because that is what the exception documents.</li>
 *     <li><strong>Reading versions inside a transaction the caller owns, and how a store takes its snapshot.</strong>
 *     Those differ between stores and live in the tests of each store.</li>
 * </ul>
 * A late commit below a position a read saw is covered by {@link DcbLateCommitConformance}, which resumes from marker
 * versions with the hold the stores that reserve a position early provide.
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the DCB marker-versions contract")
public abstract class DcbMarkerVersionsConformance extends EventStoreConformance {

    private static final String DEPOSITED = "Deposited";
    private static final String WITHDRAWN = "Withdrawn";
    private static final String CLOSED = "Closed";
    private static final String ACCOUNT_A = "account:a1";
    private static final String ACCOUNT_B = "account:b1";
    private static final String ACCOUNT_C = "account:c1";
    private static final String REGION = "region:eu";
    private static final String KEY_A = "tag:account:a1";
    private static final String KEY_B = "tag:account:b1";
    private static final String KEY_DEPOSITED = "type:Deposited";
    private static final String KEY_WITHDRAWN = "type:Withdrawn";

    @Override
    protected final Set<EventStoreCapability> requiredCapabilities() {
        return Set.of(EventStoreCapability.DCB);
    }

    @Nested
    @DisplayName("an append result")
    class AnAppendResult {

        @Test
        void reports_a_version_for_every_marker_key_of_the_criteria_of_a_conditional_append_with_a_token() {
            DcbCriteria criteria = DcbCriteria.anyOf(DcbCriteria.tags(tag(ACCOUNT_A)), DcbCriteria.type(DEPOSITED));
            DcbEventStream decidedOn = dcbEventStore().read(criteria);

            DcbAppendResult result = dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)),
                    failIfEventsMatch(decidedOn.consistencyToken()));

            assertThat(result.markerVersions())
                    .as("A conditional append with a consistency token must report the versions its criteria ended at")
                    .isPresent();
            assertThat(result.markerVersions().orElseThrow().keys())
                    .as("One entry per marker key of the criteria, a tag key per tag and a type key per type")
                    .containsExactly(KEY_A, KEY_DEPOSITED);
        }

        @Test
        void reports_only_the_marker_keys_of_the_criteria_and_not_those_of_the_other_tags_and_the_type_of_the_events() {
            DcbCriteria criteria = DcbCriteria.tags(tag(ACCOUNT_A));
            DcbEventStream decidedOn = dcbEventStore().read(criteria);

            DcbAppendResult result = dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A, REGION)),
                    failIfEventsMatch(decidedOn.consistencyToken()));

            assertThat(result.markerVersions().orElseThrow().keys())
                    .as("The events carry a region tag and a type, but the criteria names only the account tag")
                    .containsExactly(KEY_A);
        }

        @Test
        void reports_a_higher_version_of_the_same_epoch_after_each_conditional_append() {
            DcbCriteria criteria = DcbCriteria.tags(tag(ACCOUNT_A));
            DcbAppendResult first = dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)),
                    failIfEventsMatch(dcbEventStore().read(criteria).consistencyToken()));
            DcbAppendResult second = dcbEventStore().append(List.of(taggedEvent(WITHDRAWN, ACCOUNT_A)),
                    failIfEventsMatch(dcbEventStore().read(criteria).consistencyToken()));

            MarkerVersion afterFirst = first.markerVersions().orElseThrow().get(KEY_A).orElseThrow();
            MarkerVersion afterSecond = second.markerVersions().orElseThrow().get(KEY_A).orElseThrow();
            assertThat(afterSecond.epoch())
                    .as("A marker keeps its epoch while it is not removed")
                    .isEqualTo(afterFirst.epoch());
            assertThat(afterSecond.version())
                    .as("Each append that touches the marker raises its version")
                    .isGreaterThan(afterFirst.version());
        }

        @Test
        void reports_no_versions_for_an_unconditional_append() {
            DcbAppendResult result = dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)));

            assertThat(result.markerVersions()).as("An append with no condition has no criteria to report").isEmpty();
        }

        @Test
        void reports_no_versions_for_a_condition_without_a_token() {
            DcbAppendResult result = dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)),
                    failIfEventsMatch(DcbCriteria.tags(tag(ACCOUNT_A))));

            assertThat(result.markerVersions()).as("Only a condition qualified by a consistency token reports versions").isEmpty();
        }

        @Test
        void reports_no_versions_for_a_condition_that_matches_every_event() {
            DcbEventStream decidedOn = dcbEventStore().read(DcbCriteria.all());

            DcbAppendResult result = dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)),
                    failIfEventsMatch(DcbCriteria.all(), decidedOn.consistencyToken()));

            assertThat(result.markerVersions()).as("MatchAll has no marker keys of its own to report").isEmpty();
        }

        @Test
        void reports_no_versions_for_the_whole_store_lock() {
            DcbEventStream decidedOn = dcbEventStore().read(DcbCriteria.all());

            DcbAppendResult result = dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)),
                    DcbAppendCondition.wholeStoreLock(decidedOn.consistencyToken()));

            assertThat(result.markerVersions()).as("The whole-store lock is a MatchAll condition").isEmpty();
        }
    }

    @Nested
    @DisplayName("a read that asks for versions")
    class AReadThatAsksForVersions {

        @Test
        void reports_a_version_for_every_marker_key_of_its_criteria_when_it_reads_the_whole_boundary_forwards() {
            DcbCriteria criteria = DcbCriteria.anyOf(DcbCriteria.tags(tag(ACCOUNT_A)), DcbCriteria.type(DEPOSITED));
            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)));

            DcbEventStream read = dcbEventStore().read(criteria, DcbReadOptions.fromBeginning().withVersions());

            assertThat(read.markerVersions()).as("A full forward read that asks must report versions").isPresent();
            assertThat(read.markerVersions().orElseThrow().keys()).containsExactly(KEY_A, KEY_DEPOSITED);
        }

        @Test
        void reports_none_when_it_does_not_ask() {
            DcbCriteria criteria = DcbCriteria.tags(tag(ACCOUNT_A));
            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)));

            assertThat(dcbEventStore().read(criteria).markerVersions()).isEmpty();
            assertThat(dcbEventStore().read(criteria, DcbReadOptions.fromBeginning()).markerVersions()).isEmpty();
        }

        @Test
        void reports_none_for_criteria_that_match_every_event() {
            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)));

            DcbEventStream read = dcbEventStore().read(DcbCriteria.all(), DcbReadOptions.fromBeginning().withVersions());

            assertThat(read.events()).as("The read must have found the event, or it proves nothing").hasSize(1);
            assertThat(read.markerVersions()).isEmpty();
        }

        @Test
        void reports_none_with_a_limit() {
            assertReportsNoVersionsForRestriction(DcbReadOptions.fromBeginning().withVersions().limit(1));
        }

        @Test
        void reports_none_with_a_skip() {
            assertReportsNoVersionsForRestriction(DcbReadOptions.fromBeginning().withVersions().skip(1));
        }

        @Test
        void reports_none_when_it_reads_backwards() {
            assertReportsNoVersionsForRestriction(DcbReadOptions.fromBeginning().withVersions().backwards());
        }

        @Test
        void reports_none_after_a_position() {
            DcbAppendResult first = dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)));
            dcbEventStore().append(List.of(taggedEvent(WITHDRAWN, ACCOUNT_A)));

            assertReportsNoVersions(DcbReadOptions.afterPosition(first.lastSequencePosition()).withVersions());
        }

        @Test
        void reports_none_after_position_zero_because_a_lower_bound_is_a_lower_bound_whatever_its_value() {
            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)));

            assertReportsNoVersions(DcbReadOptions.afterPosition(0).withVersions());
        }

        @Test
        void reports_none_up_to_a_position() {
            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)));
            DcbAppendResult second = dcbEventStore().append(List.of(taggedEvent(WITHDRAWN, ACCOUNT_A)));

            assertReportsNoVersions(DcbReadOptions.upToPosition(second.lastSequencePosition()).withVersions());
        }

        @Test
        void reports_none_between_two_positions() {
            DcbAppendResult first = dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)));
            DcbAppendResult second = dcbEventStore().append(List.of(taggedEvent(WITHDRAWN, ACCOUNT_A)));

            assertReportsNoVersions(DcbReadOptions.between(first.lastSequencePosition(), second.lastSequencePosition()).withVersions());
        }

        private void assertReportsNoVersionsForRestriction(DcbReadOptions restricted) {
            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)));
            dcbEventStore().append(List.of(taggedEvent(WITHDRAWN, ACCOUNT_A)));

            assertReportsNoVersions(restricted);
        }

        private void assertReportsNoVersions(DcbReadOptions restricted) {
            DcbCriteria criteria = DcbCriteria.tags(tag(ACCOUNT_A));
            assertThat(dcbEventStore().read(criteria, DcbReadOptions.fromBeginning().withVersions()).markerVersions())
                    .as("The same boundary read in full reports versions, or the restricted read proves nothing")
                    .isPresent();

            assertThat(dcbEventStore().read(criteria, restricted).markerVersions())
                    .as("A read that may have stopped early, or started after an event that commits late, must not "
                            + "report versions it did not read up to")
                    .isEmpty();
        }
    }

    @Nested
    @DisplayName("the consistency token of a read with versions")
    class TheConsistencyTokenOfAReadWithVersions {

        @Test
        void holds_the_criteria_of_a_read_with_versions_and_conflicts_on_a_matching_append_committed_after_it() {
            DcbCriteria boundary = DcbCriteria.anyOf(DcbCriteria.tags(tag(ACCOUNT_A)), DcbCriteria.tags(tag(ACCOUNT_B)));
            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)));
            DcbEventStream read = dcbEventStore().read(boundary, DcbReadOptions.fromBeginning().withVersions());

            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_B)));

            Throwable thrown = catchThrowable(() -> dcbEventStore().append(
                    List.of(taggedEvent(WITHDRAWN, ACCOUNT_A)), failIfEventsMatch(read.consistencyToken())));
            assertThat(read.consistencyToken().criteria())
                    .as("The token of a read that asked for versions holds the criteria the read was given, both alternatives")
                    .isEqualTo(boundary);
            assertThat(thrown)
                    .as("An event of the second alternative committed after the read must reject the append")
                    .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        }

        @Test
        void holds_the_criteria_of_a_read_after_versions_and_conflicts_on_a_matching_append_committed_after_it() {
            DcbCriteria boundary = DcbCriteria.anyOf(DcbCriteria.tags(tag(ACCOUNT_A)), DcbCriteria.tags(tag(ACCOUNT_B)));
            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A), taggedEvent(DEPOSITED, ACCOUNT_B)));
            DcbMarkerVersions saved = versionsOf(dcbEventStore().read(boundary, DcbReadOptions.fromBeginning().withVersions()));
            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)));
            DcbEventStream resumed = dcbEventStore().read(boundary, DcbReadOptions.afterVersions(saved));

            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_B)));

            Throwable thrown = catchThrowable(() -> dcbEventStore().append(
                    List.of(taggedEvent(WITHDRAWN, ACCOUNT_A)), failIfEventsMatch(resumed.consistencyToken())));
            assertThat(resumed.consistencyToken().criteria())
                    .as("The token of a resumed read holds the criteria the read was given, not only the part it resumed")
                    .isEqualTo(boundary);
            assertThat(thrown)
                    .as("An event of the second alternative committed after the resumed read must reject the append")
                    .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        }

        @Test
        void a_read_with_versions_lets_a_conditional_append_through_when_nothing_matching_was_committed_after_it() {
            DcbCriteria boundary = DcbCriteria.anyOf(DcbCriteria.tags(tag(ACCOUNT_A)), DcbCriteria.tags(tag(ACCOUNT_B)));
            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A), taggedEvent(DEPOSITED, ACCOUNT_B)));
            DcbEventStream read = dcbEventStore().read(boundary, DcbReadOptions.fromBeginning().withVersions());

            assertThatCode(() -> dcbEventStore().append(
                    List.of(taggedEvent(WITHDRAWN, ACCOUNT_A)), failIfEventsMatch(read.consistencyToken())))
                    .as("The token of a read that asked for versions must be accepted when no event of the boundary "
                            + "was committed after the read, or it refuses an append it should let through")
                    .doesNotThrowAnyException();
        }

        @Test
        void a_read_after_versions_lets_a_conditional_append_through_when_nothing_matching_was_committed_after_it() {
            DcbCriteria boundary = DcbCriteria.anyOf(DcbCriteria.tags(tag(ACCOUNT_A)), DcbCriteria.tags(tag(ACCOUNT_B)));
            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A), taggedEvent(DEPOSITED, ACCOUNT_B)));
            DcbMarkerVersions saved = versionsOf(dcbEventStore().read(boundary, DcbReadOptions.fromBeginning().withVersions()));
            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)));
            DcbEventStream resumed = dcbEventStore().read(boundary, DcbReadOptions.afterVersions(saved));

            assertThatCode(() -> dcbEventStore().append(
                    List.of(taggedEvent(WITHDRAWN, ACCOUNT_A)), failIfEventsMatch(resumed.consistencyToken())))
                    .as("The token of a resumed read must be accepted when no event of the boundary was committed "
                            + "after the read, or it refuses an append it should let through")
                    .doesNotThrowAnyException();
        }

        @Test
        void holds_all_criteria_for_a_read_of_everything_with_versions_and_conflicts_on_a_whole_store_append_committed_after_it() {
            DcbEventStream read = dcbEventStore().read(DcbCriteria.all(), DcbReadOptions.fromBeginning().withVersions());

            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)), DcbAppendCondition.wholeStoreLock());

            Throwable thrown = catchThrowable(() -> dcbEventStore().append(
                    List.of(taggedEvent(WITHDRAWN, ACCOUNT_B)), DcbAppendCondition.wholeStoreLock(read.consistencyToken())));
            assertThat(read.consistencyToken().criteria())
                    .as("The token of a read of everything that asked for versions holds all criteria, not other criteria")
                    .isEqualTo(DcbCriteria.all());
            assertThat(thrown)
                    .as("A whole-store append committed after the read must reject a whole-store lock built from its token")
                    .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        }
    }

    @Nested
    @DisplayName("a read after versions")
    class AReadAfterVersions {

        @Test
        void returns_exactly_the_events_of_a_tag_criteria_that_were_appended_after_the_versions() {
            DcbCriteria boundary = DcbCriteria.tags(tag(ACCOUNT_A));
            dcbEventStore().append(List.of(taggedEventWithId("before-1", DEPOSITED, ACCOUNT_A), taggedEventWithId("before-other", DEPOSITED, ACCOUNT_B)));
            dcbEventStore().append(List.of(taggedEventWithId("before-2", WITHDRAWN, ACCOUNT_A)));
            DcbMarkerVersions saved = versionsOf(dcbEventStore().read(boundary, DcbReadOptions.fromBeginning().withVersions()));

            dcbEventStore().append(List.of(taggedEventWithId("after-1", DEPOSITED, ACCOUNT_A)));
            dcbEventStore().append(List.of(taggedEventWithId("after-other", DEPOSITED, ACCOUNT_B)));
            dcbEventStore().append(List.of(taggedEventWithId("after-2", WITHDRAWN, ACCOUNT_A, REGION)),
                    failIfEventsMatch(dcbEventStore().read(boundary).consistencyToken()));

            DcbEventStream resumed = dcbEventStore().read(boundary, DcbReadOptions.afterVersions(saved));

            assertThat(idsOf(resumed.events()))
                    .as("The events of the boundary after the versions, in position order, and nothing the versions covered")
                    .containsExactly("after-1", "after-2");
        }

        @Test
        void returns_every_event_of_an_append_made_after_the_versions() {
            DcbCriteria boundary = DcbCriteria.tags(tag(ACCOUNT_A));
            dcbEventStore().append(List.of(taggedEventWithId("before", DEPOSITED, ACCOUNT_A)));
            DcbMarkerVersions saved = versionsOf(dcbEventStore().read(boundary, DcbReadOptions.fromBeginning().withVersions()));

            dcbEventStore().append(List.of(
                    taggedEventWithId("after-1", DEPOSITED, ACCOUNT_A),
                    taggedEventWithId("after-2", WITHDRAWN, ACCOUNT_A),
                    taggedEventWithId("after-3", DEPOSITED, ACCOUNT_A)));

            assertThat(idsOf(dcbEventStore().read(boundary, DcbReadOptions.afterVersions(saved)).events()))
                    .containsExactly("after-1", "after-2", "after-3");
        }

        @Test
        void returns_exactly_the_events_of_a_type_only_criteria_that_were_appended_after_the_versions() {
            DcbCriteria boundary = DcbCriteria.type(DEPOSITED);
            dcbEventStore().append(List.of(untaggedWithId("before-deposited", DEPOSITED), untaggedWithId("before-withdrawn", WITHDRAWN)));
            DcbMarkerVersions saved = versionsOf(dcbEventStore().read(boundary, DcbReadOptions.fromBeginning().withVersions()));

            dcbEventStore().append(List.of(untaggedWithId("after-deposited", DEPOSITED)));
            dcbEventStore().append(List.of(untaggedWithId("after-withdrawn", WITHDRAWN)));
            dcbEventStore().append(List.of(taggedEventWithId("after-tagged-deposited", DEPOSITED, ACCOUNT_A)));

            assertThat(idsOf(dcbEventStore().read(boundary, DcbReadOptions.afterVersions(saved)).events()))
                    .as("A type-only criteria matches the events of its type whatever tags they carry, and none of another type")
                    .containsExactly("after-deposited", "after-tagged-deposited");
        }

        @Test
        void returns_the_events_of_any_of_the_types_of_a_criteria_with_several_types_that_were_appended_after_the_versions() {
            DcbCriteria boundary = DcbCriteria.types(DEPOSITED, WITHDRAWN);
            dcbEventStore().append(List.of(untaggedWithId("before-deposited", DEPOSITED), untaggedWithId("before-withdrawn", WITHDRAWN)));
            DcbMarkerVersions saved = versionsOf(dcbEventStore().read(boundary, DcbReadOptions.fromBeginning().withVersions()));
            assertThat(saved.keys()).containsExactly(KEY_DEPOSITED, KEY_WITHDRAWN);

            dcbEventStore().append(List.of(untaggedWithId("after-withdrawn", WITHDRAWN)));
            dcbEventStore().append(List.of(untaggedWithId("after-closed", CLOSED)));
            dcbEventStore().append(List.of(untaggedWithId("after-deposited", DEPOSITED)));

            assertThat(idsOf(dcbEventStore().read(boundary, DcbReadOptions.afterVersions(saved)).events()))
                    .containsExactly("after-withdrawn", "after-deposited");
        }

        @Test
        void returns_only_the_events_that_match_both_the_tags_and_the_types_of_a_criteria_with_both() {
            DcbCriteria boundary = DcbCriteria.tags(tag(ACCOUNT_A)).types(DEPOSITED);
            dcbEventStore().append(List.of(taggedEventWithId("before", DEPOSITED, ACCOUNT_A)));
            DcbMarkerVersions saved = versionsOf(dcbEventStore().read(boundary, DcbReadOptions.fromBeginning().withVersions()));

            dcbEventStore().append(List.of(taggedEventWithId("after-wrong-type", WITHDRAWN, ACCOUNT_A)));
            dcbEventStore().append(List.of(taggedEventWithId("after-wrong-tag", DEPOSITED, ACCOUNT_B)));
            dcbEventStore().append(List.of(taggedEventWithId("after-match", DEPOSITED, ACCOUNT_A)));

            assertThat(idsOf(dcbEventStore().read(boundary, DcbReadOptions.afterVersions(saved)).events()))
                    .containsExactly("after-match");
        }

        @Test
        void returns_only_the_events_that_carry_every_tag_of_a_criteria_with_several_tags() {
            DcbCriteria boundary = DcbCriteria.tags(tag(ACCOUNT_A), tag(ACCOUNT_B));
            dcbEventStore().append(List.of(taggedEventWithId("before", DEPOSITED, ACCOUNT_A, ACCOUNT_B)));
            DcbMarkerVersions saved = versionsOf(dcbEventStore().read(boundary, DcbReadOptions.fromBeginning().withVersions()));
            assertThat(saved.keys()).containsExactly(KEY_A, KEY_B);

            dcbEventStore().append(List.of(taggedEventWithId("after-first-tag-only", DEPOSITED, ACCOUNT_A)));
            dcbEventStore().append(List.of(taggedEventWithId("after-second-tag-only", DEPOSITED, ACCOUNT_B)));
            dcbEventStore().append(List.of(taggedEventWithId("after-both", DEPOSITED, ACCOUNT_A, ACCOUNT_B, REGION)));

            assertThat(idsOf(dcbEventStore().read(boundary, DcbReadOptions.afterVersions(saved)).events()))
                    .containsExactly("after-both");
        }

        @Test
        void returns_the_events_matching_any_item_of_a_criteria_with_several_items() {
            DcbCriteria boundary = DcbCriteria.anyOf(DcbCriteria.tags(tag(ACCOUNT_A)), DcbCriteria.tags(tag(ACCOUNT_B)));
            dcbEventStore().append(List.of(taggedEventWithId("before-a", DEPOSITED, ACCOUNT_A), taggedEventWithId("before-b", DEPOSITED, ACCOUNT_B)));
            DcbMarkerVersions saved = versionsOf(dcbEventStore().read(boundary, DcbReadOptions.fromBeginning().withVersions()));

            dcbEventStore().append(List.of(taggedEventWithId("after-b", DEPOSITED, ACCOUNT_B)));
            dcbEventStore().append(List.of(taggedEventWithId("after-c", DEPOSITED, ACCOUNT_C)));
            dcbEventStore().append(List.of(taggedEventWithId("after-a", DEPOSITED, ACCOUNT_A)));

            assertThat(idsOf(dcbEventStore().read(boundary, DcbReadOptions.afterVersions(saved)).events()))
                    .containsExactly("after-b", "after-a");
        }

        @Test
        void is_empty_after_the_newest_versions_and_reports_the_same_versions_again() {
            DcbCriteria boundary = DcbCriteria.tags(tag(ACCOUNT_A));
            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)));
            dcbEventStore().append(List.of(taggedEvent(WITHDRAWN, ACCOUNT_A)));
            DcbMarkerVersions newest = versionsOf(dcbEventStore().read(boundary, DcbReadOptions.fromBeginning().withVersions()));

            DcbEventStream resumed = dcbEventStore().read(boundary, DcbReadOptions.afterVersions(newest));

            assertThat(resumed.events()).as("Nothing was appended after the newest versions").isEmpty();
            assertThat(resumed.markerVersions()).as("The read reports where it ended, which is where it started").contains(newest);
        }

        @Test
        void reports_the_versions_it_ended_at_so_the_next_read_returns_only_what_came_after_them() {
            DcbCriteria boundary = DcbCriteria.tags(tag(ACCOUNT_A));
            dcbEventStore().append(List.of(taggedEventWithId("first", DEPOSITED, ACCOUNT_A)));
            DcbMarkerVersions afterFirst = versionsOf(dcbEventStore().read(boundary, DcbReadOptions.fromBeginning().withVersions()));
            dcbEventStore().append(List.of(taggedEventWithId("second", DEPOSITED, ACCOUNT_A)));

            DcbEventStream secondRead = dcbEventStore().read(boundary, DcbReadOptions.afterVersions(afterFirst));
            DcbMarkerVersions afterSecond = versionsOf(secondRead);
            dcbEventStore().append(List.of(taggedEventWithId("third", DEPOSITED, ACCOUNT_A)));
            DcbEventStream thirdRead = dcbEventStore().read(boundary, DcbReadOptions.afterVersions(afterSecond));

            assertThat(idsOf(secondRead.events())).containsExactly("second");
            assertThat(afterSecond).as("The versions moved on with the event that was read").isNotEqualTo(afterFirst);
            assertThat(idsOf(thirdRead.events())).containsExactly("third");
        }

        @Test
        void does_not_return_the_events_of_the_append_that_reported_the_versions_but_returns_a_later_one() {
            DcbCriteria boundary = DcbCriteria.tags(tag(ACCOUNT_A));
            DcbAppendResult decided = dcbEventStore().append(List.of(taggedEventWithId("decided", WITHDRAWN, ACCOUNT_A)),
                    failIfEventsMatch(dcbEventStore().read(boundary).consistencyToken()));
            DcbMarkerVersions saved = decided.markerVersions().orElseThrow(() -> new AssertionError(
                    "A conditional append with a consistency token over a tag boundary must report its marker versions"));
            dcbEventStore().append(List.of(taggedEventWithId("later", DEPOSITED, ACCOUNT_A)));

            assertThat(idsOf(dcbEventStore().read(boundary, DcbReadOptions.afterVersions(saved)).events()))
                    .as("A decider saves the versions its own append reported, and has applied that append already")
                    .containsExactly("later");
        }

        @Test
        void returns_an_event_of_the_boundary_that_an_unconditional_append_added() {
            DcbCriteria boundary = DcbCriteria.tags(tag(ACCOUNT_A));
            DcbAppendResult decided = dcbEventStore().append(List.of(taggedEventWithId("decided", WITHDRAWN, ACCOUNT_A)),
                    failIfEventsMatch(dcbEventStore().read(boundary).consistencyToken()));
            DcbMarkerVersions saved = decided.markerVersions().orElseThrow();

            dcbEventStore().append(List.of(taggedEventWithId("unconditional", DEPOSITED, ACCOUNT_A)));

            assertThat(idsOf(dcbEventStore().read(boundary, DcbReadOptions.afterVersions(saved)).events()))
                    .containsExactly("unconditional");
        }

        @Test
        void returns_a_limited_number_of_the_events_after_the_versions_when_it_has_a_limit() {
            DcbCriteria boundary = DcbCriteria.tags(tag(ACCOUNT_A));
            dcbEventStore().append(List.of(taggedEventWithId("before", DEPOSITED, ACCOUNT_A)));
            DcbMarkerVersions saved = versionsOf(dcbEventStore().read(boundary, DcbReadOptions.fromBeginning().withVersions()));
            dcbEventStore().append(List.of(taggedEventWithId("after-1", DEPOSITED, ACCOUNT_A)));
            dcbEventStore().append(List.of(taggedEventWithId("after-2", DEPOSITED, ACCOUNT_A)));

            DcbEventStream limited = dcbEventStore().read(boundary, DcbReadOptions.afterVersions(saved).limit(1));

            assertThat(idsOf(limited.events())).containsExactly("after-1");
            assertThat(limited.markerVersions())
                    .as("A read that stopped early must not report versions it did not read up to")
                    .isEmpty();
        }
    }

    @Nested
    @DisplayName("a read after versions the store cannot compare with its own")
    class AReadAfterVersionsTheStoreCannotCompareWithItsOwn {

        @Test
        void is_refused_for_criteria_that_match_every_event() {
            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)));
            DcbMarkerVersions saved = versionsOf(dcbEventStore().read(DcbCriteria.tags(tag(ACCOUNT_A)), DcbReadOptions.fromBeginning().withVersions()));

            Throwable thrown = catchThrowable(() -> dcbEventStore().read(DcbCriteria.all(), DcbReadOptions.afterVersions(saved)));

            assertThat(thrown).isInstanceOfSatisfying(DcbSnapshotInvalidException.class, refusal -> {
                assertThat(refusal.criteria()).isEqualTo(DcbCriteria.all());
                assertThat(refusal.afterVersions()).isEqualTo(saved);
            });
        }

        @Test
        void is_refused_when_the_saved_versions_lack_a_marker_key_of_the_criteria() {
            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A, ACCOUNT_B)));
            DcbMarkerVersions onlyA = versionsOf(dcbEventStore().read(DcbCriteria.tags(tag(ACCOUNT_A)), DcbReadOptions.fromBeginning().withVersions()));

            assertRefused(DcbCriteria.tags(tag(ACCOUNT_A), tag(ACCOUNT_B)), onlyA)
                    .hasMessageContaining(KEY_B);
        }

        @Test
        void is_refused_when_the_saved_versions_have_a_marker_key_the_criteria_does_not_have() {
            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A, ACCOUNT_B)));
            DcbMarkerVersions bothKeys = versionsOf(dcbEventStore().read(DcbCriteria.tags(tag(ACCOUNT_A), tag(ACCOUNT_B)), DcbReadOptions.fromBeginning().withVersions()));

            assertRefused(DcbCriteria.tags(tag(ACCOUNT_A)), bothKeys)
                    .hasMessageContaining(KEY_B);
        }

        @Test
        void is_refused_when_the_saved_versions_cover_a_type_where_the_criteria_has_a_tag() {
            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)));
            DcbMarkerVersions ofType = versionsOf(dcbEventStore().read(DcbCriteria.type(DEPOSITED), DcbReadOptions.fromBeginning().withVersions()));

            assertRefused(DcbCriteria.tags(tag(ACCOUNT_A)), ofType);
        }

        @Test
        void is_refused_when_a_saved_version_is_above_the_version_of_the_store() {
            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)));
            DcbCriteria boundary = DcbCriteria.tags(tag(ACCOUNT_A));
            DcbMarkerVersions aheadOfTheStore = changed(versionsOf(dcbEventStore().read(boundary, DcbReadOptions.fromBeginning().withVersions())),
                    marker -> new MarkerVersion(marker.epoch(), marker.version() + 1, marker.unstamped()));

            assertRefused(boundary, aheadOfTheStore).hasMessageContaining(KEY_A);
        }

        @Test
        void is_refused_when_a_saved_epoch_is_not_the_epoch_of_the_marker() {
            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)));
            DcbCriteria boundary = DcbCriteria.tags(tag(ACCOUNT_A));
            DcbMarkerVersions ofAnotherIncarnation = changed(versionsOf(dcbEventStore().read(boundary, DcbReadOptions.fromBeginning().withVersions())),
                    marker -> new MarkerVersion(marker.epoch() + "-removed-and-created-again", marker.version(), marker.unstamped()));

            assertRefused(boundary, ofAnotherIncarnation).hasMessageContaining(KEY_A);
        }

        @Test
        void is_refused_when_a_saved_unstamped_count_is_not_the_one_of_the_marker() {
            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)));
            DcbCriteria boundary = DcbCriteria.tags(tag(ACCOUNT_A));
            DcbMarkerVersions ofAnotherCount = changed(versionsOf(dcbEventStore().read(boundary, DcbReadOptions.fromBeginning().withVersions())),
                    marker -> new MarkerVersion(marker.epoch(), marker.version(), marker.unstamped() == 0 ? 1 : 0));

            assertRefused(boundary, ofAnotherCount).hasMessageContaining(KEY_A);
        }

        @Test
        void is_refused_in_the_same_way_whatever_the_options_that_follow_the_versions() {
            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)));
            DcbCriteria boundary = DcbCriteria.tags(tag(ACCOUNT_A));
            DcbMarkerVersions ahead = changed(versionsOf(dcbEventStore().read(boundary, DcbReadOptions.fromBeginning().withVersions())),
                    marker -> new MarkerVersion(marker.epoch(), marker.version() + 1, marker.unstamped()));

            for (DcbReadOptions options : List.of(DcbReadOptions.afterVersions(ahead).limit(1), DcbReadOptions.afterVersions(ahead).skip(1),
                    DcbReadOptions.afterVersions(ahead).backwards())) {
                assertThat(catchThrowable(() -> dcbEventStore().read(boundary, options)))
                        .as("Reading after versions the store cannot compare is refused, not answered with a partial read")
                        .isInstanceOf(DcbSnapshotInvalidException.class);
            }
        }

        @Test
        void names_the_criteria_and_the_saved_versions_of_the_read_it_refused() {
            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)));
            DcbCriteria boundary = DcbCriteria.tags(tag(ACCOUNT_A));
            DcbMarkerVersions ahead = changed(versionsOf(dcbEventStore().read(boundary, DcbReadOptions.fromBeginning().withVersions())),
                    marker -> new MarkerVersion(marker.epoch(), marker.version() + 1, marker.unstamped()));

            Throwable thrown = catchThrowable(() -> dcbEventStore().read(boundary, DcbReadOptions.afterVersions(ahead)));

            assertThat(thrown).isInstanceOfSatisfying(DcbSnapshotInvalidException.class, refusal -> {
                assertThat(refusal.criteria()).isEqualTo(boundary);
                assertThat(refusal.afterVersions()).isEqualTo(ahead);
            });
        }

        @Test
        void checks_for_criteria_that_match_every_event_before_it_looks_at_the_saved_versions() {
            DcbMarkerVersions nothingTheStoreHas = DcbMarkerVersions.of(Map.of("tag:unknown", new MarkerVersion("unknown-epoch", 1, 0)));

            Throwable thrown = catchThrowable(() -> dcbEventStore().read(DcbCriteria.all(), DcbReadOptions.afterVersions(nothingTheStoreHas)));

            assertThat(thrown).isInstanceOf(DcbSnapshotInvalidException.class).hasMessageContaining("every event");
        }

        @Test
        void checks_the_saved_marker_keys_before_the_epochs() {
            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A, ACCOUNT_B)));
            DcbMarkerVersions onlyAWithAnotherEpoch = changed(
                    versionsOf(dcbEventStore().read(DcbCriteria.tags(tag(ACCOUNT_A)), DcbReadOptions.fromBeginning().withVersions())),
                    marker -> new MarkerVersion(marker.epoch() + "-removed-and-created-again", marker.version(), marker.unstamped()));

            assertThat(catchThrowable(() -> dcbEventStore().read(DcbCriteria.tags(tag(ACCOUNT_A), tag(ACCOUNT_B)), DcbReadOptions.afterVersions(onlyAWithAnotherEpoch))))
                    .isInstanceOf(DcbSnapshotInvalidException.class)
                    .hasMessageContaining(KEY_B)
                    .hasMessageNotContaining("epoch");
        }

        @Test
        void checks_the_epoch_before_the_version() {
            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)));
            DcbCriteria boundary = DcbCriteria.tags(tag(ACCOUNT_A));
            DcbMarkerVersions anotherEpochAndAhead = changed(versionsOf(dcbEventStore().read(boundary, DcbReadOptions.fromBeginning().withVersions())),
                    marker -> new MarkerVersion(marker.epoch() + "-removed-and-created-again", marker.version() + 1, marker.unstamped()));

            assertThat(catchThrowable(() -> dcbEventStore().read(boundary, DcbReadOptions.afterVersions(anotherEpochAndAhead))))
                    .isInstanceOf(DcbSnapshotInvalidException.class)
                    .hasMessageContaining("epoch");
        }

        @Test
        void checks_the_version_before_the_unstamped_count() {
            dcbEventStore().append(List.of(taggedEvent(DEPOSITED, ACCOUNT_A)));
            DcbCriteria boundary = DcbCriteria.tags(tag(ACCOUNT_A));
            DcbMarkerVersions aheadAndAnotherCount = changed(versionsOf(dcbEventStore().read(boundary, DcbReadOptions.fromBeginning().withVersions())),
                    marker -> new MarkerVersion(marker.epoch(), marker.version() + 1, marker.unstamped() + 1));

            assertThat(catchThrowable(() -> dcbEventStore().read(boundary, DcbReadOptions.afterVersions(aheadAndAnotherCount))))
                    .isInstanceOf(DcbSnapshotInvalidException.class)
                    .hasMessageContaining("above")
                    .hasMessageNotContaining("writer");
        }

        private org.assertj.core.api.AbstractThrowableAssert<?, ? extends Throwable> assertRefused(DcbCriteria criteria, DcbMarkerVersions saved) {
            Throwable thrown = catchThrowable(() -> dcbEventStore().read(criteria, DcbReadOptions.afterVersions(saved)));
            return assertThat(thrown)
                    .as("A read after versions the store cannot compare with its own must be refused, so the caller reads in full")
                    .isInstanceOf(DcbSnapshotInvalidException.class);
        }
    }

    @Nested
    @DisplayName("the events a read returns")
    class TheEventsAReadReturns {

        @Test
        void carry_no_marker_versions_extension_whichever_way_they_were_read() {
            DcbCriteria boundary = DcbCriteria.tags(tag(ACCOUNT_A));
            dcbEventStore().append(List.of(taggedEventWithId("before", DEPOSITED, ACCOUNT_A)));
            DcbMarkerVersions saved = versionsOf(dcbEventStore().read(boundary, DcbReadOptions.fromBeginning().withVersions()));
            dcbEventStore().append(List.of(taggedEventWithId("after", DEPOSITED, ACCOUNT_A)),
                    failIfEventsMatch(dcbEventStore().read(boundary).consistencyToken()));

            List<CloudEvent> events = new ArrayList<>();
            events.addAll(dcbEventStore().read(boundary).events());
            events.addAll(dcbEventStore().read(boundary, DcbReadOptions.fromBeginning().withVersions()).events());
            events.addAll(dcbEventStore().read(boundary, DcbReadOptions.afterVersions(saved)).events());
            events.addAll(dcbEventStore().read(DcbCriteria.all()).events());

            assertThat(events).as("The reads must have returned events, or they prove nothing").hasSize(2 + 2 + 1 + 2);
            for (CloudEvent event : events) {
                assertThat(event.getExtensionNames())
                        .as("Marker versions belong to the store and a caller must not see them on event %s", event.getId())
                        .doesNotContain("mv");
                assertThat(extension(event, "mv")).isNull();
            }
        }
    }

    private static DcbMarkerVersions versionsOf(DcbEventStream read) {
        return read.markerVersions().orElseThrow(() -> new AssertionError(
                "A full forward read that asks for versions must report them, so the test has versions to resume from. "
                        + "The read reported none"));
    }

    private static DcbMarkerVersions changed(DcbMarkerVersions versions, UnaryOperator<MarkerVersion> change) {
        Map<String, MarkerVersion> changed = new TreeMap<>();
        versions.markers().forEach((key, marker) -> changed.put(key, change.apply(marker)));
        return DcbMarkerVersions.of(changed);
    }

    private static CloudEvent untaggedWithId(String id, String type) {
        return DcbCloudEvents.withTags(ConformanceEvents.event(id, type), Set.of());
    }
}
