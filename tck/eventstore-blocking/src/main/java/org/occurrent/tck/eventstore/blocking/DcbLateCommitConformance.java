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
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.tck.latecommit.LateAppend;
import org.occurrent.tck.latecommit.LateCommitGuarantee;
import org.occurrent.tck.latecommit.LateCommits;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.tck.ConformanceEvents.idsOf;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.tag;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.taggedEventWithId;

/**
 * What a DCB caller sees when an append is still in flight. One append, A, has been assigned a position and has not
 * committed yet. A later append, B, is assigned the next position and commits first. A caller that saw B and resumes
 * from the marker versions B reported must still get A once A commits, or A is lost to it for good.
 * <p>
 * The two resume patterns are the ones the DCB helpers use. A decider appends and takes the marker versions from its
 * own {@link DcbAppendResult}. A view reads with {@link DcbReadOptions#withVersions()} and takes them from the
 * {@link DcbEventStream}. Both resume with {@link DcbReadOptions#afterVersions(DcbMarkerVersions)}, never from a
 * position. A third test checks that the consistency token of a read taken while A is in flight still refuses a
 * condition A matches.
 * <p>
 * Every test holds A through {@link EventStoreFixture#lateCommits()}. A store that commits in position order cannot
 * have A in flight, so its tests run the same steps with A already committed, where the guarantee holds by
 * construction. The suite checks each answer, see {@link LateCommits}, so neither can pass without testing what it
 * claims to.
 * <p>
 * Assertions only use what a caller sees: append results, read results and tokens. They state the guarantee and nothing
 * about how a store keeps it.
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the DCB late-commit contract")
public abstract class DcbLateCommitConformance extends LateCommitConformance {

    private static final String ACCOUNT = "account:a1";
    private static final String DEPOSITED = "Deposited";
    private static final String WITHDRAWN = "Withdrawn";

    @Override
    protected final Set<EventStoreCapability> requiredCapabilities() {
        return Set.of(EventStoreCapability.DCB);
    }

    @Test
    @Timeout(120)
    void a_decider_that_resumes_from_its_own_append_does_not_lose_an_append_that_was_still_in_flight() {
        DcbCriteria boundary = DcbCriteria.tags(tag(ACCOUNT));
        CloudEvent inFlight = taggedEventWithId("in-flight", DEPOSITED, ACCOUNT);
        CloudEvent overtaking = taggedEventWithId("overtaking", WITHDRAWN, ACCOUNT);

        try (LateAppend<DcbAppendResult> late = LateAppend.dcbAppend(lateCommits(), dcbEventStore(),
                List.of(inFlight), () -> isVisible(boundary, inFlight))) {
            DcbEventStream decidedOn = dcbEventStore().read(boundary);
            DcbAppendResult overtook = dcbEventStore().append(List.of(overtaking),
                    failIfEventsMatch(decidedOn.consistencyToken()));
            DcbMarkerVersions resumeFrom = overtook.markerVersions().orElseThrow(() -> new AssertionError(
                    "A conditional append with a consistency token over a tag boundary must report its marker versions, "
                            + "so a decider has the versions to resume from. The append reported none"));

            DcbAppendResult committedLate = late.release();
            late.requireWindowEntered(committedLate.firstSequencePosition(), overtook.firstSequencePosition());

            DcbEventStream afterResume = dcbEventStore().read(boundary, DcbReadOptions.afterVersions(resumeFrom));

            Set<String> seen = new LinkedHashSet<>(idsOf(decidedOn.events()));
            seen.add(overtaking.getId());
            seen.addAll(idsOf(afterResume.events()));
            LateCommitGuarantee.require(seen.contains(inFlight.getId()),
                    "the append that was in flight is in what the decider read before it appended, in its own append, "
                            + "or in a read after the marker versions its append reported",
                    () -> "the decider saw " + seen + ", resumed from marker versions " + resumeFrom
                            + " and the in-flight append committed at position " + committedLate.firstSequencePosition());
        }
    }

    @Test
    @Timeout(120)
    void a_view_that_resumes_from_a_read_does_not_lose_an_append_that_was_still_in_flight() {
        DcbCriteria boundary = DcbCriteria.tags(tag(ACCOUNT));
        CloudEvent inFlight = taggedEventWithId("in-flight", DEPOSITED, ACCOUNT);
        CloudEvent overtaking = taggedEventWithId("overtaking", WITHDRAWN, ACCOUNT);

        try (LateAppend<DcbAppendResult> late = LateAppend.dcbAppend(lateCommits(), dcbEventStore(),
                List.of(inFlight), () -> isVisible(boundary, inFlight))) {
            DcbAppendResult overtook = dcbEventStore().append(List.of(overtaking));
            DcbEventStream firstRead = dcbEventStore().read(boundary, DcbReadOptions.fromBeginning().withVersions());
            DcbMarkerVersions resumeFrom = firstRead.markerVersions().orElseThrow(() -> new AssertionError(
                    "A full forward read of a tag boundary that asks for versions must report its marker versions, "
                            + "so a view has the versions to resume from. The read reported none"));

            DcbAppendResult committedLate = late.release();
            late.requireWindowEntered(committedLate.firstSequencePosition(), overtook.firstSequencePosition());

            DcbEventStream secondRead = dcbEventStore().read(boundary, DcbReadOptions.afterVersions(resumeFrom));

            Set<String> seen = new LinkedHashSet<>(idsOf(firstRead.events()));
            seen.addAll(idsOf(secondRead.events()));
            LateCommitGuarantee.require(seen.contains(inFlight.getId()),
                    "the append that was in flight is in the first read or in the read after the marker versions the "
                            + "first read reported",
                    () -> "the view saw " + seen + ", resumed from marker versions " + resumeFrom
                            + " and the in-flight append committed at position " + committedLate.firstSequencePosition());
        }
    }

    @Test
    @Timeout(120)
    void a_consistency_token_read_while_an_append_was_in_flight_still_refuses_a_condition_that_append_matches() {
        DcbCriteria boundary = DcbCriteria.tags(tag(ACCOUNT));
        CloudEvent inFlight = taggedEventWithId("in-flight", DEPOSITED, ACCOUNT);
        CloudEvent decided = taggedEventWithId("decided", WITHDRAWN, ACCOUNT);

        try (LateAppend<DcbAppendResult> late = LateAppend.dcbAppend(lateCommits(), dcbEventStore(),
                List.of(inFlight), () -> isVisible(boundary, inFlight))) {
            DcbEventStream read = dcbEventStore().read(boundary);
            late.requirePositionReserved(read.lastSequencePosition());
            boolean readSawInFlight = idsOf(read.events()).contains(inFlight.getId());

            late.release();

            Throwable thrown = catchThrowable(() ->
                    dcbEventStore().append(List.of(decided), failIfEventsMatch(read.consistencyToken())));
            if (thrown != null) {
                assertThat(thrown)
                        .describedAs("a refused append must say the condition was not fulfilled")
                        .isInstanceOf(DcbAppendConditionNotFulfilledException.class);
            }
            LateCommitGuarantee.require(readSawInFlight || thrown != null,
                    "an append that decided on a read without the in-flight event is refused once that event has "
                            + "committed, because the event matches the condition",
                    () -> "the append was accepted although the read did not contain the in-flight event");
        }
    }

    private boolean isVisible(DcbCriteria boundary, CloudEvent event) {
        return idsOf(dcbEventStore().read(boundary).events()).contains(event.getId());
    }
}
