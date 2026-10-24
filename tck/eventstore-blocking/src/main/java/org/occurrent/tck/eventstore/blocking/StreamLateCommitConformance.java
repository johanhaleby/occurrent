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
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.filter.Filter;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.tck.ConformanceEvents.event;
import static org.occurrent.tck.ConformanceEvents.idsOf;

/**
 * What a position-ordered reader sees when a stream write is still in flight. One write, A, has been assigned a
 * position and has not committed yet. A later write, B, is assigned the next position and commits first. A reader that
 * resumes from {@code currentPosition()} after B must still be delivered A once A commits, or A is lost to it for good.
 * That is the resume a catch-up subscription does.
 * <p>
 * The test holds A through {@link EventStoreFixture#lateCommits()}. A store that commits in position order cannot have
 * A in flight, so it runs the same steps with A already committed, where the guarantee holds by construction. The
 * suite checks each answer, see {@link LateCommits}, so neither can pass without testing what it claims to.
 * <p>
 * The assertion only uses what a reader sees: the events a position-ordered read returns on each side of the position
 * it resumed from.
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the stream late-commit contract")
public abstract class StreamLateCommitConformance extends LateCommitConformance {

    private static final String DEFINED = "NameDefined";
    private static final String HELD_STREAM = "stream:held";
    private static final String OVERTAKING_STREAM = "stream:overtaking";

    @Override
    protected final Set<EventStoreCapability> requiredCapabilities() {
        return Set.of(EventStoreCapability.STREAM);
    }

    @Test
    @Timeout(120)
    void a_reader_that_resumes_from_the_current_position_is_delivered_a_write_that_was_still_in_flight() {
        CloudEvent inFlight = event("in-flight", DEFINED);
        CloudEvent overtaking = event("overtaking", DEFINED);
        assertThat(positionOrderedReader().writesPosition())
                .describedAs("a store that does not write a position has nothing to resume from")
                .isTrue();

        try (LateAppend<WriteResult> late = LateAppend.streamWrite(lateCommits(), eventStore(), HELD_STREAM,
                List.of(inFlight), () -> isVisible(HELD_STREAM, inFlight))) {
            eventStore().write(OVERTAKING_STREAM, List.of(overtaking));
            long resumeFrom = ResumePositions.after(positionOrderedReader());
            List<String> deliveredBefore = idsInPositionOrder(PositionRange.upToPosition(resumeFrom));

            late.release();
            late.requireWindowEntered(positionOf(HELD_STREAM, inFlight), positionOf(OVERTAKING_STREAM, overtaking));

            List<String> deliveredAfter = idsInPositionOrder(PositionRange.afterPosition(resumeFrom));

            Set<String> delivered = new LinkedHashSet<>(deliveredBefore);
            delivered.addAll(deliveredAfter);
            LateCommitGuarantee.require(delivered.contains(inFlight.getId()),
                    "the write that was in flight is delivered by a read up to the position the reader resumed from "
                            + "or by a read after it",
                    () -> "the reader was delivered " + deliveredBefore + " up to position " + resumeFrom + " and "
                            + deliveredAfter + " after it");
        }
    }

    private List<String> idsInPositionOrder(PositionRange range) {
        try (Stream<CloudEvent> events = positionOrderedReader().readInPositionOrder(Filter.all(), range)) {
            return idsOf(events);
        }
    }

    private boolean isVisible(String streamId, CloudEvent event) {
        return idsOf(eventStore().read(streamId).events()).contains(event.getId());
    }

    private long positionOf(String streamId, CloudEvent event) {
        return eventStore().read(streamId).events()
                .filter(candidate -> candidate.getId().equals(event.getId()))
                .mapToLong(OccurrentCloudEventExtension::getPosition)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Event " + event.getId() + " was not found in " + streamId));
    }
}
