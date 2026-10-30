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

package org.occurrent.tck.internal.catchup;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.api.blocking.internal.CatchupReadScope;
import org.occurrent.eventstore.api.blocking.internal.CatchupReads;
import org.occurrent.filter.Filter;
import org.occurrent.tck.eventstore.blocking.EventStoreConformance;
import org.occurrent.tck.latecommit.LateAppend;
import org.occurrent.tck.latecommit.LateCommitGuarantee;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.tck.ConformanceEvents.event;
import static org.occurrent.tck.ConformanceEvents.idsOf;

/**
 * What a catch-up is delivered when a write is still in flight below the head its scope reads. One write, A, has been
 * assigned a position and has not committed when the scope opens. A later write, B, has committed. The catch-up replays
 * what the scope reads, then follows the change stream from the scope's live start, so A has to be in one of the two
 * once it commits.
 * <p>
 * Not part of the TCK contract, see the package. The store under test has to implement {@link CatchupReads}, and the
 * suite needs a way to read its change stream from a live start, which only the implementing module can supply.
 * <p>
 * The test reads the live start after A has committed, the way a catch-up reads it after its replay. A live start that
 * was worked out then, rather than when the scope opened, would lie above A and A would be in neither half.
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the catch-up read scope's live start")
public abstract class CatchupReadScopeLateCommitConformance extends EventStoreConformance {

    private static final String DEFINED = "NameDefined";
    private static final String HELD_STREAM = "stream:held";
    private static final String OVERTAKING_STREAM = "stream:overtaking";
    private static final String SENTINEL_STREAM = "stream:sentinel";

    /**
     * The ids of the events the change stream reports from {@code liveStart}, in order, up to and including the event
     * with id {@code lastEventId}. Fails if that event does not arrive.
     */
    protected abstract List<String> idsInChangeStreamFrom(String liveStart, String lastEventId);

    @Override
    protected final Set<EventStoreCapability> requiredCapabilities() {
        return Set.of(EventStoreCapability.STREAM);
    }

    @BeforeEach
    final void touchTheLateCommits() {
        fixture().lateCommits();
    }

    @Test
    @Timeout(120)
    void the_scope_and_the_change_stream_from_its_live_start_together_deliver_a_write_that_was_still_in_flight() {
        CloudEvent inFlight = event("in-flight", DEFINED);
        CloudEvent overtaking = event("overtaking", DEFINED);
        CloudEvent sentinel = event("sentinel", DEFINED);
        assertThat(eventStore())
                .describedAs("the store under test hands its catch-up the scope through %s", CatchupReads.class.getSimpleName())
                .isInstanceOf(CatchupReads.class);

        try (LateAppend<WriteResult> late = LateAppend.streamWrite(fixture().lateCommits(), eventStore(), HELD_STREAM,
                List.of(inFlight), () -> isVisible(HELD_STREAM, inFlight))) {
            eventStore().write(OVERTAKING_STREAM, List.of(overtaking));

            try (CatchupReadScope scope = ((CatchupReads) eventStore()).openCatchupReadScope(null)) {
                List<String> replayed = idsInPositionOrder(scope, PositionRange.upToPosition(scope.currentPosition()));

                late.release();
                late.requireWindowEntered(positionOf(HELD_STREAM, inFlight), positionOf(OVERTAKING_STREAM, overtaking));
                scope.currentPosition();
                eventStore().write(SENTINEL_STREAM, List.of(sentinel));

                String liveStart = scope.liveStart();
                assertThat(liveStart)
                        .describedAs("a store that implements %s reports the live start of the scope it opened", CatchupReads.class.getSimpleName())
                        .isNotNull();
                List<String> live = idsInChangeStreamFrom(liveStart, sentinel.getId());

                Set<String> delivered = new LinkedHashSet<>(replayed);
                delivered.addAll(live);
                LateCommitGuarantee.require(delivered.contains(inFlight.getId()),
                        "the write that was in flight is read by the scope or reported by the change stream from the scope's live start",
                        () -> "the scope read " + replayed + " and the change stream from " + liveStart + " reported " + live);
                assertThat(delivered)
                        .describedAs("the write that overtook it is delivered too")
                        .contains(overtaking.getId());
            }
        }
    }

    private static List<String> idsInPositionOrder(CatchupReadScope scope, PositionRange range) {
        try (Stream<CloudEvent> events = scope.readInPositionOrder(Filter.all(), range)) {
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
