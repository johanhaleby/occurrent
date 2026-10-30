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

package org.occurrent.eventstore.api.dcb;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Immutable result of a DCB read.
 *
 * @param events the CloudEvents matched by the DCB query and read options
 * @param lastSequencePosition the event store's global DCB head at the time of the read, that is the highest DCB
 *                             sequence position assigned anywhere in the store. This is NOT the highest position among
 *                             the matched {@code events}: the highest matched position can be lower than the head, and
 *                             when the query matches nothing there is no matched position at all, yet
 *                             {@code lastSequencePosition} still reports the store head. It is {@code 0} only when the
 *                             store holds no DCB events yet. It is a monotonic global cursor, suitable for replay and
 *                             catch-up, but it is NOT a safe optimistic-concurrency boundary on its own: a store that
 *                             assigns positions before the events commit can report a head that is ahead of the data a
 *                             reader can actually see. Use {@code consistencyToken} for optimistic concurrency.
 * @param consistencyToken     an opaque, store-defined token capturing the consistency boundary observed by this read
 *                             for the supplied query. Pass it unchanged to
 *                             {@link DcbAppendCondition#failIfEventsMatch(DcbCriteria, DcbConsistencyToken)} so a later
 *                             append is rejected if any event matching the query was committed after this read. Unlike
 *                             {@code lastSequencePosition}, it is sound under concurrent in-flight appends because the
 *                             store derives it from data that is only observable once committed. Round-trip it within
 *                             the same store; do not interpret or compare it across stores.
 * @param markerVersions       the versions of the conflict markers of the read's criteria, in the same point-in-time
 *                             view the events came from. Present only when the read asked for them with
 *                             {@link DcbReadOptions#withVersions()} or {@link DcbReadOptions#afterVersions(DcbMarkerVersions)},
 *                             had no limit, no skip, no position bound and didn't read backwards, had criteria other than
 *                             {@link DcbCriteria.MatchAll}, and every marker of the criteria exists and has an epoch. A
 *                             store that can't take such a view leaves it empty. Every constructor other than the
 *                             canonical one leaves it empty.
 */
@NullMarked
public record DcbEventStream(List<CloudEvent> events, long lastSequencePosition, DcbConsistencyToken consistencyToken,
                             Optional<DcbMarkerVersions> markerVersions) {

    public DcbEventStream {
        requireNonNull(events, "Events cannot be null");
        requireNonNull(consistencyToken, "Consistency token cannot be null");
        requireNonNull(markerVersions, "Marker versions cannot be null");
        if (lastSequencePosition < 0) {
            throw new IllegalArgumentException("Last sequence position cannot be negative");
        }
        events = List.copyOf(events);
    }

    /**
     * Builds a stream with no marker versions. Use
     * {@link #DcbEventStream(List, long, DcbConsistencyToken, Optional)} to report them.
     */
    public DcbEventStream(List<CloudEvent> events, long lastSequencePosition, DcbConsistencyToken consistencyToken) {
        this(events, lastSequencePosition, consistencyToken, Optional.empty());
    }

    /**
     * Convenience for stores whose read head is itself a sound optimistic-concurrency boundary (for example the
     * in-memory store, whose appends assign positions and commit atomically). Such stores use the position as the
     * {@code consistencyToken}.
     */
    public DcbEventStream(List<CloudEvent> events, long lastSequencePosition) {
        this(events, lastSequencePosition, DcbConsistencyToken.of(lastSequencePosition));
    }

    /**
     * Streams the events returned by the read.
     */
    public Stream<CloudEvent> stream() {
        return events.stream();
    }
}
