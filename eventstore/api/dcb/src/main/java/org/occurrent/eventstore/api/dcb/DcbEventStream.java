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
import org.occurrent.cloudevents.OccurrentCloudEventExtension;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Immutable result of a DCB read.
 *
 * @param events the CloudEvents matched by the DCB query and read options
 * @param lastSequencePosition the value of the event store's position counter as this read saw it. A write that
 *                             doesn't commit can raise it. This is NOT the highest position among the matched
 *                             {@code events}: that can be lower, and when the query matches nothing there is no matched position at all, yet
 *                             {@code lastSequencePosition} still reports the counter. Inside a transaction it can be
 *                             below what the store has reserved by then. It is {@code 0} when the read saw no
 *                             reservation. It isn't a safe place to resume from on MongoDB. To read on
 *                             from a read that reports versions, use {@link DcbReadOptions#withVersions()} and
 *                             {@link DcbReadOptions#afterVersions(DcbMarkerVersions)}, or a catch-up subscription's
 *                             checkpoint. It is NOT a safe optimistic-concurrency boundary on its
 *                             own: a store that assigns positions before the events commit can report a value that is
 *                             ahead of the data a reader can actually see. Use {@code consistencyToken} for optimistic
 *                             concurrency.
 * @param consistencyToken     the token a conditional append is checked against. It holds the criteria the read was
 *                             given, so pass it unchanged to
 *                             {@link DcbAppendCondition#failIfEventsMatch(DcbConsistencyToken)} and the append is
 *                             refused if an event matching those criteria was committed after this read. For a token read with
 *                             {@link DcbCriteria#all()}, the MongoDB stores don't refuse the append for an event
 *                             committed after the read by an append with no condition on {@code all()}, which
 *                             {@link DcbAppendCondition#wholeStoreLock(DcbConsistencyToken)} describes. A store can
 *                             also refuse the append when no matching event was committed, and a marker prune can
 *                             make a MongoDB store let a conflicting one through, both of which
 *                             {@link DcbAppendCondition} describes. Unlike
 *                             {@code lastSequencePosition}, it protects an append while other appends are still
 *                             committing, because the store derives it from data a reader can only see once it's
 *                             committed. Use it only with the store that returned it.
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
     * Streams the events returned by the read.
     */
    public Stream<CloudEvent> stream() {
        return events.stream();
    }

    /**
     * Whether an event returned by the read has a position below {@code position}. A read after marker versions can
     * return one, since a store can assign a position before the event commits.
     */
    public boolean hasEventBelow(long position) {
        return events.stream().anyMatch(event -> OccurrentCloudEventExtension.getPosition(event) < position);
    }

    /**
     * The highest position among the events returned by the read, or {@code 0} when it returned none. It isn't
     * {@link #lastSequencePosition()}, which is the store's position counter as the read saw it.
     */
    public long highestEventPosition() {
        return events.stream().mapToLong(OccurrentCloudEventExtension::getPosition).max().orElse(0);
    }
}
