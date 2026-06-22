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

package org.occurrent.dsl.dcb.blocking;

import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;

import java.util.List;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Domain-event result of a DCB read.
 *
 * @param events the domain events matched by the DCB query
 * @param lastSequencePosition the latest DCB sequence position observed by the read, or {@code 0} when none has been observed. A global cursor, not a safe optimistic-concurrency boundary on its own
 * @param consistencyToken the optimistic-concurrency boundary observed by the read. Pass it to {@link DcbAppendCondition#failIfEventsMatch(org.occurrent.eventstore.api.dcb.DcbQuery, DcbConsistencyToken)} for a conditional append
 */
@NullMarked
public record DcbDomainEventStream<E>(List<E> events, long lastSequencePosition, DcbConsistencyToken consistencyToken) {

    public DcbDomainEventStream {
        requireNonNull(events, "Events cannot be null");
        requireNonNull(consistencyToken, "Consistency token cannot be null");
        if (lastSequencePosition < 0) {
            throw new IllegalArgumentException("Last sequence position cannot be negative");
        }
        events = List.copyOf(events);
    }

    /**
     * Convenience for callers that do not need the optimistic-concurrency token, defaulting it to the position.
     */
    public DcbDomainEventStream(List<E> events, long lastSequencePosition) {
        this(events, lastSequencePosition, DcbConsistencyToken.of(lastSequencePosition));
    }

    /**
     * Streams the domain events returned by the read.
     */
    public Stream<E> stream() {
        return events.stream();
    }
}
