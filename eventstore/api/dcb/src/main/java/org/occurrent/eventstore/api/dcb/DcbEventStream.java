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
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Immutable result of a DCB read.
 *
 * @param events the CloudEvents matched by the DCB query and read options
 * @param lastSequencePosition the latest DCB sequence position observed by the read, or {@code 0} when none has been observed
 */
@NullMarked
public record DcbEventStream(List<CloudEvent> events, long lastSequencePosition) {

    public DcbEventStream {
        requireNonNull(events, "Events cannot be null");
        if (lastSequencePosition < 0) {
            throw new IllegalArgumentException("Last sequence position cannot be negative");
        }
        events = List.copyOf(events);
    }

    /**
     * Streams the events returned by the read.
     */
    public Stream<CloudEvent> stream() {
        return events.stream();
    }
}
