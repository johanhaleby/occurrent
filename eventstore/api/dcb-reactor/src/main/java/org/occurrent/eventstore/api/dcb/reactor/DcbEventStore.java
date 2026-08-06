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

package org.occurrent.eventstore.api.dcb.reactor;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.dcb.*;
import reactor.core.publisher.Mono;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Reactive event store operations for Dynamic Consistency Boundary reads and appends. This is the
 * <a href="https://projectreactor.io/">project reactor</a> counterpart of the blocking
 * {@link org.occurrent.eventstore.api.dcb.DcbEventStore}, returning {@link Mono} instead of plain values.
 * <p>
 * DCB is an optional capability over shared CloudEvent storage. Implementations keep storing CloudEvents in Occurrent
 * streams while exposing reads, append conditions, tags, and sequence positions in DCB terms. The DCB value types
 * ({@link DcbCriteria}, {@link DcbReadOptions}, {@link DcbEventStream}, {@link DcbAppendCondition}, {@link DcbAppendResult})
 * are shared with the blocking API.
 */
@NullMarked
public interface DcbEventStore {

    /**
     * Reads all events that match {@code criteria} from the beginning of the DCB sequence.
     */
    default Mono<DcbEventStream> read(DcbCriteria criteria) {
        return read(criteria, DcbReadOptions.fromBeginning());
    }

    /**
     * Reads events that match {@code criteria} using the supplied read options.
     */
    Mono<DcbEventStream> read(DcbCriteria criteria, DcbReadOptions options);

    /**
     * Returns whether any DCB event in the store matches {@code criteria}.
     */
    default Mono<Boolean> exists(DcbCriteria criteria) {
        return exists(criteria, DcbReadOptions.fromBeginning());
    }

    /**
     * Returns whether any DCB event matches {@code criteria} within the position range of {@code options}.
     * Direction, skip, and limit are ignored.
     * <p>
     * The default implementation reads the matching events. Implementations should override it with a more efficient
     * existence check.
     */
    default Mono<Boolean> exists(DcbCriteria criteria, DcbReadOptions options) {
        requireNonNull(criteria, "Criteria cannot be null");
        requireNonNull(options, "Read options cannot be null");
        return read(criteria, new DcbReadOptions(options.positionRange())).map(stream -> !stream.events().isEmpty());
    }

    /**
     * Returns the number of DCB events in the store that match {@code criteria}.
     */
    default Mono<Long> count(DcbCriteria criteria) {
        return count(criteria, DcbReadOptions.fromBeginning());
    }

    /**
     * Returns the number of DCB events matching {@code criteria} within the position range of {@code options}.
     * Direction, skip, and limit are ignored.
     * <p>
     * The default implementation reads the matching events. Implementations should override it with a more efficient
     * count.
     */
    default Mono<Long> count(DcbCriteria criteria, DcbReadOptions options) {
        requireNonNull(criteria, "Criteria cannot be null");
        requireNonNull(options, "Read options cannot be null");
        return read(criteria, new DcbReadOptions(options.positionRange())).map(stream -> (long) stream.events().size());
    }

    /**
     * Appends DCB-tagged CloudEvents without an additional DCB condition.
     * <p>
     * The Occurrent storage stream the events are placed in is derived by the store from the events' DCB tags, so
     * callers reason in DCB terms (tags and append conditions) rather than in storage stream ids.
     */
    Mono<DcbAppendResult> append(List<CloudEvent> events);

    /**
     * Appends DCB-tagged CloudEvents if {@code condition} is fulfilled.
     * <p>
     * The Occurrent storage stream the events are placed in is derived by the store from the events' DCB tags.
     */
    Mono<DcbAppendResult> append(List<CloudEvent> events, DcbAppendCondition condition);
}
