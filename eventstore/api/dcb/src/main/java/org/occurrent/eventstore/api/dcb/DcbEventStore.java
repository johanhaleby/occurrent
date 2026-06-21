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

import static java.util.Objects.requireNonNull;

/**
 * Event store operations for Dynamic Consistency Boundary reads and appends.
 * <p>
 * DCB is an optional capability over shared CloudEvent storage. Implementations keep
 * storing CloudEvents in Occurrent streams while exposing reads, append conditions,
 * tags, and sequence positions in DCB terms.
 */
@NullMarked
public interface DcbEventStore {

    /**
     * Reads all events that match {@code query} from the beginning of the DCB sequence.
     */
    default DcbEventStream read(DcbQuery query) {
        return read(query, DcbReadOptions.fromBeginning());
    }

    /**
     * Reads events that match {@code query} using the supplied read options.
     */
    DcbEventStream read(DcbQuery query, DcbReadOptions options);

    /**
     * Returns whether any DCB event in the store matches {@code query}.
     * <p>
     * The default implementation reads the matching events; implementations should override it with a more
     * efficient existence check.
     */
    default boolean exists(DcbQuery query) {
        requireNonNull(query, "Query cannot be null");
        return !read(query).events().isEmpty();
    }

    /**
     * Returns the number of DCB events in the store that match {@code query}.
     * <p>
     * The default implementation reads the matching events; implementations should override it with a more
     * efficient count.
     */
    default long count(DcbQuery query) {
        requireNonNull(query, "Query cannot be null");
        return read(query).events().size();
    }

    /**
     * Appends DCB-tagged CloudEvents without an additional DCB condition.
     * <p>
     * The Occurrent storage stream the events are placed in is derived by the store from the events' DCB tags, so
     * callers reason in DCB terms (tags and append conditions) rather than in storage stream ids.
     */
    DcbAppendResult append(List<CloudEvent> events);

    /**
     * Appends DCB-tagged CloudEvents if {@code condition} is fulfilled.
     * <p>
     * The Occurrent storage stream the events are placed in is derived by the store from the events' DCB tags.
     */
    DcbAppendResult append(List<CloudEvent> events, DcbAppendCondition condition);
}
