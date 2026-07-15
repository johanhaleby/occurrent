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

package org.occurrent.dsl.projection;

import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.dcb.DcbCriteria;

import static java.util.Objects.requireNonNull;

/**
 * A self-describing DCB (Dynamic Consistency Boundary) read model.
 * <p>
 * A {@link Projection} describes a read model in capability-agnostic terms (its fold, its id, its event types). To feed
 * it from a DCB event store, a caller must also know the {@link DcbCriteria} read boundary that selects the events for
 * the model, typically a tag filter such as {@code tags("username:bob")}. {@code DcbProjection} couples the projection
 * with that boundary, mirroring how {@code org.occurrent.dsl.dcb.DcbDecider} adds a {@code DcbCriteria} to a plain
 * {@code Decider} on the write side.
 * <p>
 * The projection's event-type handlers still drive the fold (they no-op on unrecognized events); the {@code criteria}
 * drives which events are read. A single-instance projection parameterized by a key (for example
 * {@code isUsernameClaimedProjection("bob")}) is expressed by closing over the key in the factory that builds the
 * projection and its {@code criteria}, so no per-command boundary function is needed here.
 *
 * @param projection the capability-agnostic read model
 * @param criteria   the DCB read boundary selecting the events that feed the projection
 * @param <S>        the state type
 * @param <E>        the event type
 * @param <ID>       the view-instance id type
 */
public record DcbProjection<S extends @Nullable Object, E, ID>(
        Projection<S, E, ID> projection,
        DcbCriteria criteria
) {

    public DcbProjection {
        requireNonNull(projection, "projection cannot be null");
        requireNonNull(criteria, "criteria cannot be null");
    }
}
