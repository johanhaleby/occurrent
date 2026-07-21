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
 * A {@link Projection} paired with the {@link DcbCriteria} that selects which events feed it, usually a tag filter such
 * as {@code tags("username:bob")}. The read-side mirror of {@code org.occurrent.dsl.dcb.DcbDecider}. The handlers drive
 * the fold, the criteria drives the read.
 * <p>
 * A single-instance projection parameterized by a key (for example {@code isUsernameClaimedProjection("bob")}) is built
 * with {@code dcbSingletonProjection} and closes over the key for its {@code criteria}.
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
        if (projection.filter() != null) {
            throw new IllegalArgumentException("projection has an explicit filter, but a DcbProjection is read through its DCB criteria, not the wrapped projection's filter; "
                    + "the filter would silently be ignored, so build the projection without one");
        }
    }
}
