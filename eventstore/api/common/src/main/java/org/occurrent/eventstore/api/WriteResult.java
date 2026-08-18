/*
 *
 *  Copyright 2021 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.eventstore.api;

import org.jspecify.annotations.NullMarked;

import java.util.Objects;
import java.util.Optional;

/**
 * The result of a write operation to the event store.
 *
 * @param appendId the identifier stamped on every event this call persisted, or {@link Optional#empty()} when the
 *                 call persisted no events. A result built through the three-argument constructor is always empty
 *                 here, whether or not events were written; see {@link AppendId} for the two causes of absence.
 */
@NullMarked
public record WriteResult(String streamId, long oldStreamVersion, long newStreamVersion, Optional<AppendId> appendId) {

    public WriteResult {
        Objects.requireNonNull(streamId, "Stream id cannot be null");
        Objects.requireNonNull(appendId, "Append id cannot be null");
    }

    /**
     * Builds a result with no append id. Use {@link #WriteResult(String, long, long, Optional)} to report one.
     */
    public WriteResult(String streamId, long oldStreamVersion, long newStreamVersion) {
        this(streamId, oldStreamVersion, newStreamVersion, Optional.empty());
    }
}