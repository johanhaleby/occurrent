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

import org.jspecify.annotations.NullMarked;

/**
 * An opaque, store-defined token capturing the optimistic-concurrency boundary a DCB read observed for a query.
 * <p>
 * A {@link DcbEventStream#consistencyToken() read} produces a token; pass it unchanged to
 * {@link DcbAppendCondition#failIfEventsMatch(DcbQuery, DcbConsistencyToken)} so a later append is rejected if any event
 * matching the query was committed after the read. It is deliberately a distinct type from the {@code long}
 * {@link DcbEventStream#lastSequencePosition() sequence position} so the two cannot be confused: the position is a global
 * cursor (suitable for replay) but is NOT a safe concurrency boundary on its own, whereas this token is.
 * <p>
 * The {@link #value()} is store-internal. Round-trip a token within the same store; do not interpret or compare its
 * value across stores.
 *
 * @param value the store-internal token value
 */
@NullMarked
public record DcbConsistencyToken(long value) {

    public DcbConsistencyToken {
        if (value < 0) {
            throw new IllegalArgumentException("Consistency token value cannot be negative");
        }
    }

    /**
     * Creates a token from a store-internal value.
     */
    public static DcbConsistencyToken of(long value) {
        return new DcbConsistencyToken(value);
    }
}
