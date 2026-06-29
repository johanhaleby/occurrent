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

import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

/**
 * Options that scope a DCB read.
 *
 * @param afterSequencePosition optional lower bound; when present, only events after this DCB sequence position are returned
 */
@NullMarked
public record DcbReadOptions(OptionalLong afterSequencePosition) {

    public DcbReadOptions {
        requireNonNull(afterSequencePosition, "After sequence position cannot be null");
        afterSequencePosition.ifPresent(position -> {
            if (position < 0) {
                throw new IllegalArgumentException("After sequence position cannot be negative");
            }
        });
    }

    /**
     * Reads from the beginning of the DCB sequence.
     */
    public static DcbReadOptions fromBeginning() {
        return new DcbReadOptions(OptionalLong.empty());
    }

    /**
     * Reads only events after the supplied DCB sequence position.
     */
    public static DcbReadOptions afterSequencePosition(long position) {
        return new DcbReadOptions(OptionalLong.of(position));
    }
}
