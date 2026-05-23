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

import java.util.StringJoiner;

/**
 * Thrown when a DCB append condition detects a conflicting event.
 * <p>
 * {@link #appendCondition} is the condition that was evaluated and
 * {@link #currentPosition} is the event store position at the time the append failed.
 */
@NullMarked
public class DcbAppendConditionNotFulfilledException extends RuntimeException {
    /**
     * The DCB append condition that was not fulfilled.
     */
    public final DcbAppendCondition appendCondition;
    /**
     * The current event store sequence position when the conflict was detected.
     */
    public final long currentPosition;

    /**
     * Creates an exception for a failed DCB append condition.
     */
    public DcbAppendConditionNotFulfilledException(DcbAppendCondition appendCondition, long currentPosition, String message) {
        super(message);
        this.appendCondition = appendCondition;
        this.currentPosition = currentPosition;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DcbAppendConditionNotFulfilledException.class.getSimpleName() + "[", "]")
                .add("appendCondition=" + appendCondition)
                .add("currentPosition=" + currentPosition)
                .toString();
    }
}
