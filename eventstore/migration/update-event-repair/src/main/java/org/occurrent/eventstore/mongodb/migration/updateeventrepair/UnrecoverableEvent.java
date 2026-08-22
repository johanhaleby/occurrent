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


package org.occurrent.eventstore.mongodb.migration.updateeventrepair;

import org.jspecify.annotations.NullMarked;

/**
 * An event whose damage this tool cannot undo, named by its {@code _id} so an operator can look at it.
 * <p>
 * The repair rebuilds a stored event from what the document still holds. Where the old {@code updateEvent} write-back
 * destroyed the only copy of a value, there is nothing left to rebuild it from, and the tool reports the event here
 * rather than inventing one.
 *
 * @param eventId The {@code _id} of the stored event.
 * @param reason  Why the event could not be fully repaired.
 * @param detail  The concrete value or error behind the reason, for an operator reading a report.
 */
@NullMarked
public record UnrecoverableEvent(Object eventId, Reason reason, String detail) {

    /**
     * Why an event could not be fully repaired.
     */
    public enum Reason {
        /**
         * The event carries DCB tags, so it was written by a DCB append and had a position, but the document has no
         * {@code position} field at all. An update function that returned a replacement event built from scratch
         * dropped it, and the number exists nowhere else. Assigning a fresh position in {@code _id} order would look
         * plausible and be wrong, because a consumer holding a checkpoint from before the damage would then disagree
         * with the store, so the tool refuses to do it.
         */
        POSITION_LOST,
        /**
         * The event's {@code position} is a string whose value is already held, as a number, by another event. Two
         * events claim one position, which the unique position index rejects. The old write-back preserved whatever
         * position the update function returned, so a function that forged one produced this. Only one of the two
         * events can keep the position and the tool cannot tell which.
         */
        POSITION_ALREADY_TAKEN,
        /**
         * The event's {@code position} is a string that is not a number, so the original position cannot be read back
         * out of it. Nothing in the known {@code updateEvent} defect produces this, so it points at damage from
         * somewhere else.
         */
        POSITION_NOT_A_NUMBER,
        /**
         * The event could not be read well enough to repair it. Its {@code dcbtags} is not a string, or does not
         * decode to a tag set. Nothing Occurrent writes produces either shape, so it points at a document that was
         * edited outside the library. The run continues past it rather than stopping, so one such event cannot hold
         * up the repair of a whole collection.
         */
        UNREADABLE
    }
}
