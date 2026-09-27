/*
 *
 *  Copyright 2026 Johan Haleby
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

package org.occurrent.eventstore.api.internal;

import org.jspecify.annotations.NullMarked;

/**
 * Shared wording for the startup check for events that Occurrent's own {@code updateEvent} damaged before 0.34.0,
 * so that all event stores say the same thing whether they warn about it or refuse to start.
 */
@NullMarked
public final class UpdateEventRepairValidator {

    private UpdateEventRepairValidator() {
    }

    private static final String RUNBOOK = "doc/runbooks/update-event-repair.md";

    /**
     * The message to log at WARN when the event collection holds events with a string {@code position}, which is what
     * the pre-0.34.0 {@code updateEvent} write-back left behind, and {@code requireRepairedEvents(true)} is not set.
     *
     * @param eventStoreCollectionName the name of the event collection that contains damaged events
     * @return the message to log
     */
    public static String damagedEventsMessage(String eventStoreCollectionName) {
        return "The event collection '" + eventStoreCollectionName + "' contains events that Occurrent's own"
                + " updateEvent damaged in version 0.33.0 or earlier by storing their position as a string instead of"
                + " a number. Reads in position order, position based catch-up and DCB reads skip such an event"
                + " without an error, and a conditional append can miss a conflict against it."
                + " Run the repair described in " + RUNBOOK + ". Upgrading alone does not fix events that are already"
                + " stored, and until the repair runs you can set requireRepairedEvents(true) to fail startup instead"
                + " of warning.";
    }

    /**
     * Create the {@link IllegalStateException} to throw when {@code requireRepairedEvents(true)} is set and the event
     * collection holds an event whose position or tag index is wrong, whether Occurrent's own {@code updateEvent}
     * damaged it or a position was set by hand.
     *
     * @param eventStoreCollectionName the name of the event collection that contains damaged events
     * @return the exception to throw
     */
    public static IllegalStateException damagedEventsExist(String eventStoreCollectionName) {
        return new IllegalStateException("The event collection '" + eventStoreCollectionName + "' contains events"
                + " whose position or tag index no store would have written, such as events that Occurrent's own"
                + " updateEvent damaged in version 0.33.0 or earlier, or events whose position was set by hand."
                + " Reads in position order, position based catch-up and DCB reads skip such an event, read it wrong"
                + " or fail on it, and DCB reads and conditional appends find a DCB event by its tag index alone, so"
                + " a conditional append can miss a conflict against an event they skip."
                + " This store is configured to require repaired events, so it will not start. Run the repair"
                + " described in " + RUNBOOK + ". An event the repair reports as unrecoverable can keep this store"
                + " from starting until you fix it by hand as step 5 of the runbook describes, and the queries in"
                + " its step 6 find every event that still does. A position counter below the highest position, a"
                + " missing one counting as zero, or one that is not the int32 or int64 every writer stores keeps it"
                + " from starting too, and step 5 says how to restore it. To start with the damage still in place,"
                + " turn off requireRepairedEvents.");
    }
}
