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
        return problem(eventStoreCollectionName)
                + " Run the repair described in " + RUNBOOK + ". Upgrading alone does not fix events that are already"
                + " stored, and until the repair runs you can set requireRepairedEvents(true) to fail startup instead"
                + " of warning.";
    }

    /**
     * Create the {@link IllegalStateException} to throw when {@code requireRepairedEvents(true)} is set and the event
     * collection holds an event the update-event repair tool would repair.
     *
     * @param eventStoreCollectionName the name of the event collection that contains damaged events
     * @return the exception to throw
     */
    public static IllegalStateException damagedEventsExist(String eventStoreCollectionName) {
        return new IllegalStateException(problem(eventStoreCollectionName)
                + " This store is configured to require repaired events, so it will not start. Run the repair"
                + " described in " + RUNBOOK + ". An event the repair reports as unrecoverable can keep this store"
                + " from starting until you fix it by hand as step 5 of the runbook describes, and the two queries in"
                + " its step 6 find every event that still does. To start with the damage still in place, turn off"
                + " requireRepairedEvents.");
    }

    private static String problem(String eventStoreCollectionName) {
        return "The event collection '" + eventStoreCollectionName + "' contains events that Occurrent's own"
                + " updateEvent damaged in version 0.33.0 or earlier. Such an event has its position stored as a"
                + " string instead of a number, or it was written by a DCB append and lost its tag index, and"
                + " sometimes its position as well. Position ordered reads and position based catch-up skip an event"
                + " without a numeric position. DCB reads and conditional appends only see a DCB event that has both"
                + " its tag index and a numeric position, so a conditional append can miss a conflict against a"
                + " damaged one. None of this raises an error.";
    }
}
