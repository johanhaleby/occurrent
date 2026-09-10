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
     * collection holds events with a string {@code position}.
     *
     * @param eventStoreCollectionName the name of the event collection that contains damaged events
     * @return the exception to throw
     */
    public static IllegalStateException damagedEventsExist(String eventStoreCollectionName) {
        return new IllegalStateException(problem(eventStoreCollectionName)
                + " This store is configured to require repaired events, so it will not start. Run the repair"
                + " described in " + RUNBOOK + ", or turn off requireRepairedEvents to start with the damage still"
                + " in place.");
    }

    private static String problem(String eventStoreCollectionName) {
        return "The event collection '" + eventStoreCollectionName + "' contains events that Occurrent's own"
                + " updateEvent damaged in version 0.33.0 or earlier. Their position is stored as a string instead of"
                + " a number, and events written by a DCB append also lost their tag index. DCB reads, position"
                + " ordered reads and position based catch-up all skip such an event, and a conditional append can"
                + " miss a conflict against it, with no error anywhere.";
    }
}
