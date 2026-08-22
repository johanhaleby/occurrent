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
 * Shared wording for the position-backfill startup guard across all event stores, so that they warn or fail with
 * identical text when the event collection holds events without a {@code position} field.
 */
@NullMarked
public final class PositionBackfillValidator {

    private PositionBackfillValidator() {
    }

    private static final String RUNBOOK = "doc/runbooks/position-backfill.md";

    // An event whose position the pre-0.34.0 updateEvent dropped also has no position field, so it reaches both
    // messages below looking like history that predates position. Backfilling it assigns a position it never had,
    // which cannot be undone, so both messages have to point at the other runbook before anyone runs the backfill.
    private static final String UPDATE_EVENT_CAVEAT =
            " If this application ever called updateEvent while running Occurrent 0.33.0 or earlier, read"
                    + " doc/runbooks/update-event-repair.md before running the backfill. That defect could drop an"
                    + " event's position entirely, and backfilling such an event gives it a position it never had,"
                    + " which cannot be undone.";

    /**
     * Create the {@link IllegalStateException} to throw when {@code requireBackfilledPosition(true)} is set but the
     * event collection contains events without a position, with a message consistent across all event stores.
     *
     * @param eventStoreCollectionName the name of the event collection that contains unpositioned events
     * @return the exception to throw
     */
    public static IllegalStateException unpositionedEventsExist(String eventStoreCollectionName) {
        return new IllegalStateException(problem(eventStoreCollectionName)
                + " This store is configured to require backfilled positions, so it will not start. Run the position"
                + " backfill migration described in " + RUNBOOK + ", or turn off requireBackfilledPosition."
                + UPDATE_EVENT_CAVEAT);
    }

    /**
     * The message to log at WARN when the event collection contains events without a position and
     * {@code requireBackfilledPosition(true)} is not set, with wording consistent across all event stores.
     *
     * <p>This says something different from {@link #unpositionedEventsExist(String)} on purpose. Here the store
     * starts anyway, so the reader needs to know what it silently loses rather than how to satisfy a setting that
     * is already off.
     *
     * @param eventStoreCollectionName the name of the event collection that contains unpositioned events
     * @return the message to log
     */
    public static String unpositionedEventsMessage(String eventStoreCollectionName) {
        return problem(eventStoreCollectionName)
                + " New events get a position, but position-ordered reads and position-based catch-up skip the events"
                + " that do not have one, which can drop history from a projection without any error. Run the position"
                + " backfill migration described in " + RUNBOOK + ", or set requireBackfilledPosition(true) to fail"
                + " startup instead of warning."
                + UPDATE_EVENT_CAVEAT;
    }

    private static String problem(String eventStoreCollectionName) {
        return "The event collection '" + eventStoreCollectionName + "' contains events without a position field.";
    }
}
