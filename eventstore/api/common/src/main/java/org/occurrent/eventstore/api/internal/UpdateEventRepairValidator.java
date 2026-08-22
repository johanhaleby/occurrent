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
 * Shared wording for the startup warning about events that Occurrent's own {@code updateEvent} damaged before
 * 0.34.0, so that all event stores say the same thing about it.
 */
@NullMarked
public final class UpdateEventRepairValidator {

    private UpdateEventRepairValidator() {
    }

    private static final String RUNBOOK = "doc/runbooks/update-event-repair.md";

    /**
     * The message to log at WARN when the event collection holds events with a string {@code position}, which is what
     * the pre-0.34.0 {@code updateEvent} write-back left behind.
     *
     * <p>The store starts anyway and there is no setting to make it refuse to. Unlike an un-backfilled collection,
     * this damage is finite and already done, so failing startup would take an application down over history that a
     * one-off repair fixes, without protecting anything that is still being written.
     *
     * @param eventStoreCollectionName the name of the event collection that contains damaged events
     * @return the message to log
     */
    public static String damagedEventsMessage(String eventStoreCollectionName) {
        return "The event collection '" + eventStoreCollectionName + "' contains events that Occurrent's own"
                + " updateEvent damaged in version 0.33.0 or earlier. Their position is stored as a string instead of"
                + " a number, and events written by a DCB append also lost their tag index. DCB reads, position"
                + " ordered reads and position based catch-up all skip such an event, and a conditional append can"
                + " miss a conflict against it, with no error anywhere. Run the repair described in " + RUNBOOK + "."
                + " Upgrading alone does not fix events that are already stored.";
    }
}
