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

package org.occurrent.dsl.snapshot.internal;

import org.slf4j.Logger;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * Logs a warning the first time a DCB snapshot save is skipped for a given reason, and at debug level after that, so a
 * decider or view that can never save warns once instead of on every command.
 */
public final class SkippedSaves {

    /**
     * Why a DCB snapshot wasn't saved.
     */
    public enum Reason {
        /**
         * The event store reported no marker versions to save at.
         */
        NO_MARKER_VERSIONS("the event store reported no marker versions to save the snapshot at. That happens on a boundary where a tag of an item with tags, or a type of an item without tags, has no marker yet or only one that 0.34 wrote without an epoch, over DcbCriteria.all(), inside a transaction, on a store that doesn't report them, and when a marker was removed and created again between the read and the append"),
        /**
         * The append wasn't confirmed as durably committed.
         */
        UNCONFIRMED("the append isn't confirmed as committed. That happens, for example, when the append isn't majority-committed yet, when the store's check of it fails, and on a store that doesn't implement DcbAppendConfirmation");

        private final String description;

        Reason(String description) {
            this.description = description;
        }
    }

    private final Logger log;
    private final Set<Reason> warned = ConcurrentHashMap.newKeySet();

    public SkippedSaves(Logger log) {
        this.log = requireNonNull(log, "log cannot be null");
    }

    /**
     * Logs that the snapshot with {@code key} wasn't saved for {@code reason}.
     */
    public void skip(String key, Reason reason) {
        requireNonNull(key, "key cannot be null");
        requireNonNull(reason, "reason cannot be null");
        if (warned.add(reason)) {
            log.warn("Didn't save snapshot '{}' since {}. The next command or delivery reads the events since the last saved snapshot. This is logged once per instance and reason.", key, reason.description);
        } else {
            log.debug("Didn't save snapshot '{}' since {}.", key, reason.description);
        }
    }
}
