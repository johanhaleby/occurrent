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
import org.jspecify.annotations.Nullable;

import java.util.StringJoiner;

/**
 * Thrown by a read with {@link DcbReadOptions#afterVersions(DcbMarkerVersions)} when the store can't tell which events
 * came after the supplied versions. Read the boundary in full instead, with {@link DcbReadOptions#withVersions()}, and
 * start a new snapshot from that read.
 * <p>
 * The message names the check that failed and, when there is one, the marker key it failed on. A store refuses when
 * the saved versions cover other marker keys than the criteria has, when a marker is missing or has no epoch, when an
 * epoch differs from the saved one, when a saved version is above the store's, when a marker was incremented by a
 * writer that wrote no marker versions, when the criteria is {@link DcbCriteria.MatchAll}, when resuming from marker
 * versions is turned off for the store, and when the store couldn't take a consistent snapshot to compare against.
 */
@NullMarked
public class DcbSnapshotInvalidException extends RuntimeException {
    private final DcbCriteria criteria;
    private final DcbMarkerVersions afterVersions;

    /**
     * Creates an exception for a refused read.
     *
     * @param criteria      the criteria of the refused read
     * @param afterVersions the versions the read was asked to resume after
     * @param message       the check that failed, and the marker key when there is one
     */
    public DcbSnapshotInvalidException(DcbCriteria criteria, DcbMarkerVersions afterVersions, String message) {
        this(criteria, afterVersions, message, null);
    }

    /**
     * Creates an exception for a refused read, caused by {@code cause}.
     *
     * @param criteria      the criteria of the refused read
     * @param afterVersions the versions the read was asked to resume after
     * @param message       the check that failed, and the marker key when there is one
     * @param cause         the error that kept the store from comparing the versions, or {@code null}
     */
    public DcbSnapshotInvalidException(DcbCriteria criteria, DcbMarkerVersions afterVersions, String message, @Nullable Throwable cause) {
        super(message, cause);
        this.criteria = criteria;
        this.afterVersions = afterVersions;
    }

    /**
     * The criteria of the refused read.
     */
    public DcbCriteria criteria() {
        return criteria;
    }

    /**
     * The versions the refused read was asked to resume after.
     */
    public DcbMarkerVersions afterVersions() {
        return afterVersions;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DcbSnapshotInvalidException.class.getSimpleName() + "[", "]")
                .add("message=" + getMessage())
                .add("criteria=" + criteria)
                .add("afterVersions=" + afterVersions)
                .toString();
    }
}
