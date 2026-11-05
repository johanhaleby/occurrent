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

import java.util.Objects;
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
 * versions is turned off for the store, when the read runs inside a transaction the caller owns, and when the store
 * couldn't take a consistent snapshot to compare against.
 * <p>
 * The stores don't check the order of what they return, so an application service and a snapshot view throw it too, via
 * {@link #eventBelowFoldedPosition(DcbCriteria, DcbMarkerVersions, long)}.
 */
@NullMarked
public class DcbSnapshotInvalidException extends RuntimeException {
    private final DcbCriteria criteria;
    private final DcbMarkerVersions afterVersions;
    private final @Nullable Object beforeAppendOf;

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
        this(criteria, afterVersions, message, cause, null);
    }

    private DcbSnapshotInvalidException(DcbCriteria criteria, DcbMarkerVersions afterVersions, String message, @Nullable Throwable cause, @Nullable Object beforeAppendOf) {
        super(message, cause);
        this.criteria = criteria;
        this.afterVersions = afterVersions;
        this.beforeAppendOf = beforeAppendOf;
    }

    /**
     * Creates an exception for a read after {@code afterVersions} that returned an event below the highest position
     * folded into the saved state. Folding that event last could give another state than a read in position order.
     *
     * @param criteria              the criteria of the refused read
     * @param afterVersions         the versions the read was asked to resume after
     * @param highestFoldedPosition the highest position folded into the saved state
     * @return the exception to throw
     */
    public static DcbSnapshotInvalidException eventBelowFoldedPosition(DcbCriteria criteria, DcbMarkerVersions afterVersions, long highestFoldedPosition) {
        return new DcbSnapshotInvalidException(criteria, afterVersions, "The read returned an event below position " + highestFoldedPosition + ", the highest position folded into the saved state");
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

    /**
     * This refusal, marked as thrown by the read of an attempt of {@code execute}, with {@code this} as its cause. That
     * attempt has appended nothing. The mark says nothing about an earlier attempt of the same execute, which has
     * committed if the retry strategy retried after a committed append. Without a transaction, a synchronous subscription
     * that throws what the retry strategy retries on does that.
     *
     * @param execute what identifies the execute, compared by identity. The application services use the domain
     *                function they were given, so a refusal from an execute with another function doesn't pass for the
     *                caller's own. A refusal from a second service given the same function does.
     * @return the marked exception to throw
     */
    public DcbSnapshotInvalidException beforeAppend(Object execute) {
        return new DcbSnapshotInvalidException(criteria, afterVersions, String.valueOf(getMessage()), this, Objects.requireNonNull(execute, "execute cannot be null"));
    }

    /**
     * Whether an application service marked this refusal with {@link #beforeAppend(Object)} for this {@code execute}. A
     * caller can then read in full and decide again, since the attempt that threw it appended nothing. It can't tell
     * whether an earlier attempt of the same execute committed, or whether another service given the same function did,
     * see {@link #beforeAppend(Object)}. Any other refusal can have reached the caller after its append committed, for
     * example from another application service called by a synchronous subscription.
     *
     * @param execute what the caller gave the application service to identify the execute
     */
    public boolean isBeforeAppendOf(Object execute) {
        return beforeAppendOf == Objects.requireNonNull(execute, "execute cannot be null");
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DcbSnapshotInvalidException.class.getSimpleName() + "[", "]")
                .add("message=" + getMessage())
                .add("criteria=" + criteria)
                .add("afterVersions=" + afterVersions)
                .add("beforeAppend=" + (beforeAppendOf != null))
                .toString();
    }
}
