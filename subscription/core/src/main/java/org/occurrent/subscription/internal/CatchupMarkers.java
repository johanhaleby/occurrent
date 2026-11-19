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

package org.occurrent.subscription.internal;

import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;

/**
 * Reads the position a catch-up resumes after out of the marker stored under its subscription id, for the reactor
 * projection feed and push subscription model. The blocking ones read a stored marker as done and skip the replay.
 */
public final class CatchupMarkers {

    private CatchupMarkers() {
    }

    /**
     * The position {@code stored} holds.
     *
     * @param subscriptionId The id the marker is stored under, named in the failure
     * @param stored         The checkpoint stored under {@code subscriptionId}
     * @throws NotACatchupMarkerException when {@code stored} is not a catch-up marker, such as a checkpoint a durable
     *                                    subscription stored under the same id
     */
    public static long positionOf(String subscriptionId, Checkpoint stored) {
        try {
            return GlobalCheckpoint.positionOf(stored);
        } catch (IllegalArgumentException e) {
            throw new NotACatchupMarkerException(notACatchupMarker(subscriptionId, stored.asString()), e);
        }
    }

    /**
     * Whether {@code error}, or one of its causes, is a refusal of a checkpoint that is not a catch-up marker.
     */
    public static boolean refusedANonMarker(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof NotACatchupMarkerException) {
                return true;
            }
        }
        return false;
    }

    /**
     * Thrown when the checkpoint stored under a subscription id is not a catch-up marker. An
     * {@link IllegalStateException}, as the refusal was before it had a type of its own.
     */
    public static final class NotACatchupMarkerException extends IllegalStateException {
        NotACatchupMarkerException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    static String notACatchupMarker(String subscriptionId, String stored) {
        return "The checkpoint stored for subscription id \"" + subscriptionId + "\" is not a catch-up marker: " + stored
                + ". Something other than a catch-up stored it, such as a durable subscription that used this id before."
                + " Delete the checkpoint stored for this id, or subscribe under a new id.";
    }
}
