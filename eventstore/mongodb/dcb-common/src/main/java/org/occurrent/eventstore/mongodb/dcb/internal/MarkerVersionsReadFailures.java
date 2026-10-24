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

package org.occurrent.eventstore.mongodb.dcb.internal;

import org.jspecify.annotations.NullMarked;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks why reads that asked for marker versions couldn't report them, so a store logs the first failure of each kind
 * as a warning and every later one of the same kind at debug level. A deployment that keeps failing the read-only
 * commit then doesn't flood the log. One instance per store.
 */
@NullMarked
public final class MarkerVersionsReadFailures {

    private final Set<String> seenKinds = ConcurrentHashMap.newKeySet();

    /**
     * Records a failure and returns whether it is the first of its kind for this store.
     *
     * @param phase where it failed, such as {@code "read"} or {@code "commit"}
     * @param cause the error
     */
    public boolean recordAndCheckFirst(String phase, Throwable cause) {
        return seenKinds.add(phase + ":" + cause.getClass().getName());
    }

    /**
     * The log message for a failure.
     *
     * @param refused whether the read is refused, which is what happens to a read after marker versions. Otherwise
     *                the read returns without versions.
     */
    public static String message(String phase, Throwable cause, boolean refused) {
        String consequence = refused
                ? "The read is refused, since a read after marker versions can't fall back to a full read."
                : "The read returns no versions, so a snapshot over it is not saved this time.";
        return "A DCB read that asked for marker versions could not take them from a committed snapshot (" + phase + " failed with "
                + cause.getClass().getSimpleName() + "). " + consequence;
    }
}
