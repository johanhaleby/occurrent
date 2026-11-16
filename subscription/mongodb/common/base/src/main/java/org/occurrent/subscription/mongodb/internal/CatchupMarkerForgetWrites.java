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

package org.occurrent.subscription.mongodb.internal;

import static org.occurrent.subscription.mongodb.internal.MongoCommons.isJournalNotSupported;

/**
 * Sends a write of a catch-up marker forget, which asks for {@code j: true} with
 * {@link MongoCommons#CATCHUP_MARKER_FORGET_WRITE_CONCERN}. A WiredTiger server that runs without journaling refuses
 * it, and the forget then fails with an {@link IllegalStateException} that says catch-up marker storage needs
 * journaling. A crash of that server could lose an acknowledged forget, the marker would come back, and the next
 * catch-up would skip the replay the forget asked for. The refusal repeats on every attempt, so
 * {@link #isRetryable(Throwable)} answers {@code false} for it. See ADR 179.
 */
public final class CatchupMarkerForgetWrites {

    /**
     * The message of the {@link IllegalStateException} a forget fails with on a server that runs without journaling.
     */
    public static final String JOURNALING_REQUIRED_MESSAGE = "Catch-up marker storage needs a MongoDB server with journaling. The server refused j: true for a catch-up marker forget because it runs without journaling, and a crash could then lose an acknowledged forget and make the next catch-up skip the replay that forget asked for.";

    private CatchupMarkerForgetWrites() {
    }

    /**
     * @param write sends the write with {@link MongoCommons#CATCHUP_MARKER_FORGET_WRITE_CONCERN}
     * @throws IllegalStateException when the server refuses {@code j: true} because it runs without journaling
     */
    public static void write(Runnable write) {
        try {
            write.run();
        } catch (RuntimeException e) {
            if (isJournalNotSupported(e)) {
                throw new IllegalStateException(JOURNALING_REQUIRED_MESSAGE, e);
            }
            throw e;
        }
    }

    /**
     * Whether a forget write that failed with {@code throwable} is worth another attempt. The refusal of a server that
     * runs without journaling is not, since every later attempt gets it too.
     */
    public static boolean isRetryable(Throwable throwable) {
        return !isJournalNotSupported(throwable);
    }
}
