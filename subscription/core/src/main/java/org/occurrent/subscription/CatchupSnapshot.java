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

package org.occurrent.subscription;

import org.jspecify.annotations.NullMarked;

/**
 * What a subscription model answers about one subscription's catch-up, read as one value rather than as separate
 * questions.
 * <p>
 * One value because a caller that samples this cannot hold the model still between two calls. A catch-up finishing
 * between them produces a pair that never existed, a reconciliation belonging to a catch-up that is already gone, and
 * a caller acting on it undoes work the finished catch-up had correctly done.
 *
 * @param catchingUp       Whether a catch-up is running at all, which stays {@code true} from the history read through
 *                         the reconciliation until the handover.
 * @param replayingHistory Whether that catch-up is still reading the history that was already there, rather than
 *                         delivering what was written since it started.
 * @param generation       Which catch-up this is, changing when a new one starts and {@code 0} when none is running,
 *                         so a caller can tell two of them apart even when it never sampled the gap between them.
 */
@NullMarked
public record CatchupSnapshot(boolean catchingUp, boolean replayingHistory, long generation) {

    /** No catch-up is running. */
    public static final CatchupSnapshot LIVE = new CatchupSnapshot(false, false, 0L);

    /**
     * A catch-up whose parts this model cannot tell apart, which reads as reading history for its whole length. The
     * safe answer, since a caller then treats none of it as live.
     */
    public static CatchupSnapshot ofUnknownPart(boolean catchingUp) {
        return catchingUp ? new CatchupSnapshot(true, true, 1L) : LIVE;
    }
}
