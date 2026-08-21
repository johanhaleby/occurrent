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

package org.occurrent.dsl.projection;

import org.jspecify.annotations.NullMarked;

/**
 * One reading of where a projection is, which part of a catch-up it is in and which catch-up that is, taken together
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>,
 * decision 6).
 * <p>
 * Together because a recorder decides what to write from this, and a catch-up finishing between two separate reads
 * gives it a pair that never existed. Acting on such a pair undoes work the finished catch-up had correctly done.
 *
 * @param phase      Which part of a catch-up a delivery belongs to.
 * @param generation Which catch-up that is. It changes when a new one starts and is {@code 0} while the projection is
 *                   live, so two catch-ups are told apart even when nothing sampled the gap between them. A
 *                   composition with nothing to derive it from answers {@code 0} throughout, which reads as one
 *                   unbroken catch-up and costs only the clear that a second one would otherwise get.
 */
@NullMarked
public record CatchupSnapshot(CatchupPhase phase, long generation) {

    /** Nothing is being caught up. */
    public static final CatchupSnapshot LIVE = new CatchupSnapshot(CatchupPhase.LIVE, 0L);

    /** A catch-up whose parts are not told apart, which reads as reading history for its whole length. */
    public static CatchupSnapshot readingHistory(long generation) {
        return new CatchupSnapshot(CatchupPhase.REPLAYING_HISTORY, generation);
    }
}
