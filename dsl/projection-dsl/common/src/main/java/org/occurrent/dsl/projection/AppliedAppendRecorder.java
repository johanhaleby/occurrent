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
 * The hooks a recording wrapper exposes to whoever drives its replay observation from outside
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>,
 * decision 7). Implemented by both {@code RecordingMaterializedView} (blocking) and {@code RecordingReactiveUpdate}
 * (reactor).
 * <p>
 * A recording wrapper already reacts to a replay it can see for itself, through the {@link ReplayPhase} it was built
 * with or the view-DSL replay lifecycle it forwards. These hooks cover what neither of those catches. A replay whose
 * deliveries are all filtered out server-side never delivers anything for the wrapper to check, and a clear can fail
 * while that replay is going on. The Spring Boot registrars' poll calls {@link #pollReplayPhase()} on a schedule,
 * which re-checks the phase itself before reacting, so a clear owed from a replay the phase no longer reports (it
 * ended, possibly before a live delivery ever reached the wrapper to retry it there) still gets retried until it
 * succeeds, and a poll whose earlier reading of the phase is already stale by the time it acts cannot wipe a live
 * append the wrapper recorded in the meantime.
 */
@NullMarked
public interface AppliedAppendRecorder extends CatchupListener {

    /**
     * Retries a clear already marked as owed by an earlier {@link CatchupListener#catchupStarted(Object)}, doing
     * nothing if none is owed. Never marks a new clear itself, so calling this on a projection that has not caught up
     * is a no-op rather than a spurious clear. The default no-op is for a caller (a test double, typically) that
     * never needs the retry.
     */
    default void retryPendingClear() {
    }

    /**
     * Retries an owed clear, writes whatever was waiting for it, and reports whether one is still owed. What a poller
     * calls on a schedule, and what keeps a clear moving for a projection that has gone quiet: without it, a clear
     * that failed while a catch-up ran would only be retried by the next delivery, and a projection that receives
     * none would never record again. The default no-op, returning {@code false}, is for a caller that never polls.
     */
    default boolean pollForClear() {
        return false;
    }
}
