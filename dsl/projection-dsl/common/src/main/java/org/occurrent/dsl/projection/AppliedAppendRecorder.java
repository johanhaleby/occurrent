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
 * while that replay is going on. The Spring Boot registrars' poll asks {@link ReplayPhase#isReplaying()} on a
 * schedule. It calls {@link #replayObserved()} when the answer is {@code true}, {@link #retryPendingClear()}
 * otherwise, so a clear owed from a replay the phase no longer reports (it ended, possibly before a live delivery
 * ever reached the wrapper to retry it there) still gets retried until it succeeds.
 */
@NullMarked
public interface AppliedAppendRecorder {

    /**
     * This projection was seen replaying. Marks it as needing a clear and attempts the clear on the calling thread.
     * Recording stays off until a clear succeeds, retried on every later call to this method, to
     * {@link #retryPendingClear()}, or to the wrapper's normal update path, until one does.
     */
    void replayObserved();

    /**
     * Retries a clear already marked as owed by an earlier {@link #replayObserved()}, doing nothing if none is
     * owed. Never marks a new clear itself, so calling this on a projection that has not replayed is a no-op
     * rather than a spurious clear. The default no-op is for a caller (a test double, typically) that never needs
     * the retry. The recording wrappers override it to reach the same state a normal update would retry through.
     */
    default void retryPendingClear() {
    }
}
