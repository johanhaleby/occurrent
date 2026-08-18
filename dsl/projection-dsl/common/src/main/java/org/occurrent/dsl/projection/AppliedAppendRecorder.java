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
 * The one hook a recording wrapper exposes to whoever drives its replay observation from outside
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>,
 * decision 7). Implemented by both {@code RecordingMaterializedView} (blocking) and {@code RecordingReactiveUpdate}
 * (reactor).
 * <p>
 * A recording wrapper already reacts to a replay it can see for itself, through the {@link ReplayPhase} it was built
 * with or the view-DSL replay lifecycle it forwards. This hook is for the case neither of those catches: a replay
 * whose deliveries are all filtered out server-side, where no delivery ever reaches the wrapper to be checked. The
 * Spring Boot registrars poll {@link ReplayPhase#isReplaying()} on a schedule for exactly that reason and call this
 * when it answers {@code true}.
 */
@NullMarked
public interface AppliedAppendRecorder {

    /**
     * This projection was seen replaying: mark it as needing a clear and attempt the clear on the calling thread.
     * Recording stays off until a clear succeeds, retried on every later call to this method or to the wrapper's
     * normal update path until one does.
     */
    void replayObserved();
}
