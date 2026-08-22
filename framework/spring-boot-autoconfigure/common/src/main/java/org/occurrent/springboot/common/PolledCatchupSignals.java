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

package org.occurrent.springboot.common;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.projection.AppliedAppendRecorder;

import java.util.function.BooleanSupplier;

import static java.util.Objects.requireNonNull;

/**
 * Drives an {@link AppliedAppendRecorder}'s two catch-up signals from a subscription model that can only be asked
 * whether it is catching up, for a model that does not send them itself. Registered as a
 * {@link AppliedAppendRecordingRegistry#register(String, BooleanSupplier) tick}, so the poll that already exists for
 * the clear does the watching too.
 * <p>
 * A reading that has turned from live to catching up is the start of a catch-up, and one that has turned back is the
 * end of it. Both are worse than what a model that sends the signals gives, in two ways worth knowing before relying
 * on this.
 * <p>
 * The whole catch-up counts as history, because the reading says nothing about where inside it the history ends and
 * the events written since it started begin. So the projection records nothing until the catch-up is over, and an
 * append that lands while one runs is answered from the event store rather than from what the projection recorded.
 * <p>
 * A catch-up that starts and finishes between two readings is not seen at all, and the projection records the
 * history that catch-up replayed as though it were live
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>,
 * decision 6). That is the reason a model that can send the signals should, and the reason this class is the
 * fallback rather than the mechanism.
 * <p>
 * Not thread-safe across concurrent calls. The registry serializes an id's ticks, which is the only place this runs.
 */
@NullMarked
public final class PolledCatchupSignals implements BooleanSupplier {

    private final AppliedAppendRecorder recorder;
    private final BooleanSupplier catchingUp;
    // The catch-up this poll has announced and not yet ended, null between two of them.
    private @Nullable Object episode = null;

    /**
     * @param recorder   The projection to send the signals to.
     * @param catchingUp Whether the projection's subscription is catching up right now.
     */
    public PolledCatchupSignals(AppliedAppendRecorder recorder, BooleanSupplier catchingUp) {
        this.recorder = requireNonNull(recorder, "recorder cannot be null");
        this.catchingUp = requireNonNull(catchingUp, "catchingUp cannot be null");
    }

    /**
     * Reads whether a catch-up is running, sends whichever signal that reading has turned into, and retries a clear
     * still owed.
     *
     * @return {@code true} while a catch-up is running or a clear is still owed, which keeps the poll at its fast
     * interval so the end of the catch-up is seen soon after it happens rather than up to a full backed-off interval
     * later.
     */
    @Override
    public boolean getAsBoolean() {
        boolean catchingUpNow = catchingUp.getAsBoolean();
        if (catchingUpNow && episode == null) {
            Object started = new Object();
            episode = started;
            recorder.catchupStarted(started);
        } else if (!catchingUpNow && episode != null) {
            recorder.historyRead(episode);
            episode = null;
        }
        // After the signals above, so a clear this tick owes is attempted on this tick and writes held for one that
        // has just ended are flushed on it.
        return recorder.pollForClear() || catchingUpNow;
    }
}
