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

package org.occurrent.subscription.api.reactor;

import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.StartAt;

import static java.util.Objects.requireNonNull;

/**
 * Start positions that replay from a given position on the first run and resume from the durable checkpoint afterwards.
 * <p>
 * A read model that wants to catch up from history on startup starts at {@code replayStart} the first time (when no
 * checkpoint has been stored yet) and, on every later run, resumes from wherever it left off (the subscription model
 * default, which reads the stored checkpoint). The decision is made when the subscription starts by reading the
 * reactor {@link CheckpointStorage} for an existing checkpoint, so it reflects whether this subscription has run before.
 * <p>
 * This is the reactor counterpart of the blocking {@code ResumeStartPositions} and the building block behind the
 * framework's {@code resumeBehavior = DEFAULT} handling. Because {@link StartAt}/{@link DcbStartAt} are resolved
 * synchronously when the subscription starts, the checkpoint read is awaited at that point, matching the framework's
 * own behavior.
 */
public final class ResumeStartPositions {

    private ResumeStartPositions() {
    }

    /**
     * A {@link StartAt} that starts at {@code replayStart} until a checkpoint exists for {@code subscriptionId} in
     * {@code checkpointStorage}, then resumes from the stored checkpoint (the subscription model default) on later runs.
     *
     * @param subscriptionId    the subscription whose checkpoint decides replay vs resume
     * @param checkpointStorage the reactor storage read, when the subscription starts, for an existing checkpoint
     * @param replayStart       where to start when no checkpoint exists yet (for example the beginning of the stream)
     */
    public static StartAt replayThenResume(String subscriptionId, CheckpointStorage checkpointStorage, StartAt replayStart) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(checkpointStorage, "checkpointStorage cannot be null");
        requireNonNull(replayStart, "replayStart cannot be null");
        return StartAt.dynamic(() -> checkpointStorage.read(subscriptionId).blockOptional().isPresent() ? StartAt.subscriptionModelDefault() : replayStart);
    }

    /**
     * The DCB counterpart of {@link #replayThenResume(String, CheckpointStorage, StartAt)}, returning a
     * {@link DcbStartAt} that replays from {@code replayStart} until a checkpoint exists, then resumes from the stored
     * position on later runs.
     *
     * @param subscriptionId    the subscription whose checkpoint decides replay vs resume
     * @param checkpointStorage the reactor storage read, when the subscription starts, for an existing checkpoint
     * @param replayStart       where to start when no checkpoint exists yet (for example {@link DcbStartAt#beginning()})
     */
    public static DcbStartAt replayThenResumeDcb(String subscriptionId, CheckpointStorage checkpointStorage, DcbStartAt replayStart) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(checkpointStorage, "checkpointStorage cannot be null");
        requireNonNull(replayStart, "replayStart cannot be null");
        return DcbStartAt.dynamic(ctx -> checkpointStorage.read(subscriptionId).blockOptional().isPresent() ? DcbStartAt.subscriptionModelDefault() : replayStart);
    }
}
