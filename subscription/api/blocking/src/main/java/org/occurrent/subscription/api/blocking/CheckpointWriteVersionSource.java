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

package org.occurrent.subscription.api.blocking;

import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.CheckpointWriteCondition;

import java.util.OptionalLong;

/**
 * Where a checkpoint-writing subscription model gets the version it stamps its own writes with (see ADR 116).
 * <p>
 * A subscription model that writes checkpoints asks this before every write. A version becomes
 * {@link CheckpointWriteCondition#notOlderThan(long)}, and an empty answer or no source at all becomes
 * {@link CheckpointWriteCondition#any()}, so the model has one code path rather than a choice between two. Nothing
 * in that model's own vocabulary names a lease, a fencing token, or a competing consumer, and this interface does
 * not either. The wiring that turns a lock's fencing token into a write version, for example
 * {@code CompetingConsumerStrategy::fencingToken}, lives at the call site that constructs the model.
 * <p>
 * The call runs on the per-event write path, so an implementation must not block and must not reach a database.
 *
 * @see CheckpointStorage#save(String, org.occurrent.subscription.Checkpoint, CheckpointWriteCondition)
 */
@FunctionalInterface
@NullMarked
public interface CheckpointWriteVersionSource {

    /**
     * The version to stamp the next checkpoint write for {@code subscriptionId} with.
     *
     * @param subscriptionId The id of the subscription about to write a checkpoint
     * @return The version to write, or {@link OptionalLong#empty()} for no version, which the caller turns into
     * {@link CheckpointWriteCondition#any()}
     */
    OptionalLong writeVersion(String subscriptionId);
}
