/*
 * Copyright 2020 Johan Haleby
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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;

/**
 * A {@code CheckpointStorage} provides means to read and write the checkpoint to storage.
 * This subscriptions can continue where they left off by passing the {@link Checkpoint} provided by {@link #read(String)}
 * to a {@link CheckpointAwareSubscriptionModel} when the application is restarted etc.
 */
public interface CheckpointStorage {

    /**
     * Read the checkpoint for a given subscription.
     * <p>
     * Note that when starting a new subscription you typically want to create {@link StartAt} from the global checkpoint
     * (using {@link CheckpointAwareSubscriptionModel#globalCheckpoint()}) if no {@code Checkpoint} is found for the given subscription.
     * </p>
     * For example:
     * <pre>
     * Checkpoint checkpoint = storage.read(subscriptionId);
     * if (checkpoint == null) {
     *      checkpoint = checkpointAwareSubscriptionModel.globalCheckpoint();
     *      storage.save(subscriptionId, checkpoint);
     * }
     * StartAt startAt = StartAt.checkpoint(checkpoint);
     * </pre>
     *
     * @param subscriptionId The id of the subscription whose checkpoint to find
     * @return A {@link Checkpoint} data point for the supplied subscriptionId
     */
    @Nullable
    Checkpoint read(@NonNull String subscriptionId);

    /*
     * Save the checkpoint for the supplied subscriptionId to storage and then return it for easier chaining.
     */
    @NullMarked
    Checkpoint save(String subscriptionId, Checkpoint checkpoint);

    /**
     * Delete the {@link Checkpoint} for the supplied {@code subscriptionId}.
     *
     * @param subscriptionId The id of the subscription to delete the {@link Checkpoint} for.
     */
    @NullMarked
    void delete(String subscriptionId);

    /**
     * Check if the subscription id has a stored checkpoint in this storage.
     *
     * @param subscriptionId The id of the subscription to check.
     * @return <code>true</code> if storage contains a checkpoint for the stream id, <code>false</code> otherwise.
     */
    @NullMarked
    boolean exists(String subscriptionId);
}