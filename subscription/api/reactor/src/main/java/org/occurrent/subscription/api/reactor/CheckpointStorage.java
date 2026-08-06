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

package org.occurrent.subscription.api.reactor;

import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import reactor.core.publisher.Mono;


/**
 * A {@code CheckpointStorage} provides means to read and write the checkpoint to storage.
 * This subscriptions can continue where they left off by passing the {@link Checkpoint} provided by {@link #read(String)}
 * to a {@link CheckpointAwareSubscriptionModel} when the application is restarted etc.
 */
@NullMarked
public interface CheckpointStorage {

    /**
     * Read the raw checkpoint for a given subscription.
     * <p>
     * Note that when starting a new subscription you typically want to create {@link StartAt} from the global checkpoint
     * (using {@link CheckpointAwareSubscriptionModel#globalCheckpoint()}) if no {@code Checkpoint} is found for the given subscription.
     * </p>
     * For example:
     * <pre>
     * StartAt startAt = storage.read(subscriptionId)
     *                          .switchIfEmpty(Mono.defer(() -> checkpointAwareSubscriptionModel.globalCheckpoint().flatMap(checkpoint -> storage.save(subscriptionId, checkpoint))))
     *                          .map(StartAt::checkpoint);
     * </pre>
     *
     * @param subscriptionId The id of the subscription whose checkpoint to find
     * @return A Mono with the {@link Checkpoint} data point for the supplied subscriptionId
     */
    Mono<Checkpoint> read(String subscriptionId);

    /*
     * Save the checkpoint for the supplied subscriptionId to storage and then return it for easier chaining.
     */
    Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint);


    /**
     * Delete the {@link Checkpoint} for the supplied {@code subscriptionId}.
     *
     * @param subscriptionId The id of the subscription to delete the {@link Checkpoint} for.
     */
    Mono<Void> delete(String subscriptionId);
}