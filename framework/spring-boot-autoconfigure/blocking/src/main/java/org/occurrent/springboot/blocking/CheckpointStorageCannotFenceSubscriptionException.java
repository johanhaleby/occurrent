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

package org.occurrent.springboot.blocking;

import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;

import java.util.List;

/**
 * Thrown when a {@link CompetingConsumerStrategy} bean is wired next to a {@link CheckpointStorage} that answers
 * {@code true} to {@link CheckpointStorage#evaluatesWriteConditions()} overall but {@code false} to
 * {@link CheckpointStorage#evaluatesWriteConditionsFor(String)} for one or more of the subscription ids this
 * application declares through an annotation. The first checkpoint write for one of those ids would be refused once
 * a lease is acquired, the same way {@link CheckpointStorageCannotFenceException} describes for a storage that
 * cannot fence at all.
 * <p>
 * Only an id an annotation ({@code @Subscription}, {@code @StreamSubscription}, {@code @SynchronousSubscription},
 * {@code @DcbSubscription}, {@code @Projection}, {@code @Snapshot}, {@code @Saga}) declares can be found this way,
 * at the point every singleton exists and before any of them registers. A subscription id built or read only at
 * runtime is outside what this check can enumerate. So is one an annotation declares whose registration writes a
 * checkpoint before this check runs at all, which {@code @Subscription} and its three siblings can, since they
 * register per bean as each bean initializes, ahead of the point every singleton is known to exist. Pre-existing,
 * not something this exception's own check changes.
 */
public final class CheckpointStorageCannotFenceSubscriptionException extends IllegalStateException {

    private final Class<? extends CheckpointStorage> storageType;
    private final List<String> unsupportedSubscriptionIds;

    CheckpointStorageCannotFenceSubscriptionException(Class<? extends CheckpointStorage> storageType, List<String> unsupportedSubscriptionIds) {
        super(("%s answers true to evaluatesWriteConditions() but false to evaluatesWriteConditionsFor(subscriptionId) " +
               "for %d subscription id(s) this application declares (%s), and a competing-consumer strategy is " +
               "wired, so Occurrent would stamp a checkpoint write for one of those ids with the lease version and " +
               "that storage refuses it with an exception specific to that storage on the first write after a lease " +
               "is acquired. Change the affected subscription id(s) to a shape the storage accepts, use a %s that " +
               "evaluates write conditions for them, or set " +
               "occurrent.subscription.competing-consumer.fence-checkpoints=false.")
                .formatted(storageType.getName(), unsupportedSubscriptionIds.size(), String.join(", ", unsupportedSubscriptionIds), CheckpointStorage.class.getSimpleName()));
        this.storageType = storageType;
        this.unsupportedSubscriptionIds = List.copyOf(unsupportedSubscriptionIds);
    }

    /**
     * The type of the {@link CheckpointStorage} bean that cannot evaluate a write condition for every declared
     * subscription id.
     *
     * @return The storage type.
     */
    public Class<? extends CheckpointStorage> getStorageType() {
        return storageType;
    }

    /**
     * The subscription ids {@link CheckpointStorage#evaluatesWriteConditionsFor(String)} answered {@code false}
     * for, sorted.
     *
     * @return The unsupported subscription ids.
     */
    public List<String> getUnsupportedSubscriptionIds() {
        return unsupportedSubscriptionIds;
    }
}
