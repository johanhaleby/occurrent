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

/**
 * Thrown when a {@link CompetingConsumerStrategy} bean is wired next to a {@link CheckpointStorage} that answers
 * {@code false} to {@link CheckpointStorage#evaluatesWriteConditions()}. The first checkpoint write that pair makes
 * after a lease is acquired would be refused with {@link UnsupportedOperationException}.
 */
public final class CheckpointStorageCannotFenceException extends IllegalStateException {

    private final Class<? extends CheckpointStorage> storageType;

    CheckpointStorageCannotFenceException(Class<? extends CheckpointStorage> storageType) {
        super(("%s answers false to evaluatesWriteConditions(), and a competing-consumer strategy is wired, so " +
               "Occurrent would stamp every checkpoint write with the lease version and that storage refuses it with " +
               "UnsupportedOperationException on the first write after a lease is acquired. Either use a %s that " +
               "evaluates write conditions, or set occurrent.subscription.competing-consumer.fence-checkpoints=false. " +
               "Writing every checkpoint unconditionally lets a node that has already lost its lease move a " +
               "checkpoint backwards.")
                .formatted(storageType.getName(), CheckpointStorage.class.getSimpleName()));
        this.storageType = storageType;
    }

    /**
     * The type of the {@link CheckpointStorage} bean that cannot evaluate a write condition.
     *
     * @return The storage type.
     */
    public Class<? extends CheckpointStorage> getStorageType() {
        return storageType;
    }
}
