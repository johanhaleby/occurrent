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

package org.occurrent.subscription.inmemory;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * A {@link CheckpointStorage} that keeps checkpoints in a {@link ConcurrentHashMap}. It has nothing to connect to
 * and nothing to clean up, which makes it a good fit for tests and for small applications that don't need the
 * checkpoint to survive a restart.
 */
@NullMarked
public class InMemoryCheckpointStorage implements CheckpointStorage {

    private final Map<String, Checkpoint> checkpoints = new ConcurrentHashMap<>();

    @Override
    @Nullable
    public Checkpoint read(String subscriptionId) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        return checkpoints.get(subscriptionId);
    }

    @Override
    public Checkpoint save(String subscriptionId, Checkpoint checkpoint) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(checkpoint, Checkpoint.class.getSimpleName() + " cannot be null");
        checkpoints.put(subscriptionId, checkpoint);
        return checkpoint;
    }

    @Override
    public void delete(String subscriptionId) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        checkpoints.remove(subscriptionId);
    }

    @Override
    public boolean exists(String subscriptionId) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        return checkpoints.containsKey(subscriptionId);
    }
}
