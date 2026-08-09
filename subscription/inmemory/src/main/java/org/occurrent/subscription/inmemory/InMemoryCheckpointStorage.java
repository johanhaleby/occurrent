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
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.api.blocking.CheckpointStorage;

import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Objects.requireNonNull;

/**
 * A {@link CheckpointStorage} that keeps checkpoints in a {@link ConcurrentHashMap}. It has nothing to connect to
 * and nothing to clean up, which makes it a good fit for tests and for small applications that don't need the
 * checkpoint to survive a restart.
 * <p>
 * {@link CheckpointWriteCondition} is evaluated for real, not refused. The checkpoint and its version are two
 * separate maps, since {@link CheckpointWriteCondition#any()} writes the former and leaves the latter untouched.
 */
@NullMarked
public class InMemoryCheckpointStorage implements CheckpointStorage {

    private final Map<String, Checkpoint> checkpoints = new ConcurrentHashMap<>();
    private final Map<String, Long> versions = new ConcurrentHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();

    @Override
    @Nullable
    public Checkpoint read(String subscriptionId) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        return checkpoints.get(subscriptionId);
    }

    @Override
    public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(checkpoint, Checkpoint.class.getSimpleName() + " cannot be null");
        requireNonNull(condition, CheckpointWriteCondition.class.getSimpleName() + " cannot be null");
        lock.lock();
        try {
            if (condition instanceof CheckpointWriteCondition.NotOlderThan notOlderThan) {
                Long stored = versions.get(subscriptionId);
                if (stored != null && stored > notOlderThan.writeVersion()) {
                    throw new CheckpointWriteConditionNotFulfilledException(subscriptionId, OptionalLong.of(stored), condition);
                }
                checkpoints.put(subscriptionId, checkpoint);
                versions.put(subscriptionId, notOlderThan.writeVersion());
            } else if (condition instanceof CheckpointWriteCondition.IfAbsent) {
                if (checkpoints.containsKey(subscriptionId)) {
                    Long stored = versions.get(subscriptionId);
                    OptionalLong storedVersion = stored == null ? OptionalLong.empty() : OptionalLong.of(stored);
                    throw new CheckpointWriteConditionNotFulfilledException(subscriptionId, storedVersion, condition);
                }
                checkpoints.put(subscriptionId, checkpoint);
            } else {
                // CheckpointWriteCondition.Any: the stored version, if any, is carried forward untouched.
                checkpoints.put(subscriptionId, checkpoint);
            }
            return checkpoint;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public OptionalLong writeVersion(String subscriptionId) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        Long stored = versions.get(subscriptionId);
        return stored == null ? OptionalLong.empty() : OptionalLong.of(stored);
    }

    @Override
    public void delete(String subscriptionId) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        lock.lock();
        try {
            checkpoints.remove(subscriptionId);
            versions.remove(subscriptionId);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean exists(String subscriptionId) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        return checkpoints.containsKey(subscriptionId);
    }
}
