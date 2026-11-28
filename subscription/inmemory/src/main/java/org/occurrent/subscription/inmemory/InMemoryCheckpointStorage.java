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
import java.util.Optional;
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
 * {@link #resolveFirstCheckpointRace(String, Checkpoint, long)} takes the same lock as a save, so a first position and
 * its write version are stored together, and a checkpoint already stored is never overwritten. So does
 * {@link #storeWriteVersion(String, long)}.
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

    /**
     * Settles the first position under the same lock as {@link #save(String, Checkpoint, CheckpointWriteCondition)}.
     * When no checkpoint is stored, {@code candidate} is stored with {@code writeVersion} and answered. When the
     * checkpoint stored is {@code candidate} itself, {@code writeVersion} is stored with it and it is answered. This
     * storage can't tell which of two positions is earlier, so when another checkpoint is stored, nothing is written
     * and this answers empty.
     *
     * @throws CheckpointWriteConditionNotFulfilledException if the stored version is newer than {@code writeVersion},
     *                                                       and then nothing is written
     * @throws IllegalArgumentException                      if {@code writeVersion} is negative
     */
    @Override
    public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate, long writeVersion) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(candidate, Checkpoint.class.getSimpleName() + " cannot be null");
        CheckpointWriteCondition condition = CheckpointWriteCondition.notOlderThan(writeVersion);
        lock.lock();
        try {
            Long stored = versions.get(subscriptionId);
            if (stored != null && stored > writeVersion) {
                throw new CheckpointWriteConditionNotFulfilledException(subscriptionId, OptionalLong.of(stored), condition);
            }
            Checkpoint storedCheckpoint = checkpoints.get(subscriptionId);
            if (storedCheckpoint == null) {
                checkpoints.put(subscriptionId, candidate);
            } else if (!storedCheckpoint.asString().equals(candidate.asString())) {
                return Optional.empty();
            }
            versions.put(subscriptionId, writeVersion);
            return Optional.of(candidate);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Stores {@code writeVersion} with the checkpoint stored, under the same lock as
     * {@link #save(String, Checkpoint, CheckpointWriteCondition)}, and answers that checkpoint. When nothing is
     * stored, nothing is written and this answers empty.
     *
     * @throws CheckpointWriteConditionNotFulfilledException if the stored version is newer than {@code writeVersion},
     *                                                       and then nothing is written
     * @throws IllegalArgumentException                      if {@code writeVersion} is negative
     */
    @Override
    public Optional<Checkpoint> storeWriteVersion(String subscriptionId, long writeVersion) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        CheckpointWriteCondition condition = CheckpointWriteCondition.notOlderThan(writeVersion);
        lock.lock();
        try {
            Long stored = versions.get(subscriptionId);
            if (stored != null && stored > writeVersion) {
                throw new CheckpointWriteConditionNotFulfilledException(subscriptionId, OptionalLong.of(stored), condition);
            }
            Checkpoint storedCheckpoint = checkpoints.get(subscriptionId);
            if (storedCheckpoint == null) {
                return Optional.empty();
            }
            versions.put(subscriptionId, writeVersion);
            return Optional.of(storedCheckpoint);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean evaluatesWriteConditions() {
        return true;
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
