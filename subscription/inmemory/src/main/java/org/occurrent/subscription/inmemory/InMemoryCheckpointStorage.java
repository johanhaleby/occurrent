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
import org.occurrent.subscription.CatchupMarker;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.api.blocking.CatchupMarkerStorage;
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
 * <p>
 * As a {@link CatchupMarkerStorage} it keeps the generation a marker was saved with next to the checkpoint, and the
 * generation of each subscription id in a map of its own. The generation is a counter that each forget raises by one.
 */
@NullMarked
public class InMemoryCheckpointStorage implements CatchupMarkerStorage {

    private final Map<String, Checkpoint> checkpoints = new ConcurrentHashMap<>();
    private final Map<String, Long> versions = new ConcurrentHashMap<>();
    // The generation each marker was saved with. A checkpoint without one counts as saved with generation 0
    private final Map<String, Long> stamps = new ConcurrentHashMap<>();
    private final Map<String, Long> generations = new ConcurrentHashMap<>();
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
            saveUnderLock(subscriptionId, checkpoint, condition);
            stamps.remove(subscriptionId);
            return checkpoint;
        } finally {
            lock.unlock();
        }
    }

    // Throws CheckpointWriteConditionNotFulfilledException when condition refuses the write, and writes nothing then
    private void saveUnderLock(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
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
    }

    @Override
    public CatchupMarker readCatchupMarker(String subscriptionId) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        lock.lock();
        try {
            long generation = generations.getOrDefault(subscriptionId, 0L);
            Checkpoint marker = checkpoints.get(subscriptionId);
            boolean present = marker != null && stamps.getOrDefault(subscriptionId, 0L) == generation;
            return new CatchupMarker(present ? marker : null, generation);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean saveCatchupMarker(String subscriptionId, Checkpoint marker, long generation, CheckpointWriteCondition condition) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(marker, Checkpoint.class.getSimpleName() + " cannot be null");
        requireNonNull(condition, CheckpointWriteCondition.class.getSimpleName() + " cannot be null");
        lock.lock();
        try {
            saveUnderLock(subscriptionId, marker, condition);
            stamps.put(subscriptionId, generation);
            return true;
        } catch (CheckpointWriteConditionNotFulfilledException e) {
            return false;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void forgetCatchupMarker(String subscriptionId) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        lock.lock();
        try {
            generations.merge(subscriptionId, 1L, Long::sum);
            checkpoints.remove(subscriptionId);
            versions.remove(subscriptionId);
            stamps.remove(subscriptionId);
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
            stamps.remove(subscriptionId);
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
