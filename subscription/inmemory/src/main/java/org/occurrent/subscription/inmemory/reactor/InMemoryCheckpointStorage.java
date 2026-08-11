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

package org.occurrent.subscription.inmemory.reactor;

import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Objects.requireNonNull;

/**
 * A reactive {@link CheckpointStorage} that keeps checkpoints in a {@link ConcurrentHashMap}. It has nothing to
 * connect to and nothing to clean up, which makes it a good fit for tests and for small applications that don't
 * need the checkpoint to survive a restart.
 * <p>
 * This is the reactive twin of {@link org.occurrent.subscription.inmemory.InMemoryCheckpointStorage}. The two
 * interfaces differ (the blocking one has an {@code exists}), so the same simple name in a {@code reactor}
 * subpackage mirrors how {@code org.occurrent.subscription.api.blocking} and {@code org.occurrent.subscription.api.reactor}
 * already name their halves.
 * <p>
 * Every returned {@code Mono} is cold, so nothing is read, stored, or deleted until it is subscribed to. Arguments are
 * still validated eagerly, so a {@code null} fails the calling code and not a subscriber far away.
 * <p>
 * {@link CheckpointWriteCondition} is evaluated for real, not refused. The checkpoint and its version are two
 * separate maps, since {@link CheckpointWriteCondition#any()} writes the former and leaves the latter untouched. A
 * refusal signals {@link Mono#error(Throwable)} rather than throwing from assembly.
 */
@NullMarked
public class InMemoryCheckpointStorage implements CheckpointStorage {

    private final Map<String, Checkpoint> checkpoints = new ConcurrentHashMap<>();
    private final Map<String, Long> versions = new ConcurrentHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();

    @Override
    public Mono<Checkpoint> read(String subscriptionId) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        return Mono.fromSupplier(() -> checkpoints.get(subscriptionId));
    }

    @Override
    public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(checkpoint, Checkpoint.class.getSimpleName() + " cannot be null");
        requireNonNull(condition, CheckpointWriteCondition.class.getSimpleName() + " cannot be null");
        return Mono.fromSupplier(() -> {
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
        });
    }

    @Override
    public boolean evaluatesWriteConditions() {
        return true;
    }

    @Override
    public Mono<Long> writeVersion(String subscriptionId) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        return Mono.fromSupplier(() -> versions.get(subscriptionId));
    }

    @Override
    public Mono<Void> delete(String subscriptionId) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        return Mono.fromRunnable(() -> {
            lock.lock();
            try {
                checkpoints.remove(subscriptionId);
                versions.remove(subscriptionId);
            } finally {
                lock.unlock();
            }
        });
    }
}
