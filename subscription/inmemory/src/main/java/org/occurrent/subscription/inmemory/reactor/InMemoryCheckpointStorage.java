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
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
 * Every returned {@code Mono} is cold: nothing is read, stored or deleted until it is subscribed to. Arguments are
 * still validated eagerly, so a {@code null} fails the calling code and not a subscriber far away.
 */
@NullMarked
public class InMemoryCheckpointStorage implements CheckpointStorage {

    private final Map<String, Checkpoint> checkpoints = new ConcurrentHashMap<>();

    @Override
    public Mono<Checkpoint> read(String subscriptionId) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        return Mono.fromSupplier(() -> checkpoints.get(subscriptionId));
    }

    @Override
    public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(checkpoint, Checkpoint.class.getSimpleName() + " cannot be null");
        return Mono.fromSupplier(() -> {
            checkpoints.put(subscriptionId, checkpoint);
            return checkpoint;
        });
    }

    @Override
    public Mono<Void> delete(String subscriptionId) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        return Mono.fromRunnable(() -> checkpoints.remove(subscriptionId));
    }
}
