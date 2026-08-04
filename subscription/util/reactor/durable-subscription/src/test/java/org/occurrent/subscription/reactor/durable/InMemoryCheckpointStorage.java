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

package org.occurrent.subscription.reactor.durable;

import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The published {@code InMemoryCheckpointStorage} implements the blocking interface, which has an {@code exists} the
 * reactive one does not, so it cannot stand in here. See ADR 94.
 */
final class InMemoryCheckpointStorage implements CheckpointStorage {

    final Map<String, Checkpoint> checkpoints = new ConcurrentHashMap<>();

    @Override
    public Mono<Checkpoint> read(String subscriptionId) {
        return Mono.justOrEmpty(checkpoints.get(subscriptionId));
    }

    @Override
    public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint) {
        checkpoints.put(subscriptionId, checkpoint);
        return Mono.just(checkpoint);
    }

    @Override
    public Mono<Void> delete(String subscriptionId) {
        return Mono.fromRunnable(() -> checkpoints.remove(subscriptionId));
    }
}
