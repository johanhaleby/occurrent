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

package org.occurrent.dsl.snapshot.reactor;

import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.snapshot.Snapshot;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * A thread-safe, in-memory {@link ReactiveSnapshotStore} backed by a {@link ConcurrentHashMap}. Keeps only the latest
 * snapshot per key.
 *
 * @param <S> the state type stored in the snapshot
 */
final class InMemoryReactiveSnapshotStore<S extends @Nullable Object> implements ReactiveSnapshotStore<S> {

    private final Map<String, Snapshot<S>> snapshots = new ConcurrentHashMap<>();

    @Override
    public Mono<Snapshot<S>> findLatest(String key) {
        return Mono.fromSupplier(() -> snapshots.get(requireNonNull(key, "key cannot be null")));
    }

    @Override
    public Mono<Void> save(String key, Snapshot<S> snapshot) {
        requireNonNull(key, "key cannot be null");
        requireNonNull(snapshot, "snapshot cannot be null");
        return Mono.fromRunnable(() -> snapshots.put(key, snapshot));
    }

    @Override
    public Mono<Void> delete(String key) {
        requireNonNull(key, "key cannot be null");
        return Mono.fromRunnable(() -> snapshots.remove(key));
    }
}
