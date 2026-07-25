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

package org.occurrent.dsl.snapshot.blocking;

import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.snapshot.Snapshot;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

/**
 * A thread-safe, in-memory {@link SnapshotStore} backed by a {@link ConcurrentHashMap}. Suitable for tests, examples,
 * and single-node best-effort caching. State is not copied, so callers should treat stored state as immutable.
 *
 * @param <S> the state type
 */
public final class InMemorySnapshotStore<S extends @Nullable Object> implements SnapshotStore<S> {

    private final ConcurrentMap<String, Snapshot<S>> snapshots = new ConcurrentHashMap<>();

    @Override
    public Optional<Snapshot<S>> findLatest(String key) {
        requireNonNull(key, "key cannot be null");
        return Optional.ofNullable(snapshots.get(key));
    }

    @Override
    public void save(String key, Snapshot<S> snapshot) {
        requireNonNull(key, "key cannot be null");
        requireNonNull(snapshot, "snapshot cannot be null");
        snapshots.put(key, snapshot);
    }

    @Override
    public void delete(String key) {
        requireNonNull(key, "key cannot be null");
        snapshots.remove(key);
    }
}
