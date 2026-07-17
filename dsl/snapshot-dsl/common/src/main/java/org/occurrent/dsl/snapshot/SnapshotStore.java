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

package org.occurrent.dsl.snapshot;

import org.jspecify.annotations.Nullable;

import java.util.Optional;

/**
 * Stores and retrieves the latest {@link Snapshot} for a key, the storage-neutral seam of the snapshot DSL.
 * <p>
 * The key identifies what the snapshot is a fold of: the stream id for the stream path, or a stable
 * criteria/tag-derived key for the DCB path. An implementation keeps at most the latest snapshot per key and overwrites
 * it on {@link #save}.
 *
 * @param <S> the state type stored in the snapshot
 */
public interface SnapshotStore<S extends @Nullable Object> {

    /**
     * @param key the snapshot key
     * @return the latest snapshot for the key, or {@link Optional#empty()} if none was ever saved
     */
    Optional<Snapshot<S>> findLatest(String key);

    /**
     * Saves the snapshot for the key, replacing any previous snapshot for that key.
     */
    void save(String key, Snapshot<S> snapshot);

    /**
     * Removes the snapshot for the key, if any. Used for the authoritative "closing the books" path where a period is
     * closed and its snapshot no longer applies. Defaults to failing loud so a store that cannot delete does not
     * silently keep a snapshot the caller believes gone.
     */
    default void delete(String key) {
        throw new UnsupportedOperationException(getClass().getName() + " does not support delete(...)");
    }

    /**
     * An in-memory {@link SnapshotStore}, useful for tests, examples, and single-node best-effort caching.
     */
    static <S extends @Nullable Object> SnapshotStore<S> inMemory() {
        return new InMemorySnapshotStore<>();
    }
}
