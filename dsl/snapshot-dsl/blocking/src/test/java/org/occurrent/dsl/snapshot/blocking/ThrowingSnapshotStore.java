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

import org.occurrent.dsl.snapshot.Snapshot;

import java.util.Optional;

/** A test double whose {@code save} always throws, used to assert how the facades handle a store failure. */
final class ThrowingSnapshotStore<S> implements SnapshotStore<S> {
    @Override
    public Optional<Snapshot<S>> findLatest(String key) {
        return Optional.empty();
    }

    @Override
    public void save(String key, Snapshot<S> snapshot) {
        throw new RuntimeException("snapshot store save failed (test double)");
    }
}
