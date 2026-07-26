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

package org.occurrent.springboot.blocking;

import org.occurrent.dsl.snapshot.blocking.SnapshotStore;

/**
 * The zero-config snapshot store a store starter contributes for a {@code @Snapshot} that declares none. Optional: a
 * starter that contributes no such bean simply has no default, and a snapshot without a store bean fails with an
 * actionable message instead.
 * <p>
 * Where the snapshot is persisted, and under what collection or table name, is the implementation's business. This
 * module never names a store.
 */
public interface DefaultSnapshotStoreProvider {

    /**
     * @param snapshotId the {@code @Snapshot} id, so an implementation can derive a per-snapshot storage name
     * @param stateType  the snapshot state type, reflected from the factory return type
     */
    <S> SnapshotStore<S> createDefaultSnapshotStore(String snapshotId, Class<S> stateType);
}
