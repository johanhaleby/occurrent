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

package org.occurrent.tck.subscription.blocking;

import org.occurrent.subscription.CatchupMarker;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.api.blocking.CatchupMarkerStorage;

import java.util.OptionalLong;

/**
 * A catch-up marker storage that honours none of the contract. Every method throws, so every test of
 * {@link CatchupMarkerStorageConformance} must fail against it.
 */
class NoopCatchupMarkerStorage implements CatchupMarkerStorage {

    static final NoopCatchupMarkerStorage INSTANCE = new NoopCatchupMarkerStorage();

    private NoopCatchupMarkerStorage() {
    }

    @Override
    public CatchupMarker readCatchupMarker(String subscriptionId) {
        throw new UnsupportedOperationException("NoopCatchupMarkerStorage implements nothing on purpose");
    }

    @Override
    public boolean saveCatchupMarker(String subscriptionId, Checkpoint marker, long generation, CheckpointWriteCondition condition) {
        throw new UnsupportedOperationException("NoopCatchupMarkerStorage implements nothing on purpose");
    }

    @Override
    public void forgetCatchupMarker(String subscriptionId) {
        throw new UnsupportedOperationException("NoopCatchupMarkerStorage implements nothing on purpose");
    }

    @Override
    public Checkpoint read(String subscriptionId) {
        throw new UnsupportedOperationException("NoopCatchupMarkerStorage implements nothing on purpose");
    }

    @Override
    public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
        throw new UnsupportedOperationException("NoopCatchupMarkerStorage implements nothing on purpose");
    }

    @Override
    public OptionalLong writeVersion(String subscriptionId) {
        throw new UnsupportedOperationException("NoopCatchupMarkerStorage implements nothing on purpose");
    }

    @Override
    public void delete(String subscriptionId) {
        throw new UnsupportedOperationException("NoopCatchupMarkerStorage implements nothing on purpose");
    }

    @Override
    public boolean exists(String subscriptionId) {
        throw new UnsupportedOperationException("NoopCatchupMarkerStorage implements nothing on purpose");
    }
}
