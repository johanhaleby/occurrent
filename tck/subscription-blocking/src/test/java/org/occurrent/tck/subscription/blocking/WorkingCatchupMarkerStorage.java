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
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.api.blocking.CatchupMarkerStorage;

import java.util.HashMap;
import java.util.Map;

/**
 * A catch-up marker storage that honours the whole contract, so that {@link SuiteNeverSkipsTest} can run
 * {@link CatchupMarkerStorageConformance} green. A copy of the in-memory storage, for the same reason as
 * {@link WorkingCheckpointStorage}.
 */
class WorkingCatchupMarkerStorage extends WorkingCheckpointStorage implements CatchupMarkerStorage {

    private final Map<String, Long> stamps = new HashMap<>();
    private final Map<String, Long> generations = new HashMap<>();

    @Override
    public CatchupMarker readCatchupMarker(String subscriptionId) {
        long generation = generations.getOrDefault(subscriptionId, 0L);
        Checkpoint marker = read(subscriptionId);
        boolean present = marker != null && stamps.getOrDefault(subscriptionId, 0L) == generation;
        return new CatchupMarker(present ? marker : null, generation);
    }

    @Override
    public boolean saveCatchupMarker(String subscriptionId, Checkpoint marker, long generation, CheckpointWriteCondition condition) {
        try {
            super.save(subscriptionId, marker, condition);
        } catch (CheckpointWriteConditionNotFulfilledException e) {
            return false;
        }
        stamps.put(subscriptionId, generation);
        return true;
    }

    @Override
    public void forgetCatchupMarker(String subscriptionId) {
        generations.merge(subscriptionId, 1L, Long::sum);
        delete(subscriptionId);
    }

    @Override
    public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
        Checkpoint saved = super.save(subscriptionId, checkpoint, condition);
        stamps.remove(subscriptionId);
        return saved;
    }

    @Override
    public void delete(String subscriptionId) {
        super.delete(subscriptionId);
        stamps.remove(subscriptionId);
    }
}
