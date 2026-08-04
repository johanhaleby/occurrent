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

import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;

import java.util.HashMap;
import java.util.Map;

/**
 * A checkpoint storage that honours the whole contract, so that {@link SuiteNeverSkipsTest} can run the suite green and
 * reach every line of it.
 * <p>
 * This is a copy of what {@code occurrent-subscription-inmemory} publishes, and it has to be, because that module runs
 * this suite and Maven refuses a dependency in both directions between two modules. A copy of nine lines is the cheaper
 * of the two, and it is only ever used to check the suite against itself.
 */
class WorkingCheckpointStorage implements CheckpointStorage {

    private final Map<String, Checkpoint> checkpoints = new HashMap<>();

    @Override
    public @Nullable Checkpoint read(String subscriptionId) {
        return checkpoints.get(subscriptionId);
    }

    @Override
    public Checkpoint save(String subscriptionId, Checkpoint checkpoint) {
        checkpoints.put(subscriptionId, checkpoint);
        return checkpoint;
    }

    @Override
    public void delete(String subscriptionId) {
        checkpoints.remove(subscriptionId);
    }

    @Override
    public boolean exists(String subscriptionId) {
        return checkpoints.containsKey(subscriptionId);
    }
}
