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

package org.occurrent.subscription.inmemory;

import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.tck.subscription.blocking.CheckpointStorageConformance;
import org.occurrent.tck.subscription.blocking.CheckpointStorageFixture;

class InMemoryCheckpointStorageConformanceTest extends CheckpointStorageConformance {

    @Override
    protected CheckpointStorageFixture createFixture() {
        return new InMemoryCheckpointStorageFixture();
    }

    /**
     * A fresh instance per test is all the cleanup a map needs, so there is nothing to close.
     */
    private static class InMemoryCheckpointStorageFixture implements CheckpointStorageFixture {

        private final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();

        @Override
        public CheckpointStorage checkpointStorage() {
            return storage;
        }

        /**
         * The map holds the {@link Checkpoint} it was handed, so every type comes back as itself. This is the only
         * storage that can promise that, since the others have to encode the checkpoint to store it.
         */
        @Override
        public boolean preservesCheckpointType(Checkpoint checkpoint) {
            return true;
        }
    }
}
