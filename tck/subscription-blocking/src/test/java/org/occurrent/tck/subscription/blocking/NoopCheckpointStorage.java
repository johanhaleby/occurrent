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

import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;

/**
 * A checkpoint storage that honours none of the contract. Run a suite against it and every single test must fail.
 * Nothing may pass, and nothing may be skipped. That is what {@link SuiteNeverSkipsTest} asserts, and it is the only
 * mechanical check that the suite really does refuse to skip rather than relying on nobody reaching for
 * {@code Assumptions}.
 * <p>
 * Every method throws rather than answering emptily, and that matters more than it looks. A {@code read} returning
 * {@code null} and an {@code exists} returning {@code false} would make three of the suite's tests pass against a
 * storage that stores nothing at all, since those are the answers an empty storage legitimately gives.
 */
class NoopCheckpointStorage implements CheckpointStorage {

    static final NoopCheckpointStorage INSTANCE = new NoopCheckpointStorage();

    private NoopCheckpointStorage() {
    }

    @Override
    public Checkpoint read(String subscriptionId) {
        throw new UnsupportedOperationException("NoopCheckpointStorage implements nothing on purpose");
    }

    @Override
    public Checkpoint save(String subscriptionId, Checkpoint checkpoint) {
        throw new UnsupportedOperationException("NoopCheckpointStorage implements nothing on purpose");
    }

    @Override
    public void delete(String subscriptionId) {
        throw new UnsupportedOperationException("NoopCheckpointStorage implements nothing on purpose");
    }

    @Override
    public boolean exists(String subscriptionId) {
        throw new UnsupportedOperationException("NoopCheckpointStorage implements nothing on purpose");
    }
}
