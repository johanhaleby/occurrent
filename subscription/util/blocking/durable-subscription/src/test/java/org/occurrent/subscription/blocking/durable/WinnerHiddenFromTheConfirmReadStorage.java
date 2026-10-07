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

package org.occurrent.subscription.blocking.durable;

import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.api.blocking.CheckpointStorage;

import java.util.OptionalLong;

/**
 * Holds nothing until the first {@code ifAbsent()} write arrives, at which point another node's position is written
 * first and the write is refused. {@link #resolveFirstCheckpointRace} keeps the default empty answer, and every
 * read after that refusal fails or finds nothing until {@link #answersReadsAgain()}, standing in for a store that is
 * briefly unreachable, or a replica that has not seen the other node's write yet.
 */
final class WinnerHiddenFromTheConfirmReadStorage implements CheckpointStorage {

    enum ConfirmRead {FAILS, FINDS_NOTHING}

    private final ConfirmRead confirmRead;
    private final Checkpoint positionTheOtherNodeStores;
    private @Nullable Checkpoint stored;
    private boolean answersReads = true;
    private int readsThatHidTheStoredPosition = 0;

    WinnerHiddenFromTheConfirmReadStorage(ConfirmRead confirmRead, Checkpoint positionTheOtherNodeStores) {
        this.confirmRead = confirmRead;
        this.positionTheOtherNodeStores = positionTheOtherNodeStores;
    }

    synchronized void answersReadsAgain() {
        answersReads = true;
    }

    synchronized int readsThatHidTheStoredPosition() {
        return readsThatHidTheStoredPosition;
    }

    @Override
    public synchronized @Nullable Checkpoint read(String subscriptionId) {
        if (stored == null || answersReads) {
            return stored;
        }
        readsThatHidTheStoredPosition++;
        if (confirmRead == ConfirmRead.FAILS) {
            throw new IllegalStateException("Checkpoint storage cannot be reached");
        }
        return null;
    }

    @Override
    public synchronized Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
        if (condition instanceof CheckpointWriteCondition.IfAbsent) {
            if (stored == null) {
                stored = positionTheOtherNodeStores;
                answersReads = false;
            }
            throw new CheckpointWriteConditionNotFulfilledException(subscriptionId, OptionalLong.empty(), condition);
        }
        stored = checkpoint;
        return checkpoint;
    }

    @Override
    public boolean evaluatesWriteConditions() {
        return true;
    }

    @Override
    public OptionalLong writeVersion(String subscriptionId) {
        return OptionalLong.empty();
    }

    @Override
    public synchronized void delete(String subscriptionId) {
        stored = null;
    }

    @Override
    public synchronized boolean exists(String subscriptionId) {
        return stored != null;
    }
}
