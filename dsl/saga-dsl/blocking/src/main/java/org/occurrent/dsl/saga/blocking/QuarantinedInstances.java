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

package org.occurrent.dsl.saga.blocking;

import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.FailureRecord;

import java.time.Instant;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

/**
 * The two writes a release makes to a quarantined instance, tied to the store's state type so
 * {@link SagaSubscription}, which has no state type of its own, can make them.
 *
 * @param <S> the user state type
 */
final class QuarantinedInstances<S extends @Nullable Object> {

    private final SagaStateStore<S> stateStore;

    QuarantinedInstances(SagaStateStore<S> stateStore) {
        this.stateStore = requireNonNull(stateStore, "stateStore cannot be null");
    }

    /**
     * Mark {@code sagaId} released and answer the position to replay from, or empty when the instance is not
     * quarantined and there is nothing to release.
     * <p>
     * The mark goes in before the replay starts, not after. Clearing the record first, or marking it only once the
     * replay is running, both leave a window where a live event is applied to state that still has the gap in it, and
     * nothing downstream of that can tell the gap is there.
     */
    OptionalLong markReleased(String sagaId) {
        SagaEnvelope<S> current = stateStore.find(sagaId)
                .orElseThrow(() -> new IllegalStateException("Saga instance '" + sagaId + "' does not exist, so there is nothing to release"));
        FailureRecord<S> released = SagaExecutionSupport.onRelease(current, Instant.now());
        if (released == null) {
            return OptionalLong.empty();
        }
        if (!stateStore.compareAndSave(sagaId, released.envelope(), released.expectedVersion())) {
            throw new SagaConcurrencyException("Failed to release saga instance '" + sagaId + "' because it was written concurrently. Nothing writes to a quarantined instance, so this is either a second release racing this one or the instance leaving quarantine some other way. Read it again and decide.");
        }
        // onRelease built this envelope from the record, so the record is always there.
        return OptionalLong.of(requireNonNull(released.envelope().failure(), "a released envelope has a failure record").position());
    }

    /** Take the release mark back off {@code sagaId}, for a release whose replay could not be started after all. */
    void undoRelease(String sagaId) {
        stateStore.find(sagaId).ifPresent(current -> {
            FailureRecord<S> reverted = SagaExecutionSupport.onReleaseUndone(current, Instant.now());
            if (reverted != null) {
                // Best effort. A lost compare-and-set means something else already moved the instance on, which is a
                // better outcome than the one being restored, so there is nothing to do about it.
                stateStore.compareAndSave(sagaId, reverted.envelope(), reverted.expectedVersion());
            }
        });
    }
}
