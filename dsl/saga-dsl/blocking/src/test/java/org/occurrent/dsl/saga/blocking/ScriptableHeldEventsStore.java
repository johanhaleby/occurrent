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
import org.occurrent.dsl.saga.HeldEvent;
import org.occurrent.dsl.saga.HeldEventRef;
import org.occurrent.dsl.saga.InMemorySagaStateStore;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaHeldEvents;
import org.occurrent.dsl.saga.SagaStateStore;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * A store that holds events by delegating to the in-memory one, and that a test can make lose the writes a failure
 * path makes without the state, refuse the mark that adds a self-held key as the size limit of a document would, or
 * run something just before a compare-and-set that then loses.
 */
final class ScriptableHeldEventsStore<S> implements SagaStateStore<S>, SagaHeldEvents<S> {

    private final InMemorySagaStateStore<S> delegate = new InMemorySagaStateStore<>();
    private volatile boolean losingEveryWriteWithoutState;
    private volatile boolean refusingMarksCarryingAKey;
    private volatile @Nullable Runnable beforeNextCompareAndSave;

    InMemorySagaStateStore<S> delegate() {
        return delegate;
    }

    void loseEveryWriteWithoutState(boolean losing) {
        this.losingEveryWriteWithoutState = losing;
    }

    void refuseMarksCarryingAKey(boolean refusing) {
        this.refusingMarksCarryingAKey = refusing;
    }

    /** Runs once, just before the next compare-and-save, which then loses. */
    void beforeNextCompareAndSave(Runnable action) {
        this.beforeNextCompareAndSave = action;
    }

    @Override
    public Optional<SagaEnvelope<S>> find(String sagaId) {
        return delegate.find(sagaId);
    }

    @Override
    public Optional<SagaEnvelope<S>> findWithoutState(String sagaId) {
        return delegate.findWithoutState(sagaId);
    }

    @Override
    public boolean compareAndSave(String sagaId, SagaEnvelope<S> envelope, long expectedVersion) {
        Runnable before = beforeNextCompareAndSave;
        if (before != null) {
            beforeNextCompareAndSave = null;
            before.run();
            return false;
        }
        return delegate.compareAndSave(sagaId, envelope, expectedVersion);
    }

    @Override
    public boolean compareAndSaveWithoutState(String sagaId, SagaEnvelope<S> envelope, long expectedVersion) {
        if (losingEveryWriteWithoutState) {
            return false;
        }
        return delegate.compareAndSaveWithoutState(sagaId, envelope, expectedVersion);
    }

    @Override
    public List<SagaEnvelope<S>> findWithDueTimers(Instant now, int limit) {
        return delegate.findWithDueTimers(now, limit);
    }

    @Override
    public void delete(String sagaId) {
        delegate.delete(sagaId);
    }

    @Override
    public boolean hold(String sagaId, HeldEvent event) {
        return delegate.hold(sagaId, event);
    }

    @Override
    public boolean markOutstanding(String sagaId, String key) {
        return delegate.markOutstanding(sagaId, key);
    }

    @Override
    public Optional<SagaEnvelope<S>> markHeldEvents(String sagaId, @Nullable String selfHeldKey) {
        if (selfHeldKey != null && refusingMarksCarryingAKey) {
            throw new RefusedForSizeException("The instance is at the document limit", new IllegalStateException("document too large"));
        }
        return delegate.markHeldEvents(sagaId, selfHeldKey);
    }

    @Override
    public void unmarkSelfHeld(String sagaId, String selfHeldKey) {
        delegate.unmarkSelfHeld(sagaId, selfHeldKey);
    }

    @Override
    public List<HeldEventRef> heldIndex(String sagaId) {
        return delegate.heldIndex(sagaId);
    }

    @Override
    public List<HeldEvent> held(String sagaId, Collection<String> keys) {
        return delegate.held(sagaId, keys);
    }

    @Override
    public void release(String sagaId, String key) {
        delegate.release(sagaId, key);
    }
}
