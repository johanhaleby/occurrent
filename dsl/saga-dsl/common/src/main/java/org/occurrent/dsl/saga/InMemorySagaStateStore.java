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

package org.occurrent.dsl.saga;

import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

/**
 * An in-memory {@link SagaStateStore} backed by a {@link ConcurrentHashMap}, for tests and single-node use. The
 * compare-and-set save is atomic per instance via {@link ConcurrentMap#compute}.
 *
 * @param <S> the user state type
 */
public final class InMemorySagaStateStore<S extends @Nullable Object> implements SagaStateStore<S>, SagaStateStoreQueries<S> {

    private final ConcurrentMap<String, SagaEnvelope<S>> store = new ConcurrentHashMap<>();

    @Override
    public Optional<SagaEnvelope<S>> find(String sagaId) {
        requireNonNull(sagaId, "sagaId cannot be null");
        return Optional.ofNullable(store.get(sagaId));
    }

    @Override
    public boolean compareAndSave(String sagaId, SagaEnvelope<S> envelope, long expectedVersion) {
        requireNonNull(sagaId, "sagaId cannot be null");
        requireNonNull(envelope, "envelope cannot be null");
        boolean[] saved = {false};
        store.compute(sagaId, (id, existing) -> {
            long current = existing == null ? 0 : existing.version();
            if (current != expectedVersion) {
                return existing;
            }
            saved[0] = true;
            return envelope;
        });
        return saved[0];
    }

    @Override
    public List<SagaEnvelope<S>> findWithDueTimers(Instant now, int limit) {
        requireNonNull(now, "now cannot be null");
        long nowMillis = now.toEpochMilli();
        // Sorted before truncating, so the instances that have been due longest are the ones kept. Ties go by saga id so
        // the answer does not depend on the map's iteration order.
        return store.values().stream()
                // ACTIVE rather than "not completed", so a quarantined instance drops out of the poll and fires no timers.
                // Firing one would advance its state across the gap the quarantine exists to hold open.
                .filter(envelope -> envelope.status() == SagaStatus.ACTIVE)
                .filter(envelope -> {
                    OptionalLong earliest = envelope.earliestDueAtEpochMilli();
                    return earliest.isPresent() && earliest.getAsLong() <= nowMillis;
                })
                .sorted(Comparator.<SagaEnvelope<S>>comparingLong(envelope -> envelope.earliestDueAtEpochMilli().getAsLong())
                        .thenComparing(SagaEnvelope::sagaId))
                .limit(limit)
                .toList();
    }

    @Override
    public List<SagaEnvelope<S>> findByStatus(SagaStatus status, Instant updatedBefore, int limit) {
        requireNonNull(status, "status cannot be null");
        requireNonNull(updatedBefore, "updatedBefore cannot be null");
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be positive, was " + limit);
        }
        // Sort before truncating, so "the first limit" is the stalest instances the contract promises
        return store.values().stream()
                .filter(envelope -> envelope.status() == status)
                .filter(envelope -> {
                    Instant updatedAt = envelope.updatedAt();
                    return updatedAt != null && updatedAt.isBefore(updatedBefore);
                })
                .sorted(Comparator.comparing(envelope -> requireNonNull(envelope.updatedAt())))
                .limit(limit)
                .toList();
    }

    @Override
    public void delete(String sagaId) {
        requireNonNull(sagaId, "sagaId cannot be null");
        store.remove(sagaId);
    }
}
