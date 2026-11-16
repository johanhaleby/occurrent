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
import org.occurrent.dsl.saga.codec.EncodedSaga;
import org.occurrent.dsl.saga.codec.SagaCodec;
import org.occurrent.dsl.saga.codec.SagaStateCodec;
import org.occurrent.dsl.saga.codec.StoredSaga;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

/**
 * An in-memory {@link SagaStateStore} backed by a {@link ConcurrentHashMap}, for tests and single-node use. The
 * compare-and-set save is atomic per instance via {@link ConcurrentMap#compute}. It keeps the state object as it is, and
 * keeps every other field in the form {@link SagaCodec} gives it, so an instance goes through the codec on every save
 * and read as it does in any other store.
 *
 * @param <S> the user state type
 */
public final class InMemorySagaStateStore<S extends @Nullable Object> implements SagaStateStore<S>, SagaStateStoreQueries<S>, SagaStateStoreSkippedInputs {

    private final ConcurrentMap<String, StoredSaga> store = new ConcurrentHashMap<>();
    // One immutable map from key to record per instance and quarantine id, replaced whole on every write. A read then
    // takes every record written before it, so the lowest seq it returns is the lowest recorded, also while skips record
    // concurrently. Iterating one map shared by every instance could miss a record written before one it does see
    private final ConcurrentMap<Quarantine, Map<String, SkippedInput>> skippedInputs = new ConcurrentHashMap<>();

    private record Quarantine(String sagaId, String quarantineId) {
    }

    @Override
    public SagaStateCodec<S> stateCodec() {
        return SagaStateCodec.identity();
    }

    @Override
    public Optional<StoredSaga> find(String sagaId) {
        requireNonNull(sagaId, "sagaId cannot be null");
        return Optional.ofNullable(store.get(sagaId));
    }

    @Override
    public Optional<StoredSaga> findWithoutState(String sagaId) {
        return find(sagaId);
    }

    @Override
    public boolean compareAndSave(StoredSaga saga, long expectedVersion) {
        requireNonNull(saga, "saga cannot be null");
        return compareAndSet(saga, expectedVersion, existing -> indexed(saga.encoded(), saga));
    }

    @Override
    public boolean compareAndSaveWithoutState(StoredSaga saga, long expectedVersion) {
        requireNonNull(saga, "saga cannot be null");
        if (expectedVersion == 0) {
            throw new IllegalArgumentException("compareAndSaveWithoutState cannot insert saga " + saga.sagaId() + ", since it holds no state. Insert it with compareAndSave.");
        }
        return compareAndSet(saga, expectedVersion, existing -> {
            // A positive expected version only matches an instance that is stored
            Map<String, Object> stored = requireNonNull(existing).encoded().fields();
            // The stored state and the step derived from it stay where they are
            Map<String, Object> fields = new LinkedHashMap<>(saga.encoded().fields());
            for (String kept : List.of(SagaCodec.CURRENT_STEP, SagaCodec.STATE)) {
                if (stored.containsKey(kept)) {
                    fields.put(kept, stored.get(kept));
                }
            }
            return indexed(new EncodedSaga(fields), saga);
        });
    }

    // Derived from the encoded form rather than taken from the caller, so a query sees what a store that reads its index
    // back from the stored fields sees
    private static StoredSaga indexed(EncodedSaga encoded, StoredSaga saga) {
        return new StoredSaga(saga.sagaId(), saga.version(), encoded, SagaCodec.indexOf(encoded));
    }

    private boolean compareAndSet(StoredSaga saga, long expectedVersion, java.util.function.UnaryOperator<@Nullable StoredSaga> next) {
        boolean[] saved = {false};
        store.compute(saga.sagaId(), (id, existing) -> {
            long current = existing == null ? 0 : existing.version();
            if (current != expectedVersion) {
                return existing;
            }
            saved[0] = true;
            return next.apply(existing);
        });
        return saved[0];
    }

    @Override
    public List<StoredSaga> findWithDueTimers(Instant now, int limit) {
        requireNonNull(now, "now cannot be null");
        long nowMillis = now.toEpochMilli();
        Set<String> active = SagaCodec.storedStatuses(SagaStatus.ACTIVE);
        // Sorted before truncating, so the instances that have been due longest are the ones kept. Ties go by saga id so
        // the answer does not depend on the map's iteration order.
        return store.values().stream()
                // ACTIVE rather than "not completed", so a quarantined instance drops out of the poll and fires no timers.
                // Firing one would advance its state across the gap the quarantine exists to hold open.
                .filter(saga -> active.contains(saga.index().storedStatus()))
                .filter(saga -> saga.index().nextTimerDueAtEpochMilli() != null && saga.index().nextTimerDueAtEpochMilli() <= nowMillis)
                .sorted(Comparator.<StoredSaga>comparingLong(saga -> saga.index().nextTimerDueAtEpochMilli()).thenComparing(StoredSaga::sagaId))
                .limit(limit)
                .toList();
    }

    @Override
    public List<String> findReleasingSagaIds(@Nullable String after, int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be positive, was " + limit);
        }
        return store.values().stream()
                .filter(saga -> saga.index().releasing())
                .map(StoredSaga::sagaId)
                .filter(id -> after == null || id.compareTo(after) > 0)
                .sorted()
                .limit(limit)
                .toList();
    }

    @Override
    public void recordSkippedInput(String sagaId, String quarantineId, SkippedInput input) {
        requireNonNull(input, "input cannot be null");
        skippedInputs.compute(quarantine(sagaId, quarantineId), (ignored, records) ->
                records != null && records.containsKey(input.key()) ? records : with(records, input));
    }

    @Override
    public List<SkippedInput> findSkippedInputs(String sagaId, String quarantineId, int limit) {
        requireNonNull(sagaId, "sagaId cannot be null");
        requireNonNull(quarantineId, "quarantineId cannot be null");
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be positive, was " + limit);
        }
        Map<String, SkippedInput> records = skippedInputs.getOrDefault(new Quarantine(sagaId, quarantineId), Map.of());
        return records.values().stream()
                .sorted(SkippedInput.ORDER)
                .limit(limit)
                .toList();
    }

    @Override
    public void recordSkippedInputAtHead(String sagaId, String quarantineId, SkippedInput head) {
        requireNonNull(head, "head cannot be null");
        skippedInputs.compute(quarantine(sagaId, quarantineId), (ignored, records) -> with(records, head));
    }

    @Override
    public void deleteSkippedInput(String sagaId, String quarantineId, SkippedInput input) {
        requireNonNull(input, "input cannot be null");
        skippedInputs.computeIfPresent(quarantine(sagaId, quarantineId), (ignored, records) -> {
            if (!input.equals(records.get(input.key()))) {
                return records;
            }
            Map<String, SkippedInput> remaining = new HashMap<>(records);
            remaining.remove(input.key());
            return remaining.isEmpty() ? null : Map.copyOf(remaining);
        });
    }

    @Override
    public void deleteSkippedInputs(String sagaId, String quarantineId) {
        requireNonNull(sagaId, "sagaId cannot be null");
        requireNonNull(quarantineId, "quarantineId cannot be null");
        skippedInputs.remove(new Quarantine(sagaId, quarantineId));
    }

    private static Quarantine quarantine(String sagaId, String quarantineId) {
        return new Quarantine(requireNonNull(sagaId, "sagaId cannot be null"), requireNonNull(quarantineId, "quarantineId cannot be null"));
    }

    private static Map<String, SkippedInput> with(@Nullable Map<String, SkippedInput> records, SkippedInput input) {
        Map<String, SkippedInput> written = records == null ? new HashMap<>() : new HashMap<>(records);
        written.put(input.key(), input);
        return Map.copyOf(written);
    }

    @Override
    public List<StoredSaga> findByStatus(SagaStatus status, Instant updatedBefore, int limit) {
        requireNonNull(status, "status cannot be null");
        requireNonNull(updatedBefore, "updatedBefore cannot be null");
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be positive, was " + limit);
        }
        Set<String> statuses = SagaCodec.storedStatuses(status);
        long before = updatedBefore.toEpochMilli();
        // Sort before truncating, so "the first limit" is the stalest instances the contract promises
        return store.values().stream()
                .filter(saga -> statuses.contains(saga.index().storedStatus()))
                .filter(saga -> saga.index().updatedAtEpochMilli() != null && saga.index().updatedAtEpochMilli() < before)
                .sorted(Comparator.comparingLong(saga -> requireNonNull(saga.index().updatedAtEpochMilli())))
                .limit(limit)
                .toList();
    }

    @Override
    public void delete(String sagaId) {
        requireNonNull(sagaId, "sagaId cannot be null");
        StoredSaga removed = store.remove(sagaId);
        // Only the records of the instance removed here, so a later instance with the same id keeps the ones it wrote
        // under a quarantine id of its own
        String quarantineId = removed == null ? null : removed.index().quarantineId();
        if (quarantineId != null) {
            deleteSkippedInputs(sagaId, quarantineId);
        }
    }
}
