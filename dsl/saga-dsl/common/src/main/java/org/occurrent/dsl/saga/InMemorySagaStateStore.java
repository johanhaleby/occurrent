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
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static java.util.Objects.requireNonNull;

/**
 * An in-memory {@link SagaStateStore} backed by a {@link ConcurrentHashMap}, for tests and single-node use. An instance
 * and its skipped-input records are one immutable value, and every write replaces that value in one
 * {@link ConcurrentHashMap#compute} on the instance's id. A read takes no lock and sees the instance and records of
 * exactly one write, and reads made one after another see the writes in the order they were made. It keeps every field
 * in the form {@link SagaCodec} gives it, so an instance goes through the codec on every save and read as it does in any
 * other store. The state of a core saga is that state object as it is, and the state of a flow saga is the map
 * {@link SagaCodec} encodes it to, which the codec rebuilds on every read.
 *
 * @param <S> the user state type
 */
public final class InMemorySagaStateStore<S extends @Nullable Object> implements SagaStateStore<S>, SagaStateStoreQueries<S>, SagaStateStoreSkippedInputs {

    private final ConcurrentMap<String, Instance> instances = new ConcurrentHashMap<>();

    // The records map from quarantine id to the records under it, each an immutable map from key to record. A read of the
    // records takes them whole, so the lowest seq it returns is the lowest recorded. The saga is null once a delete removed
    // it and left records under another quarantine id
    private record Instance(@Nullable StoredSaga saga, Map<String, Map<String, SkippedInput>> records) {
    }

    @Override
    public void requireAtomicTransitions() {
        // Every write takes the store's lock, so a transition is atomic
    }

    @Override
    public SagaStateCodec<S> stateCodec() {
        return SagaStateCodec.identity();
    }

    @Override
    public Optional<StoredSaga> find(String sagaId) {
        requireNonNull(sagaId, "sagaId cannot be null");
        return Optional.ofNullable(instances.get(sagaId)).map(Instance::saga);
    }

    @Override
    public Optional<StoredSaga> findWithoutState(String sagaId) {
        return find(sagaId);
    }

    @Override
    public boolean compareAndSave(StoredSaga saga, long expectedVersion) {
        requireNonNull(saga, "saga cannot be null");
        return compareAndSet(saga, expectedVersion, existing -> indexed(saga.encoded(), saga), null, UnaryOperator.identity());
    }

    @Override
    public boolean compareAndSaveWithoutState(StoredSaga saga, long expectedVersion) {
        requireNonNull(saga, "saga cannot be null");
        return compareAndSet(saga, expectedVersion, withoutState(saga, expectedVersion), null, UnaryOperator.identity());
    }

    @Override
    public boolean compareAndSaveSkipped(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput input) {
        requireNonNull(saga, "saga cannot be null");
        requireNonNull(quarantineId, "quarantineId cannot be null");
        requireNonNull(input, "input cannot be null");
        return compareAndSet(saga, expectedVersion, withoutStateUnlessInsert(saga, expectedVersion), quarantineId,
                records -> records.containsKey(input.key()) ? records : with(records, input));
    }

    @Override
    public boolean compareAndSaveReplayed(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput replayed) {
        requireNonNull(saga, "saga cannot be null");
        requireNonNull(quarantineId, "quarantineId cannot be null");
        requireNonNull(replayed, "replayed cannot be null");
        return compareAndSet(saga, expectedVersion, existing -> indexed(saga.encoded(), saga), quarantineId, records -> {
            if (!replayed.equals(records.get(replayed.key()))) {
                return records;
            }
            Map<String, SkippedInput> remaining = new HashMap<>(records);
            remaining.remove(replayed.key());
            return Map.copyOf(remaining);
        });
    }

    @Override
    public boolean compareAndSaveQuarantineEnded(StoredSaga saga, long expectedVersion, String quarantineId) {
        requireNonNull(saga, "saga cannot be null");
        requireNonNull(quarantineId, "quarantineId cannot be null");
        // An instance the release does not complete ends its quarantine only with no record left, as the SPI says
        boolean completes = SagaStatus.COMPLETED.name().equals(saga.index().storedStatus());
        return compareAndSet(saga, expectedVersion, existing -> indexed(saga.encoded(), saga), quarantineId,
                records -> completes || records.isEmpty(), records -> Map.of());
    }

    // The write that quarantines an instance whose first input failed inserts it, and an insert has no stored state to leave alone
    private static UnaryOperator<@Nullable StoredSaga> withoutStateUnlessInsert(StoredSaga saga, long expectedVersion) {
        return expectedVersion == 0 ? existing -> indexed(saga.encoded(), saga) : withoutState(saga, expectedVersion);
    }

    private static UnaryOperator<@Nullable StoredSaga> withoutState(StoredSaga saga, long expectedVersion) {
        if (expectedVersion == 0) {
            throw new IllegalArgumentException("compareAndSaveWithoutState cannot insert saga " + saga.sagaId() + ", since it holds no state. Insert it with compareAndSave.");
        }
        return existing -> {
            // A positive expected version only matches an instance that is stored
            Map<String, Object> stored = requireNonNull(existing).encoded().fields();
            // The stored state and the step derived from it stay as they are, and stay absent when nothing is stored
            Map<String, Object> fields = new LinkedHashMap<>(saga.encoded().fields());
            for (String kept : List.of(SagaCodec.CURRENT_STEP, SagaCodec.STATE)) {
                fields.remove(kept);
                if (stored.containsKey(kept)) {
                    fields.put(kept, stored.get(kept));
                }
            }
            return indexed(new EncodedSaga(fields), saga);
        };
    }

    // Derived from the encoded form rather than taken from the caller, so a query sees what a store that reads its index
    // back from the stored fields sees
    private static StoredSaga indexed(EncodedSaga encoded, StoredSaga saga) {
        return new StoredSaga(saga.sagaId(), saga.version(), encoded, SagaCodec.indexOf(encoded));
    }

    private boolean compareAndSet(StoredSaga saga, long expectedVersion, UnaryOperator<@Nullable StoredSaga> next, @Nullable String quarantineId,
                                  UnaryOperator<Map<String, SkippedInput>> writeRecords) {
        return compareAndSet(saga, expectedVersion, next, quarantineId, records -> true, writeRecords);
    }

    // Replaces the instance and the records under quarantineId in one compute, so a read sees both as one write left them.
    // Writes nothing when the records under quarantineId fail recordsAllow
    private boolean compareAndSet(StoredSaga saga, long expectedVersion, UnaryOperator<@Nullable StoredSaga> next, @Nullable String quarantineId,
                                  Predicate<Map<String, SkippedInput>> recordsAllow, UnaryOperator<Map<String, SkippedInput>> writeRecords) {
        boolean[] saved = {false};
        instances.compute(saga.sagaId(), (sagaId, existing) -> {
            StoredSaga stored = existing == null ? null : existing.saga();
            long current = stored == null ? 0 : stored.version();
            Map<String, Map<String, SkippedInput>> existingRecords = existing == null ? Map.of() : existing.records();
            if (current != expectedVersion || quarantineId != null && !recordsAllow.test(existingRecords.getOrDefault(quarantineId, Map.of()))) {
                return existing;
            }
            StoredSaga savedSaga = requireNonNull(next.apply(stored));
            Map<String, Map<String, SkippedInput>> records = existingRecords;
            if (quarantineId != null) {
                records = withRecords(records, quarantineId, writeRecords.apply(records.getOrDefault(quarantineId, Map.of())));
            }
            saved[0] = true;
            return new Instance(savedSaga, records);
        });
        return saved[0];
    }

    private static Map<String, Map<String, SkippedInput>> withRecords(Map<String, Map<String, SkippedInput>> records, String quarantineId, Map<String, SkippedInput> written) {
        Map<String, Map<String, SkippedInput>> next = new HashMap<>(records);
        if (written.isEmpty()) {
            next.remove(quarantineId);
        } else {
            next.put(quarantineId, written);
        }
        return Map.copyOf(next);
    }

    @Override
    public List<StoredSaga> findWithDueTimers(Instant now, int limit) {
        requireNonNull(now, "now cannot be null");
        long nowMillis = now.toEpochMilli();
        Set<String> active = SagaCodec.storedStatuses(SagaStatus.ACTIVE);
        // Sorted before truncating, so the instances that have been due longest are the ones kept. Ties go by saga id so
        // the answer does not depend on the map's iteration order.
        return sagas()
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
        return sagas()
                .filter(saga -> saga.index().releasing())
                .map(StoredSaga::sagaId)
                .filter(id -> after == null || id.compareTo(after) > 0)
                .sorted()
                .limit(limit)
                .toList();
    }

    @Override
    public List<SkippedInput> findSkippedInputs(String sagaId, String quarantineId, int limit) {
        requireNonNull(sagaId, "sagaId cannot be null");
        requireNonNull(quarantineId, "quarantineId cannot be null");
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be positive, was " + limit);
        }
        Instance instance = instances.get(sagaId);
        Map<String, SkippedInput> records = instance == null ? Map.of() : instance.records().getOrDefault(quarantineId, Map.of());
        return records.values().stream()
                .sorted(SkippedInput.ORDER)
                .limit(limit)
                .toList();
    }

    private static Map<String, SkippedInput> with(Map<String, SkippedInput> records, SkippedInput input) {
        Map<String, SkippedInput> written = new HashMap<>(records);
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
        return sagas()
                .filter(saga -> statuses.contains(saga.index().storedStatus()))
                .filter(saga -> saga.index().updatedAtEpochMilli() != null && saga.index().updatedAtEpochMilli() < before)
                .sorted(Comparator.comparingLong(saga -> requireNonNull(saga.index().updatedAtEpochMilli())))
                .limit(limit)
                .toList();
    }

    private java.util.stream.Stream<StoredSaga> sagas() {
        return instances.values().stream().map(Instance::saga).filter(Objects::nonNull);
    }

    // Removes the instance and the records under its quarantine id in one write
    @Override
    public void delete(String sagaId) {
        requireNonNull(sagaId, "sagaId cannot be null");
        instances.computeIfPresent(sagaId, (id, existing) -> {
            String quarantineId = existing.saga() == null ? null : existing.saga().index().quarantineId();
            Map<String, Map<String, SkippedInput>> records = quarantineId == null ? existing.records() : withRecords(existing.records(), quarantineId, Map.of());
            return records.isEmpty() ? null : new Instance(null, records);
        });
    }
}
