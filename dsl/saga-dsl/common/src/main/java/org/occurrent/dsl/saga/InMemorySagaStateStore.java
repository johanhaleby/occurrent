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

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.BytesCloudEventData;
import io.cloudevents.core.data.PojoCloudEventData;
import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.saga.codec.EncodedSaga;
import org.occurrent.dsl.saga.codec.SagaCodec;
import org.occurrent.dsl.saga.codec.SagaStateCodec;
import org.occurrent.dsl.saga.codec.StoredSaga;
import org.occurrent.dsl.saga.internal.SkippedInputData;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.time.Instant;
import java.time.OffsetDateTime;
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
 * <p>
 * The record of a skipped input holds a copy of its event, taken when it is recorded, and every read returns a copy of
 * that. Changing the delivered event afterwards, or an event a read returned, doesn't change the record. The copy
 * takes bytes, and a {@code Map} or a {@code List} at any depth that holds bytes, a {@code String}, a {@code Boolean}, a
 * {@code Character}, a number of a {@code java.lang} or {@code java.math} class, a {@code java.util.Date}, a
 * {@code UUID}, or a BSON value that {@code SpringMongoSagaStateStore} reads back as the same class, such as an
 * {@code ObjectId} or a {@code Decimal128}. It copies data that is a {@code BytesCloudEventData}, or a
 * {@code PojoCloudEventData} of exactly that class whose map or list it can copy, where every other value is of exactly
 * the class listed, so not a subclass. A {@code Map} and a {@code List} are the exception, and any implementation of them
 * is copied, as long as a map holds no key equal to another of its keys. An {@code IdentityHashMap} can hold two equal
 * keys, and a copy would keep only one of them.
 * <p>
 * Data it can't copy is held as its bytes, and every read rebuilds it the way {@code SpringMongoSagaStateStore} rebuilds
 * data it holds as bytes. A {@code JsonCloudEventData} comes back as a {@code JsonCloudEventData} read from those bytes
 * the way the CloudEvents JSON format with its default options reads them, whatever format the application registered,
 * with every decimal number read as a {@code BigDecimal} when a {@code double} can't hold one of them. Data of any other
 * class, such as a {@code PojoCloudEventData} of an object, comes back as a {@code BytesCloudEventData}.
 * {@link #compareAndSaveSkipped} throws an {@link IllegalStateException}, and records nothing, for an event with an
 * extension it can't copy, for data whose copy isn't of the class a release hands the converter, and for JSON data
 * that comes back with another value, such as a binary value as text.
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
        // Every write is one compute on the instance's id, so a transition is atomic
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
        SkippedInput held = copyOf(saga.sagaId(), input);
        return compareAndSet(saga, expectedVersion, withoutStateUnlessInsert(saga, expectedVersion), quarantineId,
                records -> records.containsKey(held.key()) ? records : with(records, held));
    }

    @Override
    public boolean compareAndSaveReplayed(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput replayed) {
        requireNonNull(saga, "saga cannot be null");
        requireNonNull(quarantineId, "quarantineId cannot be null");
        requireNonNull(replayed, "replayed cannot be null");
        // A replayed input that completes the instance ends its quarantine, so no record under it is replayed any more
        boolean completes = SagaStatus.COMPLETED.name().equals(saga.index().storedStatus());
        return compareAndSet(saga, expectedVersion, existing -> indexed(saga.encoded(), saga), quarantineId,
                records -> isStillRecorded(records, replayed), records -> {
            if (completes) {
                return Map.of();
            }
            Map<String, SkippedInput> remaining = new HashMap<>(records);
            remaining.remove(replayed.key());
            return Map.copyOf(remaining);
        });
    }

    // Another seq for the key means the input was recorded again after a release applied the record it read
    private static boolean isStillRecorded(Map<String, SkippedInput> records, SkippedInput replayed) {
        SkippedInput recorded = records.get(replayed.key());
        return recorded != null && recorded.seq() == replayed.seq();
    }

    @Override
    public boolean compareAndSaveQuarantineEnded(StoredSaga saga, long expectedVersion, String quarantineId) {
        requireNonNull(saga, "saga cannot be null");
        requireNonNull(quarantineId, "quarantineId cannot be null");
        // The quarantine ends only with no record left, as the SPI says, and the stored state stays where it is
        return compareAndSet(saga, expectedVersion, withoutState(saga, expectedVersion), quarantineId,
                Map::isEmpty, records -> Map.of());
    }

    /**
     * {@code true} for every instance, since a map in memory holds an instance of any size.
     */
    @Override
    public boolean fits(StoredSaga candidate) {
        requireNonNull(candidate, "candidate cannot be null");
        return true;
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
                .map(record -> copyOf(sagaId, record))
                .toList();
    }

    @Override
    public Optional<StoredSagaWithFirstSkippedInput> findWithFirstSkippedInput(String sagaId) {
        requireNonNull(sagaId, "sagaId cannot be null");
        // One get, so the instance and its records are the ones a single compute left
        Instance instance = instances.get(sagaId);
        StoredSaga saga = instance == null ? null : instance.saga();
        if (saga == null) {
            return Optional.empty();
        }
        String quarantineId = saga.index().quarantineId();
        SkippedInput first = quarantineId == null ? null : instance.records().getOrDefault(quarantineId, Map.of()).values().stream()
                .min(SkippedInput.ORDER)
                .map(record -> copyOf(sagaId, record))
                .orElse(null);
        return Optional.of(new StoredSagaWithFirstSkippedInput(saga, first));
    }

    @Override
    public Optional<StoredSagaWithFirstSkippedInputKey> findWithFirstSkippedInputKey(String sagaId) {
        requireNonNull(sagaId, "sagaId cannot be null");
        // One get, as findWithFirstSkippedInput reads, and no copy of the event
        Instance instance = instances.get(sagaId);
        StoredSaga saga = instance == null ? null : instance.saga();
        if (saga == null) {
            return Optional.empty();
        }
        String quarantineId = saga.index().quarantineId();
        StoredSagaWithFirstSkippedInputKey.RecordKey first = quarantineId == null ? null : instance.records().getOrDefault(quarantineId, Map.of()).values().stream()
                .min(SkippedInput.ORDER)
                .map(StoredSagaWithFirstSkippedInputKey.RecordKey::of)
                .orElse(null);
        return Optional.of(new StoredSagaWithFirstSkippedInputKey(saga, first));
    }

    private static final Set<Class<?>> UNCHANGEABLE = Set.of(String.class, Boolean.class, Character.class, Byte.class, Short.class,
            Integer.class, Long.class, Float.class, Double.class, BigInteger.class, BigDecimal.class);

    // The BSON values that SpringMongoSagaStateStore reads back from map data as the same class, each of them immutable.
    // Named, since this module doesn't depend on BSON. A CodeWithScope comes back too, but holds a Document that can change
    private static final Set<String> BSON_VALUES = Set.of("org.bson.types.ObjectId", "org.bson.types.Decimal128", "org.bson.types.Binary",
            "org.bson.types.Code", "org.bson.types.Symbol", "org.bson.types.MinKey", "org.bson.types.MaxKey", "org.bson.BsonTimestamp",
            "org.bson.BsonRegularExpression", "org.bson.BsonDbPointer", "org.bson.BsonUndefined");

    // A copy that shares nothing the producer of the event or a reader of the record can change
    private static SkippedInput copyOf(String sagaId, SkippedInput input) {
        CloudEvent event = input.event();
        CloudEventBuilder copy = CloudEventBuilder.fromSpecVersion(event.getSpecVersion()).withId(event.getId()).withSource(event.getSource()).withType(event.getType());
        if (event.getDataContentType() != null) {
            copy.withDataContentType(event.getDataContentType());
        }
        if (event.getDataSchema() != null) {
            copy.withDataSchema(event.getDataSchema());
        }
        if (event.getSubject() != null) {
            copy.withSubject(event.getSubject());
        }
        if (event.getTime() != null) {
            copy.withTime(event.getTime());
        }
        for (String name : event.getExtensionNames()) {
            switch (requireNonNull(event.getExtension(name))) {
                case String value -> copy.withExtension(name, value);
                case Boolean value -> copy.withExtension(name, value);
                case URI value -> copy.withExtension(name, value);
                case OffsetDateTime value -> copy.withExtension(name, value);
                case byte[] value -> copy.withExtension(name, value.clone());
                case Number value when UNCHANGEABLE.contains(value.getClass()) -> copy.withExtension(name, value);
                case Object value -> throw cannotCopy(sagaId, input, "the extension '" + name + "', a " + value.getClass().getName());
            }
        }
        CloudEventData data = event.getData();
        if (data != null) {
            CloudEventData held = copyOf(data);
            String difference = SkippedInputData.difference(data, held);
            if (difference != null) {
                throw new IllegalStateException("InMemorySagaStateStore cannot record the input '" + input.key() + "' that saga instance '" + sagaId + "' skipped. "
                                                + difference + ", so a release would hand the converter another event than the one it was delivered.");
            }
            copy.withData(held);
        }
        return new SkippedInput(input.seq(), input.key(), copy.build());
    }

    // Matched by exact class. A subclass would be stored as its superclass, and a release would hand the converter a
    // different data class than was delivered. Data it can't copy is held as its bytes and rebuilt from them on every
    // read, the way SpringMongoSagaStateStore holds data it can't read back as it was delivered
    private static CloudEventData copyOf(CloudEventData data) {
        if (data.getClass() == BytesCloudEventData.class) {
            return BytesCloudEventData.wrap(data.toBytes().clone());
        }
        if (data.getClass() == PojoCloudEventData.class && data instanceof PojoCloudEventData<?> pojo && (pojo.getValue() instanceof Map<?, ?> || pojo.getValue() instanceof List<?>)) {
            try {
                Object value = requireNonNull(copyOf(pojo.getValue()));
                byte[] bytes = pojo.toBytes().clone();
                return PojoCloudEventData.wrap(value, ignored -> bytes.clone());
            } catch (Uncopyable ignored) {
                // Held as its bytes below
            }
        }
        return SkippedInputData.fromBytes(SkippedInputData.classOf(data), data.toBytes().clone());
    }

    private static @Nullable Object copyOf(@Nullable Object value) {
        return switch (value) {
            case null -> null;
            case byte[] bytes -> bytes.clone();
            case Map<?, ?> map -> {
                Map<Object, @Nullable Object> copy = new LinkedHashMap<>();
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    Object key = entry.getKey();
                    if (key == null || !UNCHANGEABLE.contains(key.getClass())) {
                        throw new Uncopyable();
                    }
                    // A map that tells keys apart by more than equals, such as an IdentityHashMap, can hold two equal keys,
                    // and the copy would keep one of them
                    if (copy.containsKey(key)) {
                        throw new Uncopyable();
                    }
                    copy.put(key, copyOf(entry.getValue()));
                }
                yield copy;
            }
            case List<?> list -> {
                List<@Nullable Object> copy = new ArrayList<>(list.size());
                for (Object element : list) {
                    copy.add(copyOf(element));
                }
                yield copy;
            }
            case Date date when date.getClass() == Date.class -> new Date(date.getTime());
            case Object other when UNCHANGEABLE.contains(other.getClass()) || other instanceof UUID || BSON_VALUES.contains(other.getClass().getName()) -> other;
            case Object other -> throw new Uncopyable();
        };
    }

    // A value in the data the copy can't take, so the data is held as its bytes
    private static final class Uncopyable extends RuntimeException {
        private Uncopyable() {
            super(null, null, false, false);
        }
    }

    private static IllegalStateException cannotCopy(String sagaId, SkippedInput input, String what) {
        return new IllegalStateException("InMemorySagaStateStore cannot record the input '" + input.key() + "' that saga instance '" + sagaId
                                         + "' skipped, because it cannot copy " + what + ". The record holds a copy of the event, so that a change to the delivered event doesn't change the record.");
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
