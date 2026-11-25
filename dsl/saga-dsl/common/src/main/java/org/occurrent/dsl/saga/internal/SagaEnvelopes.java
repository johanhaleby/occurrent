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

package org.occurrent.dsl.saga.internal;

import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStateStoreQueries;
import org.occurrent.dsl.saga.SagaStateStoreSkippedInputs;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.SkippedInput;
import org.occurrent.dsl.saga.StoredSagaWithFirstSkippedInputKey;
import org.occurrent.dsl.saga.codec.EncodedSaga;
import org.occurrent.dsl.saga.codec.SagaCodec;
import org.occurrent.dsl.saga.codec.StoredSaga;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * A {@link SagaStateStore} seen as the envelopes the executor works with. Every read decodes and every write encodes
 * through one {@link SagaCodec}, so the executor never handles a stored form.
 *
 * @param <S> the user state type
 */
public final class SagaEnvelopes<S extends @Nullable Object> {

    private final SagaStateStore<S> store;
    private final SagaCodec<S> codec;

    private SagaEnvelopes(SagaStateStore<S> store, SagaCodec<S> codec) {
        this.store = store;
        this.codec = codec;
    }

    /**
     * @param store      the store to read and write
     * @param flowEvents the converter for the received events of a flow saga, or {@code null} for a saga that is not a
     *                   flow saga and for reads that never touch the state
     */
    public static <S extends @Nullable Object> SagaEnvelopes<S> of(SagaStateStore<S> store, @Nullable CloudEventConverter<?> flowEvents) {
        requireNonNull(store, "stateStore cannot be null");
        return new SagaEnvelopes<>(store, SagaCodec.of(store.stateCodec(), flowEvents));
    }

    public SagaStateStore<S> store() {
        return store;
    }

    public Optional<SagaEnvelope<S>> find(String sagaId) {
        requireNonNull(sagaId, "sagaId cannot be null");
        return store.find(sagaId).map(codec::decode);
    }

    /** Whatever the store reads without the state, decoded without reading a state it happens to hold. */
    public Optional<SagaEnvelope<S>> findWithoutState(String sagaId) {
        requireNonNull(sagaId, "sagaId cannot be null");
        return store.findWithoutState(sagaId).map(codec::decodeWithoutState);
    }

    /**
     * The instance and its first skipped input as one write left them, see
     * {@link SagaStateStoreSkippedInputs#findWithFirstSkippedInput(String)}, with the instance decoded whole.
     */
    public Optional<WithFirstSkippedInput<S>> findWithFirstSkippedInput(SagaStateStoreSkippedInputs records, String sagaId) {
        requireNonNull(records, "records cannot be null");
        requireNonNull(sagaId, "sagaId cannot be null");
        return records.findWithFirstSkippedInput(sagaId).map(read -> new WithFirstSkippedInput<>(codec.decode(read.saga()), read.firstSkippedInput()));
    }

    /**
     * The instance and the {@code seq} and {@code key} of its first skipped input as one write left them, see
     * {@link SagaStateStoreSkippedInputs#findWithFirstSkippedInputKey(String)}, with the instance decoded without
     * reading its state, so neither a state nor a record that no longer decodes fails it.
     */
    public Optional<WithFirstSkippedInputKey<S>> findWithFirstSkippedInputKey(SagaStateStoreSkippedInputs records, String sagaId) {
        requireNonNull(records, "records cannot be null");
        requireNonNull(sagaId, "sagaId cannot be null");
        return records.findWithFirstSkippedInputKey(sagaId).map(read -> new WithFirstSkippedInputKey<>(codec.decodeWithoutState(read.saga()), read.first()));
    }

    /**
     * An instance and the first record under its quarantine id, read together.
     *
     * @param envelope the instance
     * @param first    the first record under the quarantine id of {@code envelope}, or {@code null} when there is none
     */
    public record WithFirstSkippedInput<S extends @Nullable Object>(SagaEnvelope<S> envelope, @Nullable SkippedInput first) {
    }

    /**
     * An instance and the {@code seq} and {@code key} of the first record under its quarantine id, read together.
     *
     * @param envelope the instance, decoded without its state
     * @param first    the first record under the quarantine id of {@code envelope}, or {@code null} when there is none
     */
    public record WithFirstSkippedInputKey<S extends @Nullable Object>(SagaEnvelope<S> envelope, StoredSagaWithFirstSkippedInputKey.@Nullable RecordKey first) {
    }

    public boolean compareAndSave(String sagaId, SagaEnvelope<S> envelope, long expectedVersion) {
        sameInstance(sagaId, envelope);
        forgetOnInsert(sagaId, expectedVersion);
        return store.compareAndSave(codec.encode(envelope), expectedVersion);
    }

    /** An insert, which is an expected version of 0, writes the whole envelope, because no stored state exists to leave alone. */
    public boolean compareAndSaveWithoutState(String sagaId, SagaEnvelope<S> envelope, long expectedVersion) {
        sameInstance(sagaId, envelope);
        if (expectedVersion == 0) {
            forgetOnInsert(sagaId, expectedVersion);
            return store.compareAndSave(codec.encode(envelope), expectedVersion);
        }
        return store.compareAndSaveWithoutState(codec.encodeWithoutState(envelope), expectedVersion);
    }

    /**
     * Skip an input, see {@link SagaStateStoreSkippedInputs#compareAndSaveSkipped}. {@code envelope}'s state is not
     * written, except by an insert, which is an expected version of 0 and writes the whole envelope.
     */
    public boolean compareAndSaveSkipped(SagaStateStoreSkippedInputs records, SagaEnvelope<S> envelope, long expectedVersion, String quarantineId, SkippedInput input) {
        requireNonNull(records, "records cannot be null");
        if (expectedVersion == 0) {
            forgetOnInsert(envelope.sagaId(), expectedVersion);
            return records.compareAndSaveSkipped(codec.encode(envelope), expectedVersion, quarantineId, input);
        }
        return records.compareAndSaveSkipped(codec.encodeWithoutState(envelope), expectedVersion, quarantineId, input);
    }

    /** Apply or drop a replayed input, see {@link SagaStateStoreSkippedInputs#compareAndSaveReplayed}. The whole envelope is written. */
    public boolean compareAndSaveReplayed(SagaStateStoreSkippedInputs records, SagaEnvelope<S> envelope, long expectedVersion, String quarantineId, SkippedInput replayed) {
        requireNonNull(records, "records cannot be null");
        return records.compareAndSaveReplayed(codec.encode(envelope), expectedVersion, quarantineId, replayed);
    }

    /** Whether {@code records} can write the whole {@code envelope}, see {@link SagaStateStoreSkippedInputs#fits}. */
    public boolean fits(SagaStateStoreSkippedInputs records, SagaEnvelope<S> envelope) {
        requireNonNull(records, "records cannot be null");
        return records.fits(codec.encode(envelope));
    }

    /**
     * Whether {@code records} can write {@code envelope} with {@code compareAndSaveWithoutState}, which leaves the stored
     * state where it is, see {@link SagaStateStoreSkippedInputs#fitsWithoutState}. The store measures the instance as it
     * holds it, so nothing here decodes the stored state.
     */
    public boolean fitsWithoutState(SagaStateStoreSkippedInputs records, SagaEnvelope<S> envelope, long expectedVersion) {
        requireNonNull(records, "records cannot be null");
        requireNonNull(envelope, "envelope cannot be null");
        return records.fitsWithoutState(codec.encodeWithoutState(envelope), expectedVersion);
    }

    /**
     * {@code stored} with the fields a write under the rule of {@link SagaStateStore#compareAndSaveWithoutState} sets
     * replaced by those of {@code written}, which is the instance that write leaves. A field {@code written} has no
     * value for is removed, and the state and the step derived from it stay as {@code stored} has them. For a store
     * that keeps an instance as the {@link StoredSaga} it is given, to answer
     * {@link SagaStateStoreSkippedInputs#fitsWithoutState} with {@link SagaStateStoreSkippedInputs#fits}.
     */
    public static StoredSaga withFieldsExceptStateOf(StoredSaga stored, StoredSaga written) {
        requireNonNull(stored, "stored cannot be null");
        requireNonNull(written, "written cannot be null");
        Map<String, @Nullable Object> fields = new LinkedHashMap<>(stored.encoded().fields());
        Map<String, Object> set = written.encoded().fields();
        for (String field : SagaCodec.fieldsExceptState()) {
            if (set.containsKey(field)) {
                fields.put(field, set.get(field));
            } else {
                fields.remove(field);
            }
        }
        return new StoredSaga(written.sagaId(), written.version(), new EncodedSaga(fields), written.index());
    }

    /**
     * End a quarantine, see {@link SagaStateStoreSkippedInputs#compareAndSaveQuarantineEnded}. {@code envelope}'s state
     * is not written, so an envelope decoded without its state ends the quarantine too.
     */
    public boolean compareAndSaveQuarantineEnded(SagaStateStoreSkippedInputs records, SagaEnvelope<S> envelope, long expectedVersion, String quarantineId) {
        requireNonNull(records, "records cannot be null");
        return records.compareAndSaveQuarantineEnded(codec.encodeWithoutState(envelope), expectedVersion, quarantineId);
    }

    public List<SagaEnvelope<S>> findWithDueTimers(Instant now, int limit) {
        requireNonNull(now, "now cannot be null");
        return store.findWithDueTimers(now, limit).stream().map(codec::decodeWithoutState).toList();
    }

    /** @throws UnsupportedOperationException if the store does not implement {@link SagaStateStoreQueries} */
    public List<SagaEnvelope<S>> findByStatus(SagaStatus status, Instant updatedBefore, int limit) {
        requireNonNull(status, "status cannot be null");
        requireNonNull(updatedBefore, "updatedBefore cannot be null");
        if (!(store instanceof SagaStateStoreQueries<?> queries)) {
            throw new UnsupportedOperationException("The provided SagaStateStore implementation does not support enumerating instances. %s must implement SagaStateStoreQueries in order to use findByStatus.".formatted(store.getClass().getName()));
        }
        return queries.findByStatus(status, updatedBefore, limit).stream().map(codec::decodeWithoutState).toList();
    }

    // The codec's warning latch keeps the entry of an instance a delete or a TTL index removed, so a new instance under its
    // id would otherwise inherit that warning and never warn about its own retained events
    private void forgetOnInsert(String sagaId, long expectedVersion) {
        if (expectedVersion == 0) {
            codec.forget(sagaId);
        }
    }

    private static <S extends @Nullable Object> SagaEnvelope<S> sameInstance(String sagaId, SagaEnvelope<S> envelope) {
        requireNonNull(sagaId, "sagaId cannot be null");
        requireNonNull(envelope, "envelope cannot be null");
        if (!envelope.sagaId().equals(sagaId)) {
            throw new IllegalArgumentException("sagaId '" + sagaId + "' is not the id of the envelope, '" + envelope.sagaId() + "'");
        }
        return envelope;
    }
}
