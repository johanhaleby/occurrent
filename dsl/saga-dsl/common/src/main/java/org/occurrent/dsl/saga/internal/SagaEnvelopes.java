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
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.codec.SagaCodec;

import java.time.Instant;
import java.util.List;
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

    public boolean compareAndSave(String sagaId, SagaEnvelope<S> envelope, long expectedVersion) {
        return store.compareAndSave(codec.encode(sameInstance(sagaId, envelope)), expectedVersion);
    }

    /** An insert, which is an expected version of 0, writes the whole envelope, because no stored state exists to leave alone. */
    public boolean compareAndSaveWithoutState(String sagaId, SagaEnvelope<S> envelope, long expectedVersion) {
        sameInstance(sagaId, envelope);
        if (expectedVersion == 0) {
            return store.compareAndSave(codec.encode(envelope), expectedVersion);
        }
        return store.compareAndSaveWithoutState(codec.encodeWithoutState(envelope), expectedVersion);
    }

    public List<SagaEnvelope<S>> findWithDueTimers(Instant now, int limit) {
        return store.findWithDueTimers(now, limit).stream().map(codec::decodeWithoutState).toList();
    }

    /** @throws UnsupportedOperationException if the store does not implement {@link SagaStateStoreQueries} */
    public List<SagaEnvelope<S>> findByStatus(SagaStatus status, Instant updatedBefore, int limit) {
        if (!(store instanceof SagaStateStoreQueries<?> queries)) {
            throw new UnsupportedOperationException("The provided SagaStateStore implementation does not support enumerating instances. %s must implement SagaStateStoreQueries in order to use findByStatus.".formatted(store.getClass().getName()));
        }
        return queries.findByStatus(status, updatedBefore, limit).stream().map(codec::decodeWithoutState).toList();
    }

    public void delete(String sagaId) {
        store.delete(sagaId);
        codec.forget(sagaId);
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
