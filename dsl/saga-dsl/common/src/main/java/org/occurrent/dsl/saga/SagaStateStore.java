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
import java.util.List;
import java.util.Optional;

/**
 * Persistence for saga instances. Unlike a read-model {@code ViewStateRepository}, a saga store must support a
 * compare-and-set save: two threads (a subscription delivering an event and the timer poller firing a timeout) can touch
 * one instance concurrently, so the executor detects a lost update and retries rather than silently overwriting. The
 * store is also queried for instances with a due timer, since timers live in the envelope rather than an external
 * scheduler.
 * <p>
 * This is the minimal contract the executor needs. Two of its six methods are {@code default}, and both are about
 * reading and writing an instance without its application state, which is how the executor quarantines an instance
 * whose state no longer decodes. A store inherits them and works, and a store that overrides them also stops such an
 * instance from blocking the saga's other instances.
 * <p>
 * Observing instances is an optional capability layered on top. A store that also implements
 * {@link SagaStateStoreQueries} can be enumerated, which is what a progress view or a stuck-instance sweep needs. A
 * store that does not is still perfectly usable for running sagas.
 *
 * @param <S> the user state type
 */
public interface SagaStateStore<S extends @Nullable Object> {

    /** The stored envelope for {@code sagaId}, or empty if none exists yet. */
    Optional<SagaEnvelope<S>> find(String sagaId);

    /**
     * The stored envelope for {@code sagaId} without its application state, or empty if none exists yet. This is what
     * the executor reads when it has to decide whether a failing input has used up its quarantine budget, a decision
     * that needs the status, the failure record and the dedup watermarks and never needs the state.
     * <p>
     * A store that can answer this without decoding the state must do so, because the instance a quarantine exists to
     * suspend is very often the instance whose state no longer decodes. A renamed event class, a converter change, or
     * state written by a version of the application nobody runs any more all leave an instance that throws on
     * {@link #find(String)} while the rest of its document reads perfectly well. Such an instance used to keep failing
     * without ever recording that it was failing, so it never reached its budget and went on blocking every other
     * instance of the saga.
     * <p>
     * A caller therefore must not read {@link SagaEnvelope#state()} off the result. A store that answers without
     * decoding leaves it {@code null} even for a healthy instance, exactly as a
     * {@link SagaStateStoreQueries#findByStatus} result does, while the default below hands back whatever {@code find}
     * gave, so which of the two you get is the store's business and not something to branch on. Every other member is
     * populated either way. Use {@link #find(String)} when the state itself is wanted.
     * <p>
     * The default reads the whole instance through {@link #find(String)}, so a store written against 0.33.0 keeps
     * compiling and keeps behaving as it did, which means it also keeps the blocking behaviour for an instance it
     * cannot decode. The executor cannot read an instance without its state on a store that only reads it whole.
     * <p>
     * A store that overrides this must override {@link #compareAndSaveWithoutState(String, SagaEnvelope, long)} too.
     * The executor saves what it read, so a store that hands back a {@code null} state here and then writes the
     * envelope whole would erase the state it was careful not to decode.
     */
    default Optional<SagaEnvelope<S>> findWithoutState(String sagaId) {
        return find(sagaId);
    }

    /**
     * Save {@code envelope} only if the currently stored version equals {@code expectedVersion} (use {@code 0} to insert a
     * new instance). Returns {@code false} on a version conflict, so the caller can reload and retry. Returns {@code true}
     * on success.
     */
    boolean compareAndSave(String sagaId, SagaEnvelope<S> envelope, long expectedVersion);

    /**
     * Save {@code envelope} under the same compare-and-set rule as
     * {@link #compareAndSave(String, SagaEnvelope, long)}, leaving the stored application state where it is rather
     * than taking it from {@code envelope}. The executor records a failure and a quarantine through this, so neither
     * half of that decision decodes the state, and the state the instance stopped on is still there afterwards for
     * whoever repairs the converter.
     * <p>
     * An insert, meaning an {@code expectedVersion} of {@code 0}, writes the whole envelope including its state,
     * because there is no stored state to leave alone. That case is an instance whose very first input failed.
     * <p>
     * The default writes the envelope whole through {@code compareAndSave}, which is right for any store whose
     * {@link #findWithoutState(String)} hands the state back anyway, the default included. See that method for why the
     * two are overridden together.
     */
    default boolean compareAndSaveWithoutState(String sagaId, SagaEnvelope<S> envelope, long expectedVersion) {
        return compareAndSave(sagaId, envelope, expectedVersion);
    }

    /**
     * {@link SagaStatus#ACTIVE} instances that have at least one timer due at or before {@code now}, at most
     * {@code limit} of them. The executor's timer poller uses this to fire
     * timeouts. A returned instance may have several due timers.
     * <p>
     * Active, not merely unfinished. A {@link SagaStatus#QUARANTINED} instance must not be returned. Its timers stay
     * armed rather than dropped, and firing one would advance its state across the input it stopped on.
     * <p>
     * There is no ordering requirement and no fairness requirement, and the second of those is a known defect rather
     * than a design choice. This method places no limit on how often one instance is returned and asks no store to
     * give a different instance a turn, so an instance whose timer reaction always throws can be returned on every
     * poll, and once {@code limit} of them are in that state the saga can stop firing timers altogether, since a
     * batch full of them leaves no place for anything else.
     * <a href="https://github.com/johanhaleby/occurrent/issues/1003">#1003</a> is where that missing guarantee is
     * being added.
     */
    List<SagaEnvelope<S>> findWithDueTimers(Instant now, int limit);

    /**
     * Remove the instance, for retention tooling. Most deployments keep completed instances (with a TTL) instead, and that
     * is the recommended default for a reason: deleting an instance discards the dedup watermarks and the completed status
     * that make the instance absorbing. If the event source can still redeliver an event this instance already consumed (a
     * subscription replay, a redelivery after a crash, an at-least-once feed), a delete that races that redelivery lets the
     * event recreate the instance from its start event and run the process a second time. Delete an instance only once its
     * source can no longer redeliver any of its events, until then let a TTL expire it, so a late redelivery still finds
     * the terminal instance and is skipped rather than resurrecting it.
     */
    void delete(String sagaId);

    /** An in-memory store, for tests and single-node use. */
    static <S extends @Nullable Object> SagaStateStore<S> inMemory() {
        return new InMemorySagaStateStore<>();
    }
}
