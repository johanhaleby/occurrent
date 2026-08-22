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
 * This is the minimal contract the executor needs. Observing instances is an optional capability layered on top: a store
 * that also implements {@link SagaStateStoreQueries} can be enumerated, which is what a progress view or a
 * stuck-instance sweep needs. A store that does not is still perfectly usable for running sagas.
 *
 * @param <S> the user state type
 */
public interface SagaStateStore<S extends @Nullable Object> {

    /** The stored envelope for {@code sagaId}, or empty if none exists yet. */
    Optional<SagaEnvelope<S>> find(String sagaId);

    /**
     * Save {@code envelope} only if the currently stored version equals {@code expectedVersion} (use {@code 0} to insert a
     * new instance). Returns {@code false} on a version conflict, so the caller can reload and retry. Returns {@code true}
     * on success.
     */
    boolean compareAndSave(String sagaId, SagaEnvelope<S> envelope, long expectedVersion);

    /**
     * {@link SagaStatus#ACTIVE} instances that have at least one timer due at or before {@code now}, at most
     * {@code limit} of them. The executor's timer poller uses this to fire timeouts. A returned instance may have
     * several due timers.
     * <p>
     * Active, not merely unfinished. A {@link SagaStatus#QUARANTINED} instance must not be returned: its timers stay
     * armed so a release restores them, and firing one meanwhile would advance its state across the input it stopped on.
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
