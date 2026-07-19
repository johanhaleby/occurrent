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
 *
 * @param <S> the user state type
 */
public interface SagaStateStore<S extends @Nullable Object> {

    /** The stored envelope for {@code sagaId}, or empty if none exists yet. */
    Optional<SagaEnvelope<S>> find(String sagaId);

    /**
     * Save {@code envelope} only if the currently stored version equals {@code expectedVersion} (use {@code 0} to insert a
     * new instance). Returns {@code false} on a version conflict, so the caller can reload and retry; returns {@code true}
     * on success.
     */
    boolean compareAndSave(String sagaId, SagaEnvelope<S> envelope, long expectedVersion);

    /**
     * Active instances that have at least one timer due at or before {@code now}, at most {@code limit} of them. The
     * executor's timer poller uses this to fire timeouts; a returned instance may have several due timers.
     */
    List<SagaEnvelope<S>> findWithDueTimers(Instant now, int limit);

    /** Remove the instance, for retention tooling. Most deployments keep completed instances (with a TTL) instead. */
    void delete(String sagaId);

    /** An in-memory store, for tests and single-node use. */
    static <S extends @Nullable Object> SagaStateStore<S> inMemory() {
        return new InMemorySagaStateStore<>();
    }
}
