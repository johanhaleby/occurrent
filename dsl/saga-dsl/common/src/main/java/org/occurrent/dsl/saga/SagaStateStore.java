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
     * new instance). Returns {@code false} on a version conflict, so the caller can reload and retry. Returns {@code true}
     * on success.
     */
    boolean compareAndSave(String sagaId, SagaEnvelope<S> envelope, long expectedVersion);

    /**
     * Active instances that have at least one timer due at or before {@code now}, at most {@code limit} of them. The
     * executor's timer poller uses this to fire timeouts. A returned instance may have several due timers.
     */
    List<SagaEnvelope<S>> findWithDueTimers(Instant now, int limit);

    /**
     * Instances with {@code status} whose {@link SagaEnvelope#updatedAt()} is strictly before {@code updatedBefore},
     * least recently updated first, at most {@code limit} of them. This is what {@link SagaInstances} enumerates over,
     * so every store must agree on the contract:
     * <ul>
     *   <li>{@code updatedBefore} is <em>exclusive</em>. Pass the current time to mean "every instance in this status",
     *       or {@code now} minus a threshold to mean "every instance that has gone quiet for longer than that". The
     *       <em>resolution</em> of that comparison is store-dependent and at best milliseconds: a store that persists
     *       {@code updatedAt} as epoch millis compares truncated values, while the executor stamps a possibly
     *       sub-millisecond {@code Instant}. An instance updated within the same millisecond as {@code updatedBefore}
     *       may therefore be excluded. A store may not be more <em>inclusive</em> than the exclusive boundary, so no
     *       instance at or after it is ever returned.</li>
     *   <li>The order is ascending by {@code updatedAt}, so the stalest instance comes first. That is the useful end
     *       for finding a stuck instance: the worst offenders arrive before {@code limit} truncates.</li>
     *   <li>{@code limit} is a <em>bound, not a page</em>. There is no cursor: {@code updatedAt} persists at
     *       millisecond precision, so instances saved in one executor tick tie, and resuming from the last row's
     *       timestamp would silently drop the rest of a tie group. A caller that needs to walk everything should
     *       raise {@code limit}, and one that needs true paging needs an ordering this method does not offer.</li>
     *   <li>An instance whose {@code updatedAt} is {@code null} is never returned. The executor always stamps it, so
     *       this only excludes a hand-built envelope, and it keeps a store whose query engine skips a missing field
     *       from disagreeing with one that could have treated {@code null} as matching.</li>
     * </ul>
     * Unlike {@link #findWithDueTimers(Instant, int)} this reads whole instances, state included, because
     * {@link SagaInstance#currentStep()} cannot be answered without it. Enumerating flow-saga instances therefore
     * decodes their received logs, which is why {@code limit} is required rather than optional.
     * <p>
     * Because this is the observation path, an instance whose state can no longer be decoded must be <em>reported
     * without its state</em> rather than making the whole enumeration fail. One instance holding, say, a received event
     * whose class was renamed away would otherwise take down the progress view for every caller, at the exact moment
     * someone is looking into what went wrong. Such a row still answers every {@link SagaInstance} member except
     * {@link SagaInstance#currentStep()}. This is the opposite of {@link #find(String)}, which must keep failing loudly,
     * because the executor loads an instance in order to fold and save it.
     *
     * @throws IllegalArgumentException if {@code limit} is not positive
     */
    List<SagaEnvelope<S>> findByStatus(SagaStatus status, Instant updatedBefore, int limit);

    /**
     * Remove the instance, for retention tooling. Most deployments keep completed instances (with a TTL) instead, and that
     * is the recommended default for a reason: deleting an instance discards the dedup watermarks and the completed status
     * that make the instance absorbing. If the event source can still redeliver an event this instance already consumed (a
     * subscription replay, a redelivery after a crash, an at-least-once feed), a delete that races that redelivery lets the
     * event recreate the instance from its start event and run the process a second time. Delete an instance only once its
     * source can no longer redeliver any of its events; until then let a TTL expire it, so a late redelivery still finds
     * the terminal instance and is skipped rather than resurrecting it.
     */
    void delete(String sagaId);

    /** An in-memory store, for tests and single-node use. */
    static <S extends @Nullable Object> SagaStateStore<S> inMemory() {
        return new InMemorySagaStateStore<>();
    }
}
