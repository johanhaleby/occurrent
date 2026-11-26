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
import org.occurrent.dsl.saga.codec.SagaCodec;
import org.occurrent.dsl.saga.codec.SagaStateCodec;
import org.occurrent.dsl.saga.codec.StoredSaga;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Persistence for saga instances. Unlike a read-model {@code ViewStateRepository}, a saga store must support a
 * compare-and-set save: two threads (a subscription delivering an event and the timer poller firing a timeout) can touch
 * one instance concurrently, so the executor detects a lost update and retries rather than silently overwriting. The
 * store is also queried for instances with a due timer, since timers live in the instance rather than an external
 * scheduler.
 * <p>
 * A store keeps {@link StoredSaga} values and never sees a {@link SagaEnvelope}. Occurrent encodes and decodes the
 * instance with {@link SagaCodec}, so it owns every bookkeeping field, and the store persists the id, the version, the
 * encoded form and the index of each {@link StoredSaga}. A store decides only how the user state is represented,
 * which it supplies as {@link #stateCodec()}.
 * <p>
 * Two methods read and write an instance without its application state, which is how the executor quarantines an
 * instance whose state no longer decodes. A store implements both, since each is the whole read or write with the state
 * left out.
 * <p>
 * Quarantine needs a second capability. A store that also implements {@link SagaStateStoreSkippedInputs} records the
 * events a quarantined instance skips and lets the instance be released. The runner refuses to start a saga with
 * quarantine on when its store does not implement it, and {@code SagaRunnerConfig.disableQuarantine()} runs the saga
 * without quarantine on such a store.
 * <p>
 * Observing instances is an optional capability layered on top. A store that also implements
 * {@link SagaStateStoreQueries} can be enumerated, which is what a progress view or a stuck-instance sweep needs. A
 * store that does not is still perfectly usable for running sagas.
 *
 * @param <S> the user state type
 */
public interface SagaStateStore<S extends @Nullable Object> {

    /** How this store represents the user state of its saga. */
    SagaStateCodec<S> stateCodec();

    /**
     * The stored instance for {@code sagaId}, or empty if none exists yet.
     * <p>
     * An empty result has to hold every write acknowledged before the call started. The executor skips an input
     * addressed to no instance without writing anything, so no compare-and-set catches an instance this read misses, and
     * the input is acknowledged unapplied. A store that can't give that answer, such as a read at a transaction snapshot
     * that began before the write, throws instead of returning empty. An instance this returns may be an older version than the stored one, since
     * the executor writes it back with a compare-and-set that such a version loses. {@link #findWithoutState(String)}
     * answers under the same rule.
     */
    Optional<StoredSaga> find(String sagaId);

    /**
     * The stored instance for {@code sagaId} without its application state, or empty if none exists yet. This is what
     * the executor reads when it has to decide whether a failing input has used up its quarantine budget, a decision
     * that needs the status, the failure record and the dedup watermarks and never needs the state.
     * <p>
     * A store must answer this without reading the state, because the instance a quarantine exists to suspend is very
     * often the instance whose state no longer decodes. The result needs {@link SagaCodec#fieldsWithoutState()} and
     * nothing else, and it may hold the state, which the caller ignores.
     */
    Optional<StoredSaga> findWithoutState(String sagaId);

    /**
     * Save {@code saga} only if the currently stored version equals {@code expectedVersion} (use {@code 0} to insert a
     * new instance). Returns {@code false} on a version conflict, so the caller can reload and retry. Returns {@code true}
     * on success.
     * <p>
     * Store the version in {@code saga}. It is usually {@code expectedVersion + 1}, but the write that marks a
     * failed timer (see {@link #findWithDueTimers(Instant, int)}) holds {@code expectedVersion} itself, so it doesn't
     * make an event that read the instance before it lose its compare-and-set. A store that refuses a save at the same
     * version refuses every mark. For each failed fire the executor then reads the instance twice, tries the mark once
     * and logs a {@code WARN}, and the failing timers keep their place at the front of the due-timer query, as in 0.34.0.
     * A store that sets the version itself, as one built on a JPA {@code @Version} column does, raises it on the mark
     * too. An event that read the instance before the mark then loses its compare-and-set, and one that takes longer than
     * the poll interval can lose it on every attempt.
     */
    boolean compareAndSave(StoredSaga saga, long expectedVersion);

    /**
     * Save {@code saga} under the same compare-and-set rule as {@link #compareAndSave(StoredSaga, long)}, leaving the
     * stored application state where it is. {@code saga} holds no state. The store sets the fields in
     * {@link SagaCodec#fieldsExceptState()} from it and removes the ones it has no value for, and touches no other
     * field. The executor records a failure and a quarantine through this, so neither half of that decision decodes the
     * state, and the state the instance stopped on is still there afterwards for whoever repairs the converter.
     * <p>
     * An insert, meaning an {@code expectedVersion} of {@code 0}, goes through {@link #compareAndSave(StoredSaga, long)}
     * with the whole instance, because there is no stored state to leave alone. A store throws
     * {@link IllegalArgumentException} for an {@code expectedVersion} of {@code 0} here, since {@code saga} holds no state
     * to insert.
     */
    boolean compareAndSaveWithoutState(StoredSaga saga, long expectedVersion);

    /**
     * {@link SagaStatus#ACTIVE} instances that have at least one timer due at or before {@code now}, at most
     * {@code limit} of them. The executor's timer poller uses this to fire
     * timeouts. A returned instance may have several due timers. The result needs {@link SagaCodec#instanceFields()}
     * and nothing else.
     * <p>
     * Active, not merely unfinished. A {@link SagaStatus#QUARANTINED} instance must not be returned. Its timers stay
     * armed rather than dropped, and firing one would advance its state across the input it stopped on. A store matches
     * the stored statuses of {@link SagaCodec#storedStatuses(SagaStatus)} for {@code ACTIVE}.
     * <p>
     * The instances come earliest first, ordered by {@code SagaIndex.nextTimerDueAtEpochMilli()}, and {@code limit}
     * keeps the first ones in that order. Instances due at the same instant come back in no particular order. When a
     * timer's fire throws, the executor makes that timer due again one millisecond after the poll that tried it, which
     * moves the instance behind every instance that was due at that poll. So instances whose timers keep failing take
     * turns with the healthy ones rather than filling every batch.
     * <p>
     * A store that ignores the order gives up that guarantee. Once {@code limit} instances keep failing, such a store
     * can return the same ones on every poll, and the saga's other instances never get their timers fired.
     */
    List<StoredSaga> findWithDueTimers(Instant now, int limit);

    /**
     * Remove the instance, for retention tooling. Most deployments keep completed instances (with a TTL) instead, and that
     * is the recommended default for a reason: deleting an instance discards the dedup watermarks and the completed status
     * that make the instance absorbing. If the event source can still redeliver an event this instance already consumed (a
     * subscription replay, a redelivery after a crash, an at-least-once feed), a delete that races that redelivery lets the
     * event recreate the instance from its start event and run the process a second time. Delete an instance only once its
     * source can no longer redeliver any of its events, until then let a TTL expire it, so a late redelivery still finds
     * the terminal instance and is skipped rather than resurrecting it.
     * <p>
     * A store that implements {@link SagaStateStoreSkippedInputs} also deletes the records under the quarantine id the
     * deleted instance had, in the same atomic write as the instance. A record of a quarantined instance can be the only
     * copy of an input the subscription has already acknowledged, so deleting such an instance discards the inputs it
     * skipped, and no release can replay them. A TTL index or any job that removes instances from the database without
     * this method deletes none of their records, so no release can replay those inputs, and the records stay in the
     * database with no instance. For the MongoDB store they are in the
     * collections named after the instances with {@code _skipped_inputs} and {@code _skipped_input_chunks} appended. So
     * leave quarantined instances out of such a job, or clean those two collections in it.
     */
    void delete(String sagaId);

    /** An in-memory store, for tests and single-node use. */
    static <S extends @Nullable Object> SagaStateStore<S> inMemory() {
        return new InMemorySagaStateStore<>();
    }
}
