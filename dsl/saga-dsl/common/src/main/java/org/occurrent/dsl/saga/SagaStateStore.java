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
 * This is the minimal contract the executor needs. Most of its methods are {@code default}. Two are about reading and
 * writing an instance without its application state, which is how the executor quarantines an instance whose state no
 * longer decodes. A store inherits them and works, and only a store that overrides them lets such an instance be
 * considered for quarantine at all.
 * <p>
 * The others are about releasing a quarantined instance, and their defaults throw {@link UnsupportedOperationException},
 * so a store written before 0.35.0 keeps compiling. Such a store still quarantines an instance, and the instance still
 * skips its inputs, but it records none of them and cannot be released. A store that supports release implements
 * {@link #recordSkippedInput}, {@link #findSkippedInputs}, {@link #deleteSkippedInput} and {@link #deleteSkippedInputs},
 * removes an instance's records in {@link #delete(String)}, and persists every member of {@link SagaEnvelope} and
 * {@link SagaFailure}, {@link SagaFailure#releasedAt()}, {@link SagaFailure#quarantineId()} and
 * {@link SagaEnvelope#quarantineSkipCount()} included. A release reads
 * the instance back after it starts and throws when one of those did not persist. {@link #findReleasingSagaIds(String, int)}
 * is what lets the timer poller go on with a release that failed on an input or was interrupted. Without it a release
 * goes on only when it is asked for again.
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
     * without ever recording that it was failing, so it never reached its budget.
     * <p>
     * A caller therefore must not read {@link SagaEnvelope#state()} off the result. A store that answers without
     * decoding leaves it {@code null} even for a healthy instance, exactly as a
     * {@link SagaStateStoreQueries#findByStatus} result does, while the default below hands back whatever {@code find}
     * gave, so which of the two you get is the store's business and not something to branch on. Every other member is
     * populated either way. Use {@link #find(String)} when the state itself is wanted.
     * <p>
     * The default reads the whole instance through {@link #find(String)}, so a store written against 0.33.0 keeps
     * compiling and keeps behaving as it did, which means an instance it cannot decode still never reaches its
     * budget. The executor cannot read an instance without its state on a store that only reads it whole.
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
     * <p>
     * Store the version in {@code envelope}. It is usually {@code expectedVersion + 1}, but the write that marks a
     * failed timer (see {@link #findWithDueTimers(Instant, int)}) holds {@code expectedVersion} itself, so it doesn't
     * make an event that read the instance before it lose its compare-and-set. A store that refuses a save at the same
     * version refuses every mark. For each failed fire the executor then reads the instance twice, tries the mark once
     * and logs a {@code WARN}, and the failing timers keep their place at the front of the due-timer query, as in 0.34.0.
     * A store that sets the version itself, as one built on a JPA {@code @Version} column does, raises it on the mark
     * too. An event that read the instance before the mark then loses its compare-and-set, and one that takes longer than
     * the poll interval can lose it on every attempt.
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
     * The instances come earliest first, ordered by {@link SagaEnvelope#earliestDueAtEpochMilli()}, and {@code limit}
     * keeps the first ones in that order. Instances due at the same instant come back in no particular order. When a
     * timer's fire throws, the executor makes that timer due again one millisecond after the poll that tried it, which
     * moves the instance behind every instance that was due at that poll. So instances whose timers keep failing take
     * turns with the healthy ones rather than filling every batch.
     * <p>
     * A store that ignores the order gives up that guarantee. Once {@code limit} instances keep failing, such a store
     * can return the same ones on every poll, and the saga's other instances never get their timers fired.
     */
    List<SagaEnvelope<S>> findWithDueTimers(Instant now, int limit);

    /**
     * The ids of instances that are releasing, in ascending id order, strictly after {@code after} and at most
     * {@code limit} of them. A releasing instance is {@link SagaStatus#QUARANTINED} and its failure record has
     * {@link SagaFailure#releasedAt()} set. The timer poller asks for them on every poll and goes on with their
     * release, which is what retries an input that failed during a release and what resumes a release a crash
     * interrupted.
     * <p>
     * The poller passes the last id of the previous page as {@code after}, and {@code null} to start over. That is
     * what gives every releasing instance a turn however many sit at the front. A store that ignores {@code after}
     * can return the same first ids on every poll, and the instances behind them never get continued.
     * <p>
     * The default throws {@link UnsupportedOperationException}, so a store written before 0.35.0 keeps compiling. The
     * poller then logs once at {@code WARN} and stops asking, and a release on that store goes on only when
     * {@code SagaSubscription.release(sagaId)} is called again.
     *
     * @param after only ids greater than {@code after} are returned, or all of them when {@code null}
     * @param limit how many ids to return at most, at least 1
     */
    default List<String> findReleasingSagaIds(@Nullable String after, int limit) {
        throw new UnsupportedOperationException(getClass().getName() + " does not implement findReleasingSagaIds");
    }

    /**
     * Record that the quarantined instance {@code sagaId} skipped {@code input}, under the quarantine
     * {@code quarantineId}. A record of the same key under the same quarantine that is there already is kept as it is,
     * with its {@code seq}. Atomic per key, so two callers recording one key store one record.
     * <p>
     * The default throws {@link UnsupportedOperationException}. The executor then skips the input without recording it.
     */
    default void recordSkippedInput(String sagaId, String quarantineId, SkippedInput input) {
        throw new UnsupportedOperationException(getClass().getName() + " does not implement recordSkippedInput");
    }

    /**
     * The first {@code limit} records under the quarantine {@code quarantineId} of {@code sagaId}, in
     * {@link SkippedInput#ORDER}, by {@code seq} and then by {@code key}. Records under any other quarantine id are left
     * out.
     * <p>
     * No two records a skip wrote share a {@code seq}. A skip raises the instance's
     * {@link SagaEnvelope#quarantineSkipCount()} before it writes the record, with the count it read as the record's
     * {@code seq}, so a crash between the two leaves a gap in the numbers and never a second record with the same one.
     * Two records can share {@code -1}, the head record a release writes, when an earlier release kept its own head
     * record under another key for the same input. The order by {@code key} keeps their order the same on every read.
     * <p>
     * The default throws {@link UnsupportedOperationException}.
     *
     * @param limit how many records to return at most, at least 1
     */
    default List<SkippedInput> findSkippedInputs(String sagaId, String quarantineId, int limit) {
        throw new UnsupportedOperationException(getClass().getName() + " does not implement findSkippedInputs");
    }

    /**
     * Delete the record of {@code key} under the quarantine {@code quarantineId} of {@code sagaId}, whatever its
     * {@code seq}. Nothing happens when there is none. Release uses it only to move the record of the input it starts
     * from to the head, and deletes every other record with {@link #deleteSkippedInput(String, String, SkippedInput)}.
     * <p>
     * The default throws {@link UnsupportedOperationException}.
     */
    default void deleteSkippedInputByKey(String sagaId, String quarantineId, String key) {
        throw new UnsupportedOperationException(getClass().getName() + " does not implement deleteSkippedInputByKey");
    }

    /**
     * Delete the record of {@code input}'s key under the quarantine {@code quarantineId} of {@code sagaId}, only when its
     * {@code seq} is {@code input}'s too. Nothing happens when there is none, or when the record of that key has another
     * {@code seq}, which is what a release that recorded the key again in between left there.
     * <p>
     * The default throws {@link UnsupportedOperationException}.
     */
    default void deleteSkippedInput(String sagaId, String quarantineId, SkippedInput input) {
        throw new UnsupportedOperationException(getClass().getName() + " does not implement deleteSkippedInput");
    }

    /**
     * Delete every record of {@code sagaId}, under every quarantine id. {@link #delete(String)} does this too.
     * <p>
     * The default throws {@link UnsupportedOperationException}.
     */
    default void deleteSkippedInputs(String sagaId) {
        throw new UnsupportedOperationException(getClass().getName() + " does not implement deleteSkippedInputs");
    }

    /**
     * Remove the instance, for retention tooling. Most deployments keep completed instances (with a TTL) instead, and that
     * is the recommended default for a reason: deleting an instance discards the dedup watermarks and the completed status
     * that make the instance absorbing. If the event source can still redeliver an event this instance already consumed (a
     * subscription replay, a redelivery after a crash, an at-least-once feed), a delete that races that redelivery lets the
     * event recreate the instance from its start event and run the process a second time. Delete an instance only once its
     * source can no longer redeliver any of its events, until then let a TTL expire it, so a late redelivery still finds
     * the terminal instance and is skipped rather than resurrecting it.
     * <p>
     * A store that records skipped inputs deletes the instance's records too, under every quarantine id, so a later
     * instance with the same id inherits none of them.
     */
    void delete(String sagaId);

    /** An in-memory store, for tests and single-node use. */
    static <S extends @Nullable Object> SagaStateStore<S> inMemory() {
        return new InMemorySagaStateStore<>();
    }
}
