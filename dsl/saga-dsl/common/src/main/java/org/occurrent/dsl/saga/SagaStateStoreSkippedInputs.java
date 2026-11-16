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

import java.util.List;

/**
 * The capability a {@link SagaStateStore} needs for quarantine. A quarantined instance records here every input it
 * skips, and a release replays those records and deletes them. The runner checks for this with {@code instanceof} on
 * the saga's store, the way {@link SagaInstances} checks for {@link SagaStateStoreQueries}. On a subscription model that
 * holds every event, it refuses to start a saga with quarantine on when its store does not implement it. Call
 * {@code SagaRunnerConfig.disableQuarantine()} to run a saga on such a store.
 * <p>
 * A store that implements this removes, in {@link SagaStateStore#delete(String)}, the records under the quarantine id of
 * the instance it deletes. It persists the quarantine id and the skip count of an instance like every other field,
 * since it keeps the {@link org.occurrent.dsl.saga.codec.StoredSaga} it is given as it is.
 * <p>
 * The runner sees only the store it is given, so a decorator around a store that implements this has to implement it
 * too and pass each call on. On a subscription model that holds every event, a saga with quarantine on and a decorator
 * that does not refuses to start, as above. Otherwise it starts, and {@code SagaSubscription.release(sagaId)} then
 * throws an {@link IllegalStateException} that names this interface, even for an instance the store does not hold,
 * instead of returning {@code NOT_FOUND}. The delivery of an input that an instance quarantined before the decorator
 * was put in place has to record as skipped throws the same exception.
 * <p>
 * The interface takes no state type parameter, since no member reads or writes the state.
 */
public interface SagaStateStoreSkippedInputs {

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
     *
     * @param after only ids greater than {@code after} are returned, or all of them when {@code null}
     * @param limit how many ids to return at most, at least 1
     */
    List<String> findReleasingSagaIds(@Nullable String after, int limit);

    /**
     * Record that the quarantined instance {@code sagaId} skipped {@code input}, under the quarantine
     * {@code quarantineId}. A record of the same key under the same quarantine that is there already is kept as it is,
     * with its {@code seq}. Atomic per key, so two callers recording one key store one record.
     */
    void recordSkippedInput(String sagaId, String quarantineId, SkippedInput input);

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
     *
     * @param limit how many records to return at most, at least 1
     */
    List<SkippedInput> findSkippedInputs(String sagaId, String quarantineId, int limit);

    /**
     * Record {@code head} under the quarantine {@code quarantineId} of {@code sagaId} with {@code head}'s {@code seq},
     * whatever {@code seq} a record of the same key has already. A record of that key that is there already gets
     * {@code head}'s {@code seq}, and a key with no record gets one. Atomic per key, so no reader finds the key without a
     * record while it moves, and two callers writing one key store one record with the {@code seq} the last of them
     * wrote. Release calls it with the input it starts from at {@code seq} {@code -1}, which moves the record an earlier
     * release left for that input to the head, and records every other input with
     * {@link #recordSkippedInput(String, String, SkippedInput)}.
     */
    void recordSkippedInputAtHead(String sagaId, String quarantineId, SkippedInput head);

    /**
     * Delete the record of {@code input}'s key under the quarantine {@code quarantineId} of {@code sagaId}, only when its
     * {@code seq} is {@code input}'s too. Nothing happens when there is none, or when the record of that key has another
     * {@code seq}, which is what a release that recorded the key again in between left there.
     */
    void deleteSkippedInput(String sagaId, String quarantineId, SkippedInput input);

    /**
     * Delete every record under the quarantine {@code quarantineId} of {@code sagaId}. Records under any other quarantine
     * id stay, so a later instance with the same saga id, which records under a quarantine id of its own, keeps its
     * records. {@link SagaStateStore#delete(String)} does this for the quarantine id of the instance it deletes.
     */
    void deleteSkippedInputs(String sagaId, String quarantineId);
}
