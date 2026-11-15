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
 * A store that implements this persists every member of {@link SagaEnvelope} and {@link SagaFailure},
 * {@link SagaFailure#releasedAt()}, {@link SagaFailure#quarantineId()} and {@link SagaEnvelope#quarantineSkipCount()}
 * included, and removes an instance's records in {@link SagaStateStore#delete(String)}. The executor reads an instance
 * back after the write that quarantines it and after a release starts, because an instance without a quarantine id or
 * a skip count records nothing it skips. When the quarantine id or the skip count did not persist, it writes the
 * instance back as it was before the quarantine and fails the input, so every delivery of that input fails until the
 * store persists both. When a release finds one of those members missing, it throws.
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
     * Delete the record of {@code key} under the quarantine {@code quarantineId} of {@code sagaId}, whatever its
     * {@code seq}. Nothing happens when there is none. Release uses it only to move the record of the input it starts
     * from to the head, and deletes every other record with {@link #deleteSkippedInput(String, String, SkippedInput)}.
     */
    void deleteSkippedInputByKey(String sagaId, String quarantineId, String key);

    /**
     * Delete the record of {@code input}'s key under the quarantine {@code quarantineId} of {@code sagaId}, only when its
     * {@code seq} is {@code input}'s too. Nothing happens when there is none, or when the record of that key has another
     * {@code seq}, which is what a release that recorded the key again in between left there.
     */
    void deleteSkippedInput(String sagaId, String quarantineId, SkippedInput input);

    /**
     * Delete every record of {@code sagaId}, under every quarantine id. {@link SagaStateStore#delete(String)} does this
     * too.
     */
    void deleteSkippedInputs(String sagaId);
}
