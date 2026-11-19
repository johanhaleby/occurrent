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
import org.occurrent.dsl.saga.codec.StoredSaga;

import java.util.List;

/**
 * The capability a {@link SagaStateStore} needs for quarantine. A quarantined instance records here every input it
 * skips, and a release replays those records and deletes them. The runner checks for this with {@code instanceof} on
 * the saga's store, the way {@link SagaInstances} checks for {@link SagaStateStoreQueries}. On a subscription model that
 * holds every event, it refuses to start a saga with quarantine on when its store does not implement it. Call
 * {@code SagaRunnerConfig.disableQuarantine()} to run a saga on such a store.
 * <p>
 * Every write of a record is one of the four {@code compareAndSave} methods here, and each writes the instance and its
 * records together, in one atomic write. The instance is saved only when its stored version is the expected one, and
 * when it is not, the method returns {@code false} and writes nothing at all, neither the instance nor a record. So the
 * records under a quarantine id change only in a write that also changes the version of the instance, and a release
 * that read the instance before its records, and then wins a compare-and-set on the version it read, has seen every
 * record there was.
 * <p>
 * A call that returns normally has made its write survive a failover, because the executor acknowledges the input right
 * after it. On a replicated database that means the write is on a majority of the members, in their journals, before
 * the call returns. The reads of the store, those here and those of {@link SagaStateStore}, return only writes that
 * survive a failover, so the executor never acknowledges an input because it read a write that a failover can roll back.
 * A read can still miss a write acknowledged before it, such as on a new primary right after a failover, except where
 * {@link SagaStateStore#find(String)} says it can't. The executor acts on any other read through a compare-and-set,
 * which a read that missed a write loses, or on the next poll.
 * Inside a transaction the caller opened, the four methods and {@link SagaStateStore#delete(String)}
 * are atomic through that transaction, and the write concern of its commit decides whether the write survives a
 * failover.
 * <p>
 * A store that implements this removes, in {@link SagaStateStore#delete(String)}, the records under the quarantine id of
 * the instance it deletes, in the same atomic write as the instance. It has to persist the quarantine id and the skip
 * count of an instance, which are fields of the {@link StoredSaga} it is given. A store
 * that keeps that {@code StoredSaga} as it is does so already, while a store that maps each field to a column of its own
 * has to add both. {@link org.occurrent.dsl.saga.codec.SagaCodec} refuses to read an instance stored as quarantined
 * without either of them.
 * <p>
 * The runner sees only the store it is given, so a decorator around a store that implements this has to implement it
 * too and pass each call on. On a subscription model that holds every event, a saga with quarantine on and a decorator
 * that does not refuses to start, as above. Otherwise it starts, and {@code SagaSubscription.release(sagaId)} then
 * throws an {@link IllegalStateException}, even for an instance the store does not hold. It names this interface when
 * the runner has an {@code EventStoreQueries}, and names {@code EventStoreQueries} when it has none,
 * instead of returning {@code NOT_FOUND}. The delivery of an input that an instance quarantined before the decorator
 * was put in place has to record as skipped throws the same exception.
 * <p>
 * The interface takes no state type parameter, since its members take an instance as a {@link StoredSaga}, which holds
 * the state encoded already.
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
     * The first {@code limit} records under the quarantine {@code quarantineId} of {@code sagaId}, in
     * {@link SkippedInput#ORDER}, by {@code seq} and then by {@code key}. Records under any other quarantine id are left
     * out.
     * <p>
     * No two records a skip wrote share a {@code seq}. A skip raises the instance's
     * {@link SagaEnvelope#quarantineSkipCount()} in the write that records the input, with the count it read as the
     * record's {@code seq}. A skip of a key that has a record already leaves that record as it is, so its count goes
     * unused and leaves a gap in the numbers. Two records can share {@code -1}, the head record a release writes, when
     * an earlier release kept its own head record under another key for the same input. The order by {@code key} keeps
     * their order the same on every read.
     *
     * @param limit how many records to return at most, at least 1
     */
    List<SkippedInput> findSkippedInputs(String sagaId, String quarantineId, int limit);

    /**
     * Save {@code saga} and record that the quarantined instance skipped {@code input}, in one atomic write.
     * {@code saga} holds no state, and is saved under the rule of
     * {@link SagaStateStore#compareAndSaveWithoutState(StoredSaga, long)}, which leaves the stored state where it is.
     * {@code input} is recorded under the quarantine {@code quarantineId}. A record of the same key under that quarantine
     * id is kept as it is, with its {@code seq}.
     *
     * @return {@code true} when both are written, {@code false} when the stored version is not {@code expectedVersion},
     * and then neither is written
     */
    boolean compareAndSaveSkipped(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput input);

    /**
     * Save {@code saga} and record {@code head}, in one atomic write, which starts a release. {@code saga} holds no
     * state, and is saved under the rule of {@link SagaStateStore#compareAndSaveWithoutState(StoredSaga, long)}.
     * {@code head} is recorded under the quarantine {@code quarantineId} with its own {@code seq}, whatever {@code seq} a
     * record of the same key has. A record of that key gets {@code head}'s {@code seq}, and a key with no record gets
     * one. The executor passes the input the instance was quarantined on at {@code seq} {@code -1}, which moves the
     * record an earlier release kept for that input to the head.
     *
     * @return {@code true} when both are written, {@code false} when the stored version is not {@code expectedVersion},
     * and then neither is written
     */
    boolean compareAndSaveReleaseStarted(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput head);

    /**
     * Save the whole {@code saga}, under the rule of {@link SagaStateStore#compareAndSave(StoredSaga, long)}, and delete
     * the record of {@code replayed}'s key under the quarantine {@code quarantineId}, in one atomic write. The record is
     * deleted only when its {@code seq} is {@code replayed}'s too. Nothing is deleted when the key has no record, or
     * when its record has another {@code seq}. A release calls this for every record it applies, and with {@code saga}
     * one version up and nothing else changed for a record the instance applied already.
     *
     * @return {@code true} when the instance is written, {@code false} when the stored version is not
     * {@code expectedVersion}, and then nothing is written or deleted
     */
    boolean compareAndSaveReplayed(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput replayed);

    /**
     * Save the whole {@code saga}, under the rule of {@link SagaStateStore#compareAndSave(StoredSaga, long)}, and delete
     * every record under the quarantine {@code quarantineId} of its saga id, in one atomic write. A release calls this
     * when it turns the instance active after a read of its records found none, and when a record it replays completes
     * the instance.
     * <p>
     * When {@code saga} is not completed, the write takes place only while no record is under {@code quarantineId}. The
     * release's read of the records can miss one, and deleting it would lose an input the instance skipped and never
     * applied. The store checks inside the atomic write, against every record written before it.
     *
     * @return {@code true} when the instance is written, {@code false} when the stored version is not
     * {@code expectedVersion}, or when {@code saga} is not completed and a record is under {@code quarantineId}, and then
     * nothing is written or deleted
     */
    boolean compareAndSaveQuarantineEnded(StoredSaga saga, long expectedVersion, String quarantineId);
}
