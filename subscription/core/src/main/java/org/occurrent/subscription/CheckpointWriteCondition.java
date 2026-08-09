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

package org.occurrent.subscription;

/**
 * What must be true of a checkpoint's stored version before {@code CheckpointStorage.save} is allowed to write it.
 * <p>
 * A checkpoint write states its condition the same way an event store write does, except the version here comes
 * from the caller rather than from the store, and the store never learns where it comes from. That is what lets a
 * competing-consumer subscription refuse a write made from a lease it has already lost, without a fencing token
 * ever appearing in the vocabulary of a checkpoint store.
 * <p>
 * This is sealed with exactly the three cases below. A store that cannot evaluate anything but {@link #any()}
 * refuses the others with {@link UnsupportedOperationException}, which is the same answer an event store gives for
 * a capability it was not built with.
 */
public sealed interface CheckpointWriteCondition {

    /**
     * The version stored does not matter. The write always succeeds and leaves the stored version exactly as it
     * was, carrying it forward rather than clearing it.
     *
     * @return A {@link CheckpointWriteCondition} with the behavior described above.
     */
    static CheckpointWriteCondition any() {
        return new Any();
    }

    /**
     * The write succeeds when nothing is stored, or when the stored version is not greater than {@code writeVersion}.
     * On success the stored version becomes {@code writeVersion}. Otherwise the write is refused with a
     * {@link CheckpointWriteConditionNotFulfilledException}.
     * <p>
     * Nothing stored is accepted rather than refused, because it means a checkpoint written before this condition
     * existed, and every checkpoint written by an earlier release of Occurrent has to stay readable.
     *
     * @param writeVersion The version this write represents, assigned by the caller.
     * @return A {@link CheckpointWriteCondition} with the behavior described above.
     */
    static CheckpointWriteCondition notOlderThan(long writeVersion) {
        return new NotOlderThan(writeVersion);
    }

    /**
     * The write succeeds only when no checkpoint is stored yet for the subscription id, whatever version it would
     * carry. Otherwise the write is refused with a {@link CheckpointWriteConditionNotFulfilledException}.
     * <p>
     * This exists for a caller that wants to write a subscription's very first checkpoint, and only the very first
     * one, so it can fix the subscription's start position without racing another writer through {@link #any()}.
     * <p>
     * One edge is deliberately tolerated. A save offering exactly the value that is already stored may be reported
     * as success rather than refused, since some storages tell the two outcomes apart by comparing values. The
     * stored checkpoint is identical either way.
     *
     * @return A {@link CheckpointWriteCondition} with the behavior described above.
     */
    static CheckpointWriteCondition ifAbsent() {
        return new IfAbsent();
    }

    /**
     * See {@link #any()}.
     */
    record Any() implements CheckpointWriteCondition {
    }

    /**
     * See {@link #notOlderThan(long)}.
     *
     * @param writeVersion The version this write represents.
     */
    record NotOlderThan(long writeVersion) implements CheckpointWriteCondition {
    }

    /**
     * See {@link #ifAbsent()}.
     */
    record IfAbsent() implements CheckpointWriteCondition {
    }
}
