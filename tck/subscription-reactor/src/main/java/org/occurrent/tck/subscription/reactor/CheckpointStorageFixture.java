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

package org.occurrent.tck.subscription.reactor;

import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.reactor.CheckpointStorage;

import java.util.List;

/**
 * What a reactive {@link CheckpointStorage} implementation hands the conformance suite.
 * <p>
 * This is the reactive twin of {@code org.occurrent.tck.subscription.blocking.CheckpointStorageFixture}, kept as a
 * separate interface rather than a shared one because the storage it hands back is the reactive
 * {@link CheckpointStorage}, whose {@code save}, {@code read}, {@code writeVersion} and {@code delete} all return a
 * {@code Mono} instead of the blocking storage's plain values. A fixture is created fresh for every test method, and
 * the storage it hands back <strong>must hold no checkpoints</strong>. How that is achieved is the implementation's
 * business, whether dropping a collection, flushing a database, or constructing a new instance. The suite never
 * cleans up on an implementation's behalf, because what needs cleaning is exactly the part a contract cannot
 * describe.
 */
@NullMarked
public interface CheckpointStorageFixture {

    /**
     * The storage under test, holding no checkpoints.
     */
    CheckpointStorage checkpointStorage();

    /**
     * Whether {@code read} gives back a checkpoint of the same type that {@code save} was given, for this checkpoint.
     * <p>
     * A storage that answers {@code false} still has to round-trip {@link Checkpoint#asString()} faithfully, which is
     * the whole of what {@code Checkpoint} promises, and the suite asserts that either way. What differs is only
     * whether the type survives. See the blocking fixture's version of this method for the full reasoning, which
     * applies unchanged here.
     *
     * @param checkpoint the checkpoint the suite is about to save
     */
    boolean preservesCheckpointType(Checkpoint checkpoint);

    /**
     * Whether this storage evaluates a {@link org.occurrent.subscription.CheckpointWriteCondition} for real.
     * <p>
     * {@code true}, the default, means {@code notOlderThan} and {@code ifAbsent} are both accepted and refused as
     * documented, and {@code any()} leaves a stored version untouched and carries it forward. {@code false} means
     * this storage refuses every condition but {@link org.occurrent.subscription.CheckpointWriteCondition#any()}
     * with {@link UnsupportedOperationException} signalled through {@link reactor.core.publisher.Mono#error(Throwable)},
     * the interim answer some of Occurrent's own storages give until a sibling change teaches them the real
     * comparison. The storage reports the same property itself, through
     * {@code CheckpointStorage.evaluatesWriteConditions()}. The suite still asks the fixture rather than the storage,
     * so a storage whose answer disagrees with what it does is tested against the fixture's declaration and fails on
     * the disagreement.
     */
    default boolean evaluatesWriteConditions() {
        return true;
    }

    /**
     * Checkpoint types of the implementation's own, round-tripped in addition to the two the suite always covers.
     * <p>
     * The suite covers {@code StringBasedCheckpoint} and {@code GlobalCheckpoint} for every storage, because both live
     * in {@code occurrent-subscription-core} and {@link Checkpoint#asString()} is the whole contract, so every storage
     * owes an answer for them. A storage that recognises checkpoint types of its own adds them here and the suite
     * round-trips those too.
     */
    default List<Checkpoint> additionalCheckpoints() {
        return List.of();
    }

    /**
     * Releases whatever the fixture opened. Called after every test method, including a failing one.
     */
    default void close() {
    }
}
