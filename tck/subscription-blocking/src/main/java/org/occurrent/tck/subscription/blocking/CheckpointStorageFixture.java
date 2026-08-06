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

package org.occurrent.tck.subscription.blocking;

import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;

import java.util.List;

/**
 * What a {@link CheckpointStorage} implementation hands the conformance suite.
 * <p>
 * A fixture is created fresh for every test method, and the storage it hands back <strong>must hold no checkpoints</strong>.
 * How that is achieved is the implementation's business, whether dropping a collection, flushing a database, or
 * constructing a new instance. The suite never cleans up on an implementation's behalf, because what needs cleaning
 * is exactly the part a contract cannot describe.
 * <p>
 * There is one thing the suite has to be told rather than ask, and it is {@link #preservesCheckpointType(Checkpoint)}.
 * Every storage round-trips the value, but they disagree about whether the {@link Checkpoint} that comes back is the
 * same type that went in, and nothing on {@code CheckpointStorage} reports which way a given storage goes.
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
     * whether the type survives.
     * <p>
     * Both answers cost something to give. {@code true} means the type has to come back, and {@code false} means it has
     * to <em>not</em> come back, so a fixture cannot answer {@code false} everywhere to be left alone.
     * <p>
     * Both answers are correct and both are exercised. Occurrent's MongoDB storages recognise their own two checkpoint
     * types and rebuild them, so they answer {@code true} for those and {@code false} for anything else, while the Redis
     * storage answers {@code false} for everything because it stores the string and rebuilds a
     * {@code StringBasedCheckpoint}. This is a declaration rather than a question put to the storage because it is a
     * property of how the storage encodes what it was given, and no method reports it back. That is the same line
     * {@code EventStoreFixture.timePrecision()} sits on.
     *
     * @param checkpoint the checkpoint the suite is about to save
     */
    boolean preservesCheckpointType(Checkpoint checkpoint);

    /**
     * Checkpoint types of the implementation's own, round-tripped in addition to the two the suite always covers.
     * <p>
     * The suite covers {@code StringBasedCheckpoint} and {@code GlobalCheckpoint} for every storage, because both live
     * in {@code occurrent-subscription-core} and {@link Checkpoint#asString()} is the whole contract, so every storage
     * owes an answer for them. A storage that recognises checkpoint types of its own adds them here and the suite
     * round-trips those too. Occurrent's MongoDB storages add their resume-token and operation-time checkpoints.
     * <p>
     * Nothing storage-specific reaches this module, which is why these arrive from the fixture rather than from a
     * dependency the TCK would otherwise have to take on.
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
