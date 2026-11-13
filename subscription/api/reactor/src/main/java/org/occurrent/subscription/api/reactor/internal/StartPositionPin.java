/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.subscription.api.reactor.internal;

import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StartPositionAlreadyPinnedException;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import reactor.core.publisher.Mono;

import static java.util.Objects.requireNonNull;

/**
 * Internal, not part of the public API. The reactor twin of the blocking {@code StartPositionPin}. Pins a
 * subscription's first position as its stored checkpoint, and resolves a race between two nodes pinning at the same
 * moment by order where the storage can compare the two positions, see ADR 130.
 */
public final class StartPositionPin {

    private StartPositionPin() {
    }

    /**
     * Writes {@code position} as the first checkpoint of {@code subscriptionId}, unless a checkpoint is stored already.
     * A write that loses to a stored checkpoint is settled by {@link CheckpointStorage#resolveFirstCheckpointRace} first.
     * When the storage can't order the two, a checkpoint that existed before {@code position} was read is kept, and one
     * that arrived after it must be {@code position} itself.
     *
     * @param checkpointStorage        The storage that holds the subscription's checkpoint
     * @param subscriptionId           The subscription whose first position is pinned
     * @param checkpointAlreadyExisted Whether a checkpoint was stored when the caller read {@code position}, asked before it
     * @param position                 The position to pin
     * @return A {@link Mono} that completes once the first position is settled. It errors with
     * {@link StartPositionAlreadyPinnedException} when a checkpoint arrived after {@code position} was read, the storage
     * can't order the two, and it holds another position.
     */
    public static Mono<Void> pin(CheckpointStorage checkpointStorage, String subscriptionId, boolean checkpointAlreadyExisted, Checkpoint position) {
        requireNonNull(checkpointStorage, CheckpointStorage.class.getSimpleName() + " cannot be null");
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(position, "position cannot be null");
        // ifAbsent() rather than a read followed by a save, so two nodes pinning the same id at the same moment
        // cannot both write. resolveFirstCheckpointRace is asked first, since a storage that compares the two
        // positions settles the race by order instead of by write order, and checkpointAlreadyExisted only answers
        // a presence question. See ADR 130.
        return Mono.defer(() -> checkpointStorage.save(subscriptionId, position, CheckpointWriteCondition.ifAbsent()))
                .then()
                .onErrorResume(CheckpointWriteConditionNotFulfilledException.class,
                        __ -> checkpointStorage.resolveFirstCheckpointRace(subscriptionId, position)
                                .switchIfEmpty(Mono.defer(() -> checkpointAlreadyExisted ? Mono.empty()
                                        : refuseUnlessTheStoredPositionIsTheOneRead(checkpointStorage, subscriptionId, position)))
                                .then());
    }

    // Something was stored between the read and this write, where this class cannot order it against the position
    // read. Anything other than that same position is refused rather than started from, since that would skip
    // whatever lies between the two.
    private static Mono<Checkpoint> refuseUnlessTheStoredPositionIsTheOneRead(CheckpointStorage storage, String subscriptionId, Checkpoint positionRead) {
        return Mono.defer(() -> storage.read(subscriptionId))
                .onErrorMap(e -> StartPositionAlreadyPinnedException.readingTheStoredPositionBackFailed(subscriptionId, positionRead, e))
                .switchIfEmpty(Mono.error(() -> StartPositionAlreadyPinnedException.readingTheStoredPositionBackFoundNothing(subscriptionId, positionRead)))
                .flatMap(stored -> positionRead.asString().equals(stored.asString())
                        ? Mono.just(stored)
                        : Mono.error(new StartPositionAlreadyPinnedException(subscriptionId, positionRead, stored)));
    }
}
