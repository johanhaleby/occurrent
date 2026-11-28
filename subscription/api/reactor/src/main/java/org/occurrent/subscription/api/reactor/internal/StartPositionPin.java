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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import static java.util.Objects.requireNonNull;

/**
 * Internal, not part of the public API. The reactor twin of the blocking {@code StartPositionPin}. Pins a
 * subscription's first position as its stored checkpoint, and resolves a race between two nodes pinning at the same
 * moment by order where the storage can compare the two positions, see ADR 130.
 */
public final class StartPositionPin {

    private static final Logger log = LoggerFactory.getLogger(StartPositionPin.class);

    private static final String UNCONDITIONAL_WRITE_WARNING = "Checkpoint storage {} does not evaluate write conditions for subscription {}, so the first " +
                                                              "position recorded for it is written unconditionally. Two nodes recording a first position " +
                                                              "for this subscription at the same moment can then lose the events between the two positions. " +
                                                              "Answer true from evaluatesWriteConditionsFor(String) on a storage that does evaluate " +
                                                              "ifAbsent(), or use one of the storages Occurrent ships, to close that.";

    private StartPositionPin() {
    }

    /**
     * Writes {@code position} as the first checkpoint of {@code subscriptionId}, unless a checkpoint is stored already.
     * A write that loses to a stored checkpoint is settled by {@link CheckpointStorage#resolveFirstCheckpointRace} first.
     * When the storage can't order the two, a checkpoint that existed before {@code position} was read is kept, and one
     * that arrived after it must be {@code position} itself. A storage that answers {@code false} from
     * {@link CheckpointStorage#evaluatesWriteConditionsFor(String)} gets an unconditional write with a warning instead,
     * unless a checkpoint was stored already. Two nodes pinning at the same moment can then overwrite each other's first
     * position and lose the events between the two, so a caller refuses that case first unless it accepts the loss.
     *
     * @param checkpointStorage        The storage that holds the subscription's checkpoint
     * @param subscriptionId           The subscription whose first position is pinned
     * @param checkpointAlreadyExisted Whether a checkpoint was stored when the caller read {@code position}, asked before it
     * @param position                 The position to pin
     * @return A {@link Mono} of the checkpoint the subscription goes on from once the first position is settled. That
     * is {@code position} when it is written or is the one stored, and the stored checkpoint when the storage orders it
     * as earlier. It completes empty when a checkpoint was stored before {@code position} was read and the storage
     * doesn't order the two, since the stored one is kept and this class can't tell where it is. It errors
     * with {@link StartPositionAlreadyPinnedException} when a checkpoint arrived after {@code position} was read, the
     * storage can't order the two, and it holds another position.
     */
    public static Mono<Checkpoint> pin(CheckpointStorage checkpointStorage, String subscriptionId, boolean checkpointAlreadyExisted, Checkpoint position) {
        requireNonNull(checkpointStorage, CheckpointStorage.class.getSimpleName() + " cannot be null");
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(position, "position cannot be null");
        // ifAbsent() rather than a read followed by a save, so two nodes pinning the same id at the same moment
        // cannot both write. resolveFirstCheckpointRace is asked first, since a storage that compares the two
        // positions settles the race by order instead of by write order, and checkpointAlreadyExisted only answers
        // a presence question. See ADR 130.
        if (!checkpointStorage.evaluatesWriteConditionsFor(subscriptionId)) {
            return saveUnconditionally(checkpointStorage, subscriptionId, checkpointAlreadyExisted, position);
        }
        return Mono.defer(() -> checkpointStorage.save(subscriptionId, position, CheckpointWriteCondition.ifAbsent()))
                .thenReturn(position)
                .onErrorResume(CheckpointWriteConditionNotFulfilledException.class,
                        __ -> checkpointStorage.resolveFirstCheckpointRace(subscriptionId, position)
                                .switchIfEmpty(Mono.defer(() -> checkpointAlreadyExisted ? Mono.empty()
                                        : refuseUnlessTheStoredPositionIsTheOneRead(checkpointStorage, subscriptionId, position))));
    }

    // Reached only when the caller accepts the loss of an unconditional first write, as the Spring Boot starters do
    // with occurrent.subscription.start-when-first-position-can-be-overwritten. A stored checkpoint is left alone,
    // because an unconditional write would move the subscription off it.
    private static Mono<Checkpoint> saveUnconditionally(CheckpointStorage storage, String subscriptionId, boolean checkpointAlreadyExisted, Checkpoint position) {
        if (checkpointAlreadyExisted) {
            return Mono.empty();
        }
        return Mono.defer(() -> {
            log.warn(UNCONDITIONAL_WRITE_WARNING, storage.getClass().getName(), subscriptionId);
            return storage.save(subscriptionId, position);
        }).thenReturn(position);
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
