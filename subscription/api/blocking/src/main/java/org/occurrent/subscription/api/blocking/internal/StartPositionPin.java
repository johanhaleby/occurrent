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

package org.occurrent.subscription.api.blocking.internal;

import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StartPositionAlreadyPinnedException;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Internal, not part of the public API. Pins a subscription's first position as its stored checkpoint, the way
 * {@link org.occurrent.subscription.api.blocking.ManualStartSubscriptionModel} pins one at registration. A race between
 * two nodes pinning at the same moment resolves by order where the storage can compare the two positions, see ADR 130.
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
     * Writes {@code position} as the first checkpoint of {@code subscriptionId} with
     * {@link CheckpointWriteCondition#ifAbsent()}, unless a checkpoint is stored already, the way
     * {@link #pinOrSaveUnconditionally} does, but never falls back to an unconditional write. A storage that can't
     * evaluate {@code ifAbsent()} for this id throws its own refusal, so a caller that promises the conditional pin
     * keeps it.
     *
     * @param checkpointStorage        The storage that holds the subscription's checkpoint
     * @param subscriptionId           The subscription whose first position is pinned
     * @param checkpointAlreadyExisted Whether a checkpoint was stored when the caller read {@code position}, asked before it
     * @param position                 The position to pin
     * @throws StartPositionAlreadyPinnedException when a checkpoint arrived after {@code position} was read, the storage
     *                                             can't order the two, and it holds another position
     */
    public static void pin(CheckpointStorage checkpointStorage, String subscriptionId, boolean checkpointAlreadyExisted, Checkpoint position) {
        requireArguments(checkpointStorage, subscriptionId, position);
        pinIfAbsent(checkpointStorage, subscriptionId, checkpointAlreadyExisted, position);
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
     * @return The checkpoint the subscription goes on from once the first position is settled. That is
     * {@code position} when it is written or is the one stored, and the stored checkpoint when the storage orders it as
     * earlier. It is {@code null} when a checkpoint was stored before {@code position} was read and the storage doesn't
     * order the two, since the stored one is kept and this class can't tell where it is.
     * @throws StartPositionAlreadyPinnedException when a checkpoint arrived after {@code position} was read, the storage
     *                                             can't order the two, and it holds another position
     */
    public static @Nullable Checkpoint pinOrSaveUnconditionally(CheckpointStorage checkpointStorage, String subscriptionId, boolean checkpointAlreadyExisted, Checkpoint position) {
        requireArguments(checkpointStorage, subscriptionId, position);
        // Written with ifAbsent() rather than an exists() check followed by a save(), because those are two calls with
        // nothing holding them together, so two nodes registering the same subscription id at the same moment would
        // both see nothing stored and both write.
        //
        // checkpointAlreadyExisted answers a presence question, not an identity one, and the checkpoint it read can be
        // gone by the time the write below runs, replaced by an unrelated one a cancelSubscription-then-register
        // elsewhere raced in. resolveFirstCheckpointRace is asked first specifically because it settles this by
        // comparing positions atomically with any write it makes, so it is never fooled by that replacement the way
        // trusting checkpointAlreadyExisted alone would be. checkpointAlreadyExisted stays as the fallback's own
        // signal, for a storage that answered resolveFirstCheckpointRace empty. See ADR 130.
        if (!checkpointStorage.evaluatesWriteConditionsFor(subscriptionId)) {
            return saveUnconditionally(checkpointStorage, subscriptionId, checkpointAlreadyExisted, position);
        }
        return pinIfAbsent(checkpointStorage, subscriptionId, checkpointAlreadyExisted, position);
    }

    private static void requireArguments(CheckpointStorage checkpointStorage, String subscriptionId, Checkpoint position) {
        requireNonNull(checkpointStorage, CheckpointStorage.class.getSimpleName() + " cannot be null");
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(position, "position cannot be null");
    }

    private static @Nullable Checkpoint pinIfAbsent(CheckpointStorage checkpointStorage, String subscriptionId, boolean checkpointAlreadyExisted, Checkpoint position) {
        try {
            checkpointStorage.save(subscriptionId, position, CheckpointWriteCondition.ifAbsent());
            return position;
        } catch (CheckpointWriteConditionNotFulfilledException e) {
            Optional<Checkpoint> settled = checkpointStorage.resolveFirstCheckpointRace(subscriptionId, position);
            if (settled.isPresent()) {
                // Settled by position: either this node's own position was durably written in place of a stored one
                // it proved later, or the stored one was confirmed earlier than or equal to this node's own. Neither
                // outcome can have skipped anything this node's position would have covered.
                return settled.get();
            }
            if (checkpointAlreadyExisted) {
                return null;
            }
            refuseUnlessTheStoredPositionIsTheOneRead(checkpointStorage, subscriptionId, position);
            return position;
        }
    }

    // Reached only when the caller accepts the loss of an unconditional first write, as the Spring Boot starters do
    // with occurrent.subscription.start-when-first-position-can-be-overwritten. A stored checkpoint is left alone,
    // because an unconditional write would move the subscription off it.
    private static @Nullable Checkpoint saveUnconditionally(CheckpointStorage storage, String subscriptionId, boolean checkpointAlreadyExisted, Checkpoint position) {
        if (checkpointAlreadyExisted) {
            return null;
        }
        log.warn(UNCONDITIONAL_WRITE_WARNING, storage.getClass().getName(), subscriptionId);
        storage.save(subscriptionId, position);
        return position;
    }

    // Something was stored between the read above and this write, so it was written where this class cannot order
    // it against the position it read. Reading it back answers the only question that settles the registration,
    // whether it holds that same position. Anything else is refused rather than started from a position this
    // registration never read, which would skip whatever lies between the two.
    private static void refuseUnlessTheStoredPositionIsTheOneRead(CheckpointStorage storage, String subscriptionId, Checkpoint positionRead) {
        @Nullable Checkpoint stored;
        try {
            stored = storage.read(subscriptionId);
        } catch (RuntimeException e) {
            throw StartPositionAlreadyPinnedException.readingTheStoredPositionBackFailed(subscriptionId, positionRead, e);
        }
        if (stored == null) {
            throw StartPositionAlreadyPinnedException.readingTheStoredPositionBackFoundNothing(subscriptionId, positionRead);
        }
        if (positionRead.asString().equals(stored.asString())) {
            return;
        }
        throw new StartPositionAlreadyPinnedException(subscriptionId, positionRead, stored);
    }
}
