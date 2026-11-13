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

import static java.util.Objects.requireNonNull;

/**
 * Internal, not part of the public API. Pins a subscription's first position as its stored checkpoint, the way
 * {@link org.occurrent.subscription.api.blocking.ManualStartSubscriptionModel} pins one at registration. A race between
 * two nodes pinning at the same moment resolves by order where the storage can compare the two positions, see ADR 130.
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
     * @throws StartPositionAlreadyPinnedException when a checkpoint arrived after {@code position} was read, the storage
     *                                             can't order the two, and it holds another position
     */
    public static void pin(CheckpointStorage checkpointStorage, String subscriptionId, boolean checkpointAlreadyExisted, Checkpoint position) {
        requireNonNull(checkpointStorage, CheckpointStorage.class.getSimpleName() + " cannot be null");
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(position, "position cannot be null");
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
        try {
            checkpointStorage.save(subscriptionId, position, CheckpointWriteCondition.ifAbsent());
        } catch (CheckpointWriteConditionNotFulfilledException e) {
            if (checkpointStorage.resolveFirstCheckpointRace(subscriptionId, position).isPresent()) {
                // Settled by position: either this node's own position was durably written in place of a stored one
                // it proved later, or the stored one was confirmed earlier than or equal to this node's own. Neither
                // outcome can have skipped anything this node's position would have covered.
                return;
            }
            if (checkpointAlreadyExisted) {
                return;
            }
            refuseUnlessTheStoredPositionIsTheOneRead(checkpointStorage, subscriptionId, position);
        }
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
