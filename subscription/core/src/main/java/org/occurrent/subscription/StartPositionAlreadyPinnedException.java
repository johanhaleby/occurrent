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

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.Optional;

/**
 * A start position was already stored for a subscription id by the time a registration's own write reached
 * storage, and it is not the position that registration read from its position source, so the registration was
 * refused rather than started from a position it never read.
 * <p>
 * Two nodes registering the same subscription for the very first time at close to the same moment is the ordinary
 * way to reach this. One of them writes first and the other is refused. The two positions were read on different
 * machines, {@link Checkpoint} promises nothing beyond {@link Checkpoint#asString()}, and neither node can tell
 * which of the two is earlier, so a subscription starting from the stored position may skip the events written
 * between them. Recovering those events means replaying that interval, which is only safe while the subscription
 * is not running anywhere.
 * <p>
 * One node on its own can reach it too, with no second node involved, when the storage answers a question about
 * an existing checkpoint from a replica that has not seen the write yet. That is why this is named for what the
 * storage held rather than for another registration, which is something the refusal cannot establish.
 * <p>
 * This reports the state of another machine, or of a store, rather than a mistake in the calling code, so it
 * extends {@link IllegalStateException} and does not join {@link SubscriptionRefusedException}'s sealed family,
 * the way {@link CheckpointWriteConditionNotFulfilledException} does not.
 */
@NullMarked
public class StartPositionAlreadyPinnedException extends IllegalStateException {

    /**
     * The id of the subscription whose registration was refused.
     */
    public final String subscriptionId;

    /**
     * The position this registration read from its position source, and would have stored.
     */
    public final Checkpoint positionRead;

    /**
     * The position that was stored instead, or empty when it was removed again, or could not be read, before it
     * could be named here.
     */
    public final Optional<Checkpoint> positionStored;

    /**
     * Creates an exception with the standard message naming both positions. This is the message Occurrent
     * produces for this condition, so prefer this constructor over supplying your own.
     *
     * @param subscriptionId The id of the subscription whose registration was refused
     * @param positionRead   The position this registration read from its position source
     * @param positionStored The position that was stored instead
     */
    public StartPositionAlreadyPinnedException(String subscriptionId, Checkpoint positionRead, Checkpoint positionStored) {
        this(subscriptionId, positionRead, positionStored,
                "Subscription " + subscriptionId + " was registered at position " + positionRead.asString() +
                ", but " + positionStored.asString() + " was already stored for it by the time this registration's " +
                "write reached storage. The two positions were read independently and cannot be compared, so this " +
                "registration is refused rather than started from a position it never read, and the events between " +
                "the two may not reach the subscription. Recovering them means replaying that interval, which is " +
                "only safe while this subscription is not running anywhere.", null);
    }

    /**
     * Creates an exception with a message of your own, for the cases the standard message cannot name a stored
     * position for.
     *
     * @param subscriptionId The id of the subscription whose registration was refused
     * @param positionRead   The position this registration read from its position source
     * @param positionStored The position that was stored instead, or {@code null} when it cannot be named
     * @param message        The message to report
     */
    public StartPositionAlreadyPinnedException(String subscriptionId, Checkpoint positionRead, @Nullable Checkpoint positionStored, String message) {
        this(subscriptionId, positionRead, positionStored, message, null);
    }

    /**
     * As {@link #StartPositionAlreadyPinnedException(String, Checkpoint, Checkpoint, String)}, for a refusal that
     * has a failure to report as well, such as a storage that could not be read.
     *
     * @param subscriptionId The id of the subscription whose registration was refused
     * @param positionRead   The position this registration read from its position source
     * @param positionStored The position that was stored instead, or {@code null} when it cannot be named
     * @param message        The message to report
     * @param cause          The failure that stopped the stored position from being named, or {@code null}
     */
    public StartPositionAlreadyPinnedException(String subscriptionId, Checkpoint positionRead, @Nullable Checkpoint positionStored,
                                               String message, @Nullable Throwable cause) {
        super(message, cause);
        this.subscriptionId = subscriptionId;
        this.positionRead = positionRead;
        this.positionStored = Optional.ofNullable(positionStored);
    }
}
