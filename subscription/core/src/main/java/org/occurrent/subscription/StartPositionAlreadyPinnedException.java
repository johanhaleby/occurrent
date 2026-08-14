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

import static java.util.Objects.requireNonNull;

/**
 * A start position was already stored for a subscription id by the time a registration's own write reached
 * storage, and the registration could not confirm it to be the position it read from its position source, so it
 * was refused rather than started from a position it never read.
 * <p>
 * Three ways to fail that confirmation. The stored position read back differs from the one this registration
 * read, or reading it back found nothing, or reading it failed, and the last two are told apart by whether
 * {@link #getCause()} is set. Only the first shows two positions that disagree. The other two are refused
 * because nothing here can show they agree, which is the same answer for a weaker reason.
 * <p>
 * A read that finds nothing is not proof the checkpoint was removed. A checkpoint deleted between the write and
 * the read answers that way, and so does a read served from somewhere that has not caught up with the write, and
 * this class cannot tell those apart.
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
     * The position storage held when it was read back after the refusal, or empty when that read found nothing or
     * failed.
     * <p>
     * That read is a second call, and no checkpoint storage reports the value that actually refused the write, so
     * a checkpoint advanced in between is what this holds, by whichever writer advanced it. Treat it as the
     * position stored at the moment it was read rather than as the one that won.
     */
    public final Optional<Checkpoint> positionStored;

    /**
     * Creates an exception with the standard message naming both positions. This is the message Occurrent
     * produces for this condition, so prefer this constructor over supplying your own.
     *
     * @param subscriptionId The id of the subscription whose registration was refused
     * @param positionRead   The position this registration read from its position source
     * @param positionStored The position storage held when it was read back after the refusal
     */
    public StartPositionAlreadyPinnedException(String subscriptionId, Checkpoint positionRead, Checkpoint positionStored) {
        this(subscriptionId, positionRead, positionStored,
                standardMessage(subscriptionId, positionRead, positionStored), null);
    }

    // Builds the message through the same checks the constructor makes, so a null argument is reported as the
    // argument it is rather than as a failure to read a position off it.
    private static String standardMessage(String subscriptionId, Checkpoint positionRead, Checkpoint positionStored) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(positionRead, Checkpoint.class.getSimpleName() + " read at registration cannot be null");
        requireNonNull(positionStored, Checkpoint.class.getSimpleName() + " read back from storage cannot be null");
        return "Subscription " + subscriptionId + " was registered at position " + positionRead.asString() +
               ", but recording it was refused because a position was already stored for this subscription id. " +
               "Reading that back found " + positionStored.asString() + ", in a second call, so it is what storage " +
               "held at that moment rather than certainly the position that refused the write. The two positions " +
               "were read independently and cannot be compared, so this registration is refused rather than " +
               "started from a position it never read, and the events between them may not reach the " +
               "subscription. Recovering them means replaying that interval, which is only safe while this " +
               "subscription is not running anywhere.";
    }

    /**
     * Creates an exception with a message of your own, for the cases the standard message cannot name a stored
     * position for.
     *
     * @param subscriptionId The id of the subscription whose registration was refused
     * @param positionRead   The position this registration read from its position source
     * @param positionStored The position storage held when it was read back, or {@code null} when it cannot be named
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
     * @param positionStored The position storage held when it was read back, or {@code null} when it cannot be named
     * @param message        The message to report
     * @param cause          The failure that stopped the stored position from being named, or {@code null}
     */
    public StartPositionAlreadyPinnedException(String subscriptionId, Checkpoint positionRead, @Nullable Checkpoint positionStored,
                                               String message, @Nullable Throwable cause) {
        super(requireNonNull(message, "Message cannot be null"), cause);
        this.subscriptionId = requireNonNull(subscriptionId, "subscriptionId cannot be null");
        this.positionRead = requireNonNull(positionRead, Checkpoint.class.getSimpleName() + " read at registration cannot be null");
        this.positionStored = Optional.ofNullable(positionStored);
    }
}
