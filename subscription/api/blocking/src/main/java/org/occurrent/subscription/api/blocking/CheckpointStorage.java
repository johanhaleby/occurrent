/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.subscription.api.blocking;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartPositionAlreadyPinnedException;

import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

/**
 * A {@code CheckpointStorage} provides means to read and write the checkpoint to storage.
 * This subscriptions can continue where they left off by passing the {@link Checkpoint} provided by {@link #read(String)}
 * to a {@link CheckpointAwareSubscriptionModel} when the application is restarted etc.
 */
public interface CheckpointStorage {

    /**
     * Read the checkpoint for a given subscription.
     * <p>
     * Note that when starting a new subscription you typically want to create {@link StartAt} from the global checkpoint
     * (using {@link CheckpointAwareSubscriptionModel#globalCheckpoint()}) if no {@code Checkpoint} is found for the given subscription.
     * </p>
     * For example:
     * <pre>
     * Checkpoint checkpoint = storage.read(subscriptionId);
     * if (checkpoint == null) {
     *      checkpoint = checkpointAwareSubscriptionModel.globalCheckpoint();
     *      storage.save(subscriptionId, checkpoint);
     * }
     * StartAt startAt = StartAt.checkpoint(checkpoint);
     * </pre>
     *
     * @param subscriptionId The id of the subscription whose checkpoint to find
     * @return A {@link Checkpoint} data point for the supplied subscriptionId
     */
    @Nullable
    Checkpoint read(@NonNull String subscriptionId);

    /**
     * Save the checkpoint for the supplied subscriptionId to storage, unconditionally, and then return it for
     * easier chaining. This is the same as calling {@link #save(String, Checkpoint, CheckpointWriteCondition)} with
     * {@link CheckpointWriteCondition#any()}, so it always succeeds and leaves the stored version untouched.
     *
     * @param subscriptionId The id of the subscription whose checkpoint to save
     * @param checkpoint     The checkpoint to save
     * @return The checkpoint that was saved, for chaining
     */
    @NullMarked
    default Checkpoint save(String subscriptionId, Checkpoint checkpoint) {
        return save(subscriptionId, checkpoint, CheckpointWriteCondition.any());
    }

    /**
     * Save the checkpoint for the supplied subscriptionId to storage if {@code condition} is fulfilled, and then
     * return it for easier chaining.
     * <p>
     * A store that can evaluate only {@link CheckpointWriteCondition#any()} refuses every other condition with
     * {@link UnsupportedOperationException}, the same answer an event store gives for a capability it was not built
     * with. Check the implementation's own documentation for whether it evaluates conditions.
     *
     * @param subscriptionId The id of the subscription whose checkpoint to save
     * @param checkpoint     The checkpoint to save
     * @param condition      What must be true of the stored version for the write to be allowed
     * @return The checkpoint that was saved, for chaining
     * @throws CheckpointWriteConditionNotFulfilledException if {@code condition} was not fulfilled
     * @throws UnsupportedOperationException                 if this storage cannot evaluate {@code condition}
     */
    @NullMarked
    Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition);

    /**
     * Whether this storage evaluates {@link CheckpointWriteCondition#notOlderThan(long)} and
     * {@link CheckpointWriteCondition#ifAbsent()} for real, rather than refusing them with
     * {@link UnsupportedOperationException}.
     * <p>
     * Answer {@code true} only when {@link #save(String, Checkpoint, CheckpointWriteCondition)} accepts and refuses
     * both as documented, and {@code any()} keeps a stored version as it is. The default is {@code false}.
     * A durable subscription that has to record its first position in a storage answering {@code false} for its id,
     * such as one that starts at the subscription-model default with no checkpoint stored, is then refused with
     * {@code IllegalStateException} before anything is written. Two nodes recording a first position at the same
     * moment could otherwise overwrite each other's and lose the events between the two positions. To start anyway and
     * accept that loss, set {@code startWhenFirstPositionCanBeOverwritten(true)} on the durable subscription model's
     * config, or {@code occurrent.subscription.start-when-first-position-can-be-overwritten=true} with a Spring Boot
     * starter. See ADR 184. A caller that depends on a conditional write can ask before it wires anything up, rather
     * than finding out from an exception on the first write.
     *
     * @return {@code true} if both {@code notOlderThan} and {@code ifAbsent} are evaluated, {@code false} if either of
     * them is refused
     */
    @NullMarked
    default boolean evaluatesWriteConditions() {
        return false;
    }

    /**
     * Whether this storage evaluates {@link CheckpointWriteCondition#notOlderThan(long)} and
     * {@link CheckpointWriteCondition#ifAbsent()} for real for the given {@code subscriptionId}, rather than
     * refusing them with an exception specific to this implementation.
     * <p>
     * Most storages answer the same for every id, so the default delegates to {@link #evaluatesWriteConditions()}.
     * A storage overrides this instead when its answer depends on the id itself, for example one that reserves a
     * shape of id it cannot evaluate a condition for while accepting that same shape for {@link
     * CheckpointWriteCondition#any()}. Answering per id is what lets a caller ask, for the exact ids it plans to
     * use, before it wires anything up, the same "ask before you wire anything up" promise
     * {@link #evaluatesWriteConditions()} already makes for a storage whose answer never varies by id.
     *
     * @param subscriptionId The id to ask about
     * @return {@code true} if both {@code notOlderThan} and {@code ifAbsent} are evaluated for this id, {@code false}
     * if either of them is refused for it
     */
    @NullMarked
    default boolean evaluatesWriteConditionsFor(String subscriptionId) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        return evaluatesWriteConditions();
    }

    /**
     * Read the version currently stored for the supplied subscriptionId, the one a {@link CheckpointWriteCondition}
     * is evaluated against.
     * <p>
     * This is not needed to evaluate a condition, since {@link #save(String, Checkpoint, CheckpointWriteCondition)}
     * does that itself. It exists so a caller can find out which version is stored and why a write keeps being
     * refused, without reading the underlying database by hand.
     *
     * @param subscriptionId The id of the subscription whose stored version to find
     * @return The version stored, or empty if none is stored, including for a storage that cannot evaluate
     * conditions and therefore never records one
     */
    @NullMarked
    OptionalLong writeVersion(String subscriptionId);

    /**
     * Delete the {@link Checkpoint} for the supplied {@code subscriptionId}.
     *
     * @param subscriptionId The id of the subscription to delete the {@link Checkpoint} for.
     */
    @NullMarked
    void delete(String subscriptionId);

    /**
     * Check if the subscription id has a stored checkpoint in this storage.
     *
     * @param subscriptionId The id of the subscription to check.
     * @return <code>true</code> if storage contains a checkpoint for the stream id, <code>false</code> otherwise.
     */
    @NullMarked
    boolean exists(String subscriptionId);

    /**
     * Resolves who a subscription's very first checkpoint should be, between {@code candidate} and whatever
     * {@code subscriptionId} holds now, by which position is earlier rather than by which write reaches storage
     * first or which read happens to run first. A caller reaches for this once a plain
     * {@link #save(String, Checkpoint, CheckpointWriteCondition) save(id, checkpoint, ifAbsent())} is not enough to
     * settle a first position on its own, whether because that write already lost to whatever is stored now, or
     * because {@code candidate} was captured earlier and needs reconciling against whatever governs by the time it
     * is asked about again.
     * <p>
     * A storage that can compare {@code candidate} against whatever it holds now, atomically with any write that
     * comparison calls for, answers with the checkpoint that governs the subscription's first position once this
     * call returns: {@code candidate} once this call has durably written it, whether nothing was stored yet or what
     * was stored proved later, or the stored one, untouched, once this call has confirmed it is earlier than or
     * equal to {@code candidate}. Either way the caller's own race is over, with no
     * {@link StartPositionAlreadyPinnedException} to raise, because a position established this way cannot skip
     * anything {@code candidate} would have covered.
     * <p>
     * The default answers {@link Optional#empty()}, unconditionally, which is the honest answer for a storage with
     * nothing to compare positions by. That leaves the caller to its own fallback, the write-order-based rule this
     * capability exists to narrow: a stored position already there when the caller checked for one is taken without
     * comparison, and one that only appeared afterwards is refused unless reading it back shows the same position
     * {@code candidate} names. A storage overrides this only when it can make that comparison for real. The MongoDB
     * storages, {@code NativeMongoCheckpointStorage} among them, compare the operation time both a stored and an
     * offered {@code org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint} hold. Each checkpoint they
     * store from 0.35.0 on also keeps the operation time of the first position it was delivered on from, so a stored
     * resume token or generic checkpoint whose first position is later than {@code candidate} counts as later too, and
     * {@code candidate} replaces it. They answer empty for any other stored resume token or generic checkpoint, and for
     * one an older Occurrent version stored, which keeps no such operation time. See ADR 130 and ADR 186.
     * <p>
     * The node that stored the later position may already deliver from it, with its version stored next to it, and
     * this keeps that version. So when {@code candidate} replaces a stored checkpoint, the MongoDB storages also
     * store a mark with it, and refuse a {@link CheckpointWriteCondition#notOlderThan(long) notOlderThan} write of a
     * resume token or an operation time while the mark is stored. Without it the next save of that node would store a
     * position after {@code candidate} and skip the events between the two. A write that replaces the stored checkpoint
     * removes the mark, and so does {@link #resolveFirstCheckpointRace(String, Checkpoint, long)} when it keeps the
     * stored checkpoint.
     *
     * @param subscriptionId The id of the subscription whose first position is being contested
     * @param candidate      The position competing to be the subscription's first, whether a losing
     *                       {@code save(id, checkpoint, ifAbsent())} offered it or a caller holds it from earlier
     * @return The checkpoint that now governs {@code subscriptionId}'s first position, or empty when this storage
     * cannot compare the two
     */
    @NullMarked
    default Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate) {
        return Optional.empty();
    }

    /**
     * Settles a subscription's first position and stores {@code writeVersion} with it, in one atomic write, so a caller
     * can ask this before it writes anything. The checkpoint that governs is {@code candidate} when nothing is stored
     * or the stored first position is later, and the stored checkpoint otherwise, so this never moves a stored
     * position later. A stored checkpoint that is {@code candidate} itself governs too, and {@code writeVersion} is
     * stored with it. On the MongoDB storages, a stored resume token or generic checkpoint whose first position is
     * later than an operation-time {@code candidate} counts as a later first position, as for the two-argument
     * {@link #resolveFirstCheckpointRace(String, Checkpoint)}. When the stored checkpoint is another one this storage
     * can't order before {@code candidate}, such as one written for a delivered event whose first position isn't later,
     * nothing is written and this answers {@link Optional#empty()}, as the default does.
     * <p>
     * A start of the blocking {@code DurableSubscriptionModel} at a {@code StartAt.checkpoint(..)} of the stored
     * checkpoint stores its version through
     * {@link #resolveFirstCheckpointRaceWithoutCreatingACheckpoint(String, Checkpoint, long)}, which writes nothing when
     * nothing is stored. A start at the model default from a checkpoint already stored goes through
     * {@link #storeWriteVersion(String, long)}.
     * <p>
     * When the MongoDB storages keep the stored checkpoint, they also remove the mark the two-argument
     * {@link #resolveFirstCheckpointRace(String, Checkpoint)} stores when it moves a position earlier, so a
     * {@code notOlderThan(writeVersion)} save is allowed again, as {@link #storeWriteVersion(String, long)} does.
     * <p>
     * The write is allowed when {@link CheckpointWriteCondition#notOlderThan(long) notOlderThan(writeVersion)} would
     * allow it. When the storage holds a newer version, whatever checkpoint is stored, nothing is written and
     * {@link CheckpointWriteConditionNotFulfilledException} is thrown.
     * <p>
     * The default answers {@link Optional#empty()} and writes nothing, like the default of
     * {@link #resolveFirstCheckpointRace(String, Checkpoint)}. Whenever this answers empty, the blocking
     * {@code DurableSubscriptionModel} writes the first position with {@link CheckpointWriteCondition#ifAbsent()} and
     * stores no version with it, as in 0.34.0. When a checkpoint is already stored, that write is refused, and the
     * subscribe starts from the checkpoint {@link #resolveFirstCheckpointRace(String, Checkpoint)} answers. When that
     * answers empty too, the subscribe is refused with {@code StartPositionAlreadyPinnedException} unless the stored
     * checkpoint is the position it read. The model doesn't save the position again with the version, since that
     * save would write over whatever another node stored between the two writes. Until the next checkpoint write stores the version, a node
     * that has lost its lease can then write over the first position, which delivers events again but skips none.
     * {@code NativeMongoCheckpointStorage}, {@code SpringMongoCheckpointStorage}, {@code SpringRedisCheckpointStorage}
     * and the blocking {@code InMemoryCheckpointStorage} override this method.
     *
     * @param subscriptionId The id of the subscription whose first position is being settled
     * @param candidate      The position competing to be the subscription's first
     * @param writeVersion   The version to store with the checkpoint that governs
     * @return The checkpoint that governs {@code subscriptionId}'s first position once this call returns, stored with
     * {@code writeVersion}, or empty when this storage doesn't settle the first position for this call, and then
     * nothing is written
     * @throws CheckpointWriteConditionNotFulfilledException if the storage holds a version newer than
     *                                                       {@code writeVersion}
     * @throws IllegalArgumentException                      if {@code writeVersion} is negative, as
     *                                                       {@link CheckpointWriteCondition#notOlderThan(long)} throws.
     *                                                       The four storages named above throw it, and the default
     *                                                       answers empty
     */
    @NullMarked
    default Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate, long writeVersion) {
        return Optional.empty();
    }

    /**
     * Settles which checkpoint a subscription goes on from, the way
     * {@link #resolveFirstCheckpointRace(String, Checkpoint, long)} does, but only over a checkpoint that is already
     * stored. When nothing is stored, nothing is written and this answers {@link Optional#empty()}, so a checkpoint
     * that a cancel on another node deleted isn't stored again.
     * <p>
     * When a checkpoint is stored, the one that governs is {@code candidate}, written over the stored one, when the
     * stored first position is later, and the stored checkpoint otherwise, so this never answers a checkpoint later
     * than {@code candidate}. A stored checkpoint that is {@code candidate} itself governs too. {@code writeVersion} is
     * stored with the checkpoint that governs. When the stored checkpoint is another one this storage can't order
     * before {@code candidate}, nothing is written and this answers empty. When the MongoDB storages keep the stored
     * checkpoint, they also remove the mark the two-argument {@link #resolveFirstCheckpointRace(String, Checkpoint)}
     * stores when it moves a position earlier, so a {@code notOlderThan(writeVersion)} save is allowed again.
     * <p>
     * The blocking {@code DurableSubscriptionModel} calls this before it starts a subscription at a
     * {@code StartAt.checkpoint(..)} of the stored checkpoint, with that checkpoint as {@code candidate}, and starts
     * from the checkpoint this answers. That is an earlier one when another node stored it between the model's read
     * and this call. When this answers empty, the subscription starts at {@code candidate} without this node's version
     * stored with it, as in 0.34.0, and the first checkpoint save of a delivered event stores the version.
     * <p>
     * The write is allowed when {@link CheckpointWriteCondition#notOlderThan(long) notOlderThan(writeVersion)} would
     * allow it. When the storage holds a newer version, nothing is written and
     * {@link CheckpointWriteConditionNotFulfilledException} is thrown.
     * <p>
     * The default answers {@link Optional#empty()} and writes nothing.
     * {@code NativeMongoCheckpointStorage}, {@code SpringMongoCheckpointStorage}, {@code SpringRedisCheckpointStorage}
     * and the blocking {@code InMemoryCheckpointStorage} override this method.
     *
     * @param subscriptionId The id of the subscription whose stored checkpoint is settled
     * @param candidate      The checkpoint the subscription is asked to go on from
     * @param writeVersion   The version to store with the checkpoint that governs
     * @return The checkpoint that governs once this call returns, stored with {@code writeVersion}, or empty when
     * nothing is stored, when the stored checkpoint can't be ordered before {@code candidate}, or when this storage
     * doesn't settle it, and then nothing is written
     * @throws CheckpointWriteConditionNotFulfilledException if the storage holds a version newer than
     *                                                       {@code writeVersion}. The four storages named above throw
     *                                                       it, and the default answers empty
     * @throws IllegalArgumentException                      if {@code writeVersion} is negative. The four storages
     *                                                       named above throw it, and the default answers empty
     */
    @NullMarked
    default Optional<Checkpoint> resolveFirstCheckpointRaceWithoutCreatingACheckpoint(String subscriptionId, Checkpoint candidate, long writeVersion) {
        return Optional.empty();
    }

    /**
     * Stores {@code writeVersion} with the checkpoint stored for {@code subscriptionId}, whatever checkpoint that is,
     * and answers that checkpoint. The checkpoint itself is left as it is, in the same atomic write. When nothing is
     * stored, nothing is written and this answers {@link Optional#empty()}, so a checkpoint that a cancel on another
     * node deleted isn't stored again. The MongoDB storages also remove the mark the two-argument
     * {@link #resolveFirstCheckpointRace(String, Checkpoint)} stores when it moves a position earlier, so a
     * {@code notOlderThan(writeVersion)} save is allowed again.
     * <p>
     * The blocking {@code DurableSubscriptionModel} calls this before it starts a subscription from a checkpoint
     * already stored. A node that has lost its lease then can't store the checkpoint of a later event over the one
     * this node starts from, since its next save with {@link CheckpointWriteCondition#notOlderThan(long)} is refused.
     * The subscription starts from the checkpoint this answers, which is the one read unless another node stored a
     * checkpoint after the read. When this answers empty, it starts from the checkpoint it read, without a version,
     * as in 0.34.0.
     * <p>
     * The write is allowed when {@link CheckpointWriteCondition#notOlderThan(long) notOlderThan(writeVersion)} would
     * allow it. When the storage holds a newer version, nothing is written and
     * {@link CheckpointWriteConditionNotFulfilledException} is thrown.
     * <p>
     * The default answers {@link Optional#empty()} and writes nothing.
     * {@code NativeMongoCheckpointStorage}, {@code SpringMongoCheckpointStorage}, {@code SpringRedisCheckpointStorage}
     * and the blocking {@code InMemoryCheckpointStorage} override this method.
     *
     * @param subscriptionId The id of the subscription whose stored checkpoint gets the version
     * @param writeVersion   The version to store with the checkpoint stored
     * @return The checkpoint stored for {@code subscriptionId}, now stored with {@code writeVersion}, or empty when
     * nothing is stored or this storage doesn't store a version this way, and then nothing is written
     * @throws CheckpointWriteConditionNotFulfilledException if the storage holds a version newer than
     *                                                       {@code writeVersion}
     * @throws IllegalArgumentException                      if {@code writeVersion} is negative. The four storages
     *                                                       named above throw it, and the default answers empty
     */
    @NullMarked
    default Optional<Checkpoint> storeWriteVersion(String subscriptionId, long writeVersion) {
        return Optional.empty();
    }
}