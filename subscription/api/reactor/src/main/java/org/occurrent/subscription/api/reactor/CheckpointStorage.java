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

package org.occurrent.subscription.api.reactor;

import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StartAt;
import reactor.core.publisher.Mono;

import static java.util.Objects.requireNonNull;


/**
 * A {@code CheckpointStorage} provides means to read and write the checkpoint to storage.
 * This subscriptions can continue where they left off by passing the {@link Checkpoint} provided by {@link #read(String)}
 * to a {@link CheckpointAwareSubscriptionModel} when the application is restarted etc.
 */
@NullMarked
public interface CheckpointStorage {

    /**
     * Read the raw checkpoint for a given subscription.
     * <p>
     * Note that when starting a new subscription you typically want to create {@link StartAt} from the global checkpoint
     * (using {@link CheckpointAwareSubscriptionModel#globalCheckpoint()}) if no {@code Checkpoint} is found for the given subscription.
     * </p>
     * For example:
     * <pre>
     * StartAt startAt = storage.read(subscriptionId)
     *                          .switchIfEmpty(Mono.defer(() -> checkpointAwareSubscriptionModel.globalCheckpoint()
     *                                  .flatMap(checkpoint -> storage.save(subscriptionId, checkpoint, CheckpointWriteCondition.ifAbsent()))))
     *                          .map(StartAt::checkpoint);
     * </pre>
     * <p>
     * The condition on that write is what the read above cannot do on its own. Two callers reading an empty storage
     * at the same moment both get as far as writing, and without it the second write wins and the events between the
     * two positions reach neither. With it the second is refused, which leaves you to decide what that means rather
     * than deciding it by accident. {@code ReactorDurableSubscriptionModel} reads the stored position back and
     * refuses the registration unless it holds the position that caller read. Drop the condition only for a storage
     * that answers {@code false} from {@link #evaluatesWriteConditionsFor(String)}, which cannot evaluate it.
     *
     * @param subscriptionId The id of the subscription whose checkpoint to find
     * @return A Mono with the {@link Checkpoint} data point for the supplied subscriptionId
     */
    Mono<Checkpoint> read(String subscriptionId);

    /**
     * Save the checkpoint for the supplied subscriptionId to storage, unconditionally, and then return it for
     * easier chaining. This is the same as calling {@link #save(String, Checkpoint, CheckpointWriteCondition)} with
     * {@link CheckpointWriteCondition#any()}, so it always succeeds and leaves the stored version untouched.
     *
     * @param subscriptionId The id of the subscription whose checkpoint to save
     * @param checkpoint     The checkpoint to save
     * @return A Mono with the checkpoint that was saved, for chaining
     */
    default Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint) {
        return save(subscriptionId, checkpoint, CheckpointWriteCondition.any());
    }

    /**
     * Save the checkpoint for the supplied subscriptionId to storage if {@code condition} is fulfilled, and then
     * return it for easier chaining.
     * <p>
     * A store that can evaluate only {@link CheckpointWriteCondition#any()} refuses every other condition with a
     * {@link Mono#error(Throwable)} carrying {@link UnsupportedOperationException}, the same answer an event store
     * gives for a capability it was not built with, signalled rather than thrown so nothing escapes assembly. Check
     * the implementation's own documentation for whether it evaluates conditions.
     *
     * @param subscriptionId The id of the subscription whose checkpoint to save
     * @param checkpoint     The checkpoint to save
     * @param condition      What must be true of the stored version for the write to be allowed
     * @return A Mono with the checkpoint that was saved, for chaining, or a Mono signalling
     * {@link CheckpointWriteConditionNotFulfilledException} if {@code condition} was not fulfilled, or
     * {@link UnsupportedOperationException} if this storage cannot evaluate {@code condition}
     */
    Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition);

    /**
     * Whether this storage evaluates {@link CheckpointWriteCondition#notOlderThan(long)} and
     * {@link CheckpointWriteCondition#ifAbsent()} for real, rather than refusing them with
     * {@link UnsupportedOperationException}.
     * <p>
     * Answer {@code true} only when {@link #save(String, Checkpoint, CheckpointWriteCondition)} accepts and refuses
     * both as documented, and {@code any()} carries a stored version forward untouched. The default is {@code false},
     * so a storage that writes unconditionally needs to say nothing. A caller that depends on a conditional write can
     * ask before it wires anything up, rather than finding out from an error signal on the first write.
     *
     * @return {@code true} if both {@code notOlderThan} and {@code ifAbsent} are evaluated, {@code false} if either of
     * them is refused
     */
    default boolean evaluatesWriteConditions() {
        return false;
    }

    /**
     * Whether this storage evaluates {@link CheckpointWriteCondition#notOlderThan(long)} and
     * {@link CheckpointWriteCondition#ifAbsent()} for real for the given {@code subscriptionId}, rather than
     * signalling an error specific to this implementation.
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
     * @return A Mono with the version stored, or an empty Mono if none is stored, including for a storage that
     * cannot evaluate conditions and therefore never records one
     */
    Mono<Long> writeVersion(String subscriptionId);


    /**
     * Delete the {@link Checkpoint} for the supplied {@code subscriptionId}.
     *
     * @param subscriptionId The id of the subscription to delete the {@link Checkpoint} for.
     */
    Mono<Void> delete(String subscriptionId);
}