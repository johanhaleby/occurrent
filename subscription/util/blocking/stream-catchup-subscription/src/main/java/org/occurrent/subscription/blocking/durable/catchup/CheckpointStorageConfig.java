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

package org.occurrent.subscription.blocking.durable.catchup;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.CheckpointWriteVersionSource;
import org.occurrent.subscription.util.predicate.EveryN;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * Configures if and how checkpoint persistence should be handled during the catch-up phase.
 */
@NullMarked
public sealed interface CheckpointStorageConfig {

    /**
     * Don't use a checkpoint storage. The catch-up subscription will start from beginning of time each time it is started (for example
     * each time the application is restarted).
     *
     * @return An instance of {@link DontUseCheckpointInStorage}.
     */
    static DontUseCheckpointInStorage dontUseCheckpointStorage() {
        return new DontUseCheckpointInStorage();
    }

    /**
     * Use a specific storage instance. The catch-up subscription will use this storage to check if a position has already been persisted,
     * and if so the catch-up subscription, will continue from this position. The catch-up subscription will delegate to the wrapping subscription
     * if the position belongs to it.
     * <br><br>
     * This is really useful if you want to start-off with a catch-up subscription but then automatically continue with from the wrapped subscription
     * position once the events have caught up.
     * <br><br>
     * Note that if this setting is not combined with {@link UseCheckpointInStorage#andPersistCheckpointDuringCatchupPhaseForEveryNEvents(int)}
     * or {@link UseCheckpointInStorage#andPersistCheckpointDuringCatchupPhaseWhen(Predicate)} the checkpoint
     * is will not be stored during the catch-up phase. This means that if the application crashes during catch-up it'll restart from the beginning
     * when the application is restarted. Combine this settings with any of the two methods defined above to alleviate this, if deemed required.
     *
     * @param storage The storage to use. Must be the same instance as used by the wrapped subscription in order to allow continuing from the checkpoint
     *                on application restart.
     * @return A {@link UseCheckpointInStorage} instance.
     */
    static UseCheckpointInStorage useCheckpointStorage(CheckpointStorage storage) {
        return new UseOnlyCheckpointInStorage(storage, null);
    }

    /**
     * Use a specific storage instance, stamping every checkpoint write this configuration triggers with a version
     * from {@code writeVersionSource} (see ADR 116). Otherwise the same as {@link #useCheckpointStorage(CheckpointStorage)}.
     *
     * @param storage            The storage to use. Must be the same instance as used by the wrapped subscription in order to allow continuing from the checkpoint
     *                           on application restart.
     * @param writeVersionSource Asked for a version before each checkpoint write. A version stamps the write
     *                           {@code notOlderThan} it, an empty answer or no source at all stamps it {@code any()}.
     * @return A {@link UseCheckpointInStorage} instance.
     */
    static UseCheckpointInStorage useCheckpointStorage(CheckpointStorage storage, CheckpointWriteVersionSource writeVersionSource) {
        return new UseOnlyCheckpointInStorage(storage, writeVersionSource);
    }

    record DontUseCheckpointInStorage() implements CheckpointStorageConfig {
    }

    sealed interface UseCheckpointInStorage extends CheckpointStorageConfig {
        CheckpointStorage storage();

        /**
         * The source asked for a version before every checkpoint write this configuration triggers, or
         * {@code null} for none, in which case every write is unconditional. Set by
         * {@link #useCheckpointStorage(CheckpointStorage, CheckpointWriteVersionSource)}.
         *
         * @return The configured {@link CheckpointWriteVersionSource}, or {@code null}.
         */
        @Nullable CheckpointWriteVersionSource checkpointWriteVersionSource();

        /**
         * Configure the catch-up subscription to periodically store the event position in a storage in case
         * the application is restarted during the catch-up phase. On restart the application will continue from the
         * last stored position, instead of starting from the beginning. This is useful if you have lot's of events
         * and don't want to risk starting from the beginning on failure!
         *
         * @param persistCloudEventPositionPredicate A predicate that evaluates to <code>true</code> if the cloud event position should be persisted for the <i>catch-up</i> subscription.
         *                                           See {@link EveryN}. Supply a predicate that always returns {@code false} to never store the position.
         * @return An instance of {@link PersistCheckpointDuringCatchupPhase}
         * @see EveryN
         */
        default PersistCheckpointDuringCatchupPhase andPersistCheckpointDuringCatchupPhaseWhen(Predicate<CloudEvent> persistCloudEventPositionPredicate) {
            return new PersistCheckpointDuringCatchupPhase(storage(), persistCloudEventPositionPredicate, checkpointWriteVersionSource());
        }

        /**
         * Configure the catch-up subscription to periodically store the event position in a storage in case
         * the application is restarted during the catch-up phase. On restart the application will continue from the
         * last stored position, instead of starting from the beginning. This is useful if you have lot's of events
         * and don't want to risk starting from the beginning on failure!
         *
         * @param persistPositionForEveryNCloudEvent Persist the position of every N cloud event so that it's possible to avoid restarting from scratch when the <i>catch-up</i> subscription is restarted.
         * @return An instance of {@link PersistCheckpointDuringCatchupPhase}
         */
        default PersistCheckpointDuringCatchupPhase andPersistCheckpointDuringCatchupPhaseForEveryNEvents(int persistPositionForEveryNCloudEvent) {
            return new PersistCheckpointDuringCatchupPhase(storage(), EveryN.every(persistPositionForEveryNCloudEvent), checkpointWriteVersionSource());
        }
    }

    /**
     * @param storage                            The storage that will maintain the checkpoint during catch-up mode.
     * @param persistCloudEventPositionPredicate A predicate that evaluates to <code>true</code> if the cloud event position should be persisted. See {@link EveryN}.
     *                                           Supply a predicate that always returns {@code false} to never store the position.
     * @param checkpointWriteVersionSource       Asked for a version before every checkpoint write this configuration triggers, or {@code null} for none.
     * @see UseCheckpointInStorage#andPersistCheckpointDuringCatchupPhaseWhen(Predicate)
     * @see UseCheckpointInStorage#andPersistCheckpointDuringCatchupPhaseForEveryNEvents(int)
     */
    record PersistCheckpointDuringCatchupPhase(CheckpointStorage storage,
                                                         Predicate<CloudEvent> persistCloudEventPositionPredicate,
                                                         @Nullable CheckpointWriteVersionSource checkpointWriteVersionSource) implements UseCheckpointInStorage {
        public PersistCheckpointDuringCatchupPhase {
            Objects.requireNonNull(storage, CheckpointStorage.class.getSimpleName() + " cannot be null");
            Objects.requireNonNull(persistCloudEventPositionPredicate, "persistCloudEventPositionPredicate cannot be null");
        }
    }

    /**
     * @param storage                      The storage to use.
     * @param checkpointWriteVersionSource Asked for a version before every checkpoint write this configuration triggers, or {@code null} for none.
     */
    record UseOnlyCheckpointInStorage(CheckpointStorage storage, @Nullable CheckpointWriteVersionSource checkpointWriteVersionSource) implements UseCheckpointInStorage {
        public UseOnlyCheckpointInStorage {
            Objects.requireNonNull(storage, CheckpointStorage.class.getSimpleName() + " cannot be null");
        }
    }
}