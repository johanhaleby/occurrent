/*
 * Copyright 2021 Johan Haleby
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

package org.occurrent.subscription.blocking.durable;

import io.cloudevents.CloudEvent;
import jakarta.annotation.PreDestroy;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.*;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static org.occurrent.subscription.CheckpointAwareCloudEvent.getCheckpointOrThrowIAE;
import static org.occurrent.subscription.util.predicate.EveryN.everyEvent;

/**
 * Combines a {@link SubscriptionModel} with a {@link CheckpointStorage}, persisting the checkpoint after each
 * successful call to the action in {@link DurableSubscriptionModel#subscribe(String, Consumer)}.
 *
 * <p>
 * By default the checkpoint is written after every event, doubling write load but resuming right after the
 * last delivered event on crash. Pass a {@link DurableSubscriptionModelConfig} with
 * {@link org.occurrent.subscription.util.predicate.EveryN#every(int)} to checkpoint less often, trading fewer
 * writes for events being re-delivered (must be handled idempotently) after a crash.
 */
@NullMarked
public class DurableSubscriptionModel implements CheckpointAwareSubscriptionModel, SubscriptionModelWrapper {

    private final CheckpointAwareSubscriptionModel subscriptionModel;
    private final CheckpointStorage storage;
    private final DurableSubscriptionModelConfig config;
    private final @Nullable CheckpointWriteVersionSource writeVersionSource;
    // subscribe(..) records a subscription id here when its StartAt resolved to null, opting it out of this model's
    // checkpoint management (the same "not allowed to start" case CompetingConsumerSubscriptionModel has its own
    // set for). resumeSubscription reads this so it forwards such a subscription unchanged too, rather than
    // resuming it from a checkpoint this model was never asked to manage. A plain set is safe here only because
    // subscribe, cancelSubscription and resumeSubscription all run under subscriptionIdLock, which makes at most
    // one of them active for a given id at a time, so no two attempts for the same id are ever both live against
    // this set.
    private final Set<String> notCheckpointedSubscriptions = Collections.newSetFromMap(new ConcurrentHashMap<>());
    // Striped rather than one lock object per id, since subscriptionId is caller-supplied to public methods
    // (cancelSubscription, resumeSubscription) and an unknown or made-up id must not grow this without bound. A
    // fixed number of locks bounds memory for good and needs no lifecycle bookkeeping to remove an entry once its
    // holder is gone, at the cost of occasional cross-id serialization when two ids hash to the same stripe. These
    // are startup and reconfiguration calls rather than the event path, so that cost is ordinarily microseconds,
    // but if the delegate or checkpoint storage hangs inside one id's call, every other id sharing its stripe
    // blocks too until it returns.
    private static final int SUBSCRIPTION_ID_LOCK_STRIPES = 1024;
    private final Object[] subscriptionIdLocks = new Object[SUBSCRIPTION_ID_LOCK_STRIPES];

    {
        for (int i = 0; i < subscriptionIdLocks.length; i++) {
            subscriptionIdLocks[i] = new Object();
        }
    }

    private Object lockFor(String subscriptionId) {
        return subscriptionIdLocks[Math.floorMod(subscriptionId.hashCode(), subscriptionIdLocks.length)];
    }

    /**
     * Create a subscription that combines a {@link CheckpointAwareSubscriptionModel} with a {@link CheckpointStorage} to automatically
     * store the subscription after each successful call to <code>action</code> (The "consumer" in {@link #subscribe(String, Consumer)}).
     *
     * @param subscriptionModel The subscription that will read events from the event store
     * @param storage           The {@link CheckpointStorage} that'll be used to persist the stream position
     */
    public DurableSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, CheckpointStorage storage) {
        this(subscriptionModel, storage, new DurableSubscriptionModelConfig(everyEvent()));
    }

    /**
     * Create a subscription that combines a {@link CheckpointAwareSubscriptionModel} with a {@link CheckpointStorage} to automatically
     * store the subscription when the predicate defined in {@link DurableSubscriptionModelConfig#persistCloudEventPositionPredicate} is fulfilled.
     *
     * @param subscriptionModel The subscription that will read events from the event store
     * @param storage           The {@link CheckpointStorage} that'll be used to persist the stream position
     */
    public DurableSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, CheckpointStorage storage,
                                    DurableSubscriptionModelConfig config) {
        this(subscriptionModel, storage, config, null);
    }

    /**
     * Create a subscription that combines a {@link CheckpointAwareSubscriptionModel} with a {@link CheckpointStorage} to automatically
     * store the subscription after each successful call to <code>action</code> (The "consumer" in {@link #subscribe(String, Consumer)}),
     * stamping every checkpoint write with a version from {@code writeVersionSource}.
     *
     * @param subscriptionModel  The subscription that will read events from the event store
     * @param storage            The {@link CheckpointStorage} that'll be used to persist the stream position
     * @param writeVersionSource Asked for a version before each checkpoint write. A version stamps the write
     *                           {@link CheckpointWriteCondition#notOlderThan(long) notOlderThan} it, an empty answer
     *                           or no source at all stamps it {@link CheckpointWriteCondition#any() any()}.
     */
    public DurableSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, CheckpointStorage storage,
                                    CheckpointWriteVersionSource writeVersionSource) {
        this(subscriptionModel, storage, new DurableSubscriptionModelConfig(everyEvent()), writeVersionSource);
    }

    /**
     * Create a subscription that combines a {@link CheckpointAwareSubscriptionModel} with a {@link CheckpointStorage} to automatically
     * store the subscription when the predicate defined in {@link DurableSubscriptionModelConfig#persistCloudEventPositionPredicate} is fulfilled,
     * stamping every checkpoint write with a version from {@code writeVersionSource}.
     *
     * @param subscriptionModel  The subscription that will read events from the event store
     * @param storage            The {@link CheckpointStorage} that'll be used to persist the stream position
     * @param config             The {@link DurableSubscriptionModelConfig} to use
     * @param writeVersionSource Asked for a version before each checkpoint write. A version stamps the write
     *                           {@link CheckpointWriteCondition#notOlderThan(long) notOlderThan} it, an empty answer
     *                           or no source at all stamps it {@link CheckpointWriteCondition#any() any()}.
     */
    public DurableSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, CheckpointStorage storage,
                                    DurableSubscriptionModelConfig config, @Nullable CheckpointWriteVersionSource writeVersionSource) {
        requireNonNull(subscriptionModel, "subscription cannot be null");
        requireNonNull(storage, CheckpointStorage.class.getSimpleName() + " cannot be null");
        requireNonNull(config, DurableSubscriptionModelConfig.class.getSimpleName() + " cannot be null");

        this.storage = storage;
        this.subscriptionModel = subscriptionModel;
        this.config = config;
        this.writeVersionSource = writeVersionSource;
    }

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, @Nullable StartAt startAt, Consumer<CloudEvent> action) {
        Objects.requireNonNull(startAt, StartAt.class.getSimpleName() + " supplier cannot be null");

        // Held for the whole method, not just the opt-out branch, because subscribe, resumeSubscription and
        // cancelSubscription for the same id must stay serialized against notCheckpointedSubscriptions (see the
        // field comment above). It does not cover the checkpoint read and write that generateStartAtPositionFrom's
        // default case defers to its returned supplier. That runs later, when the delegate evaluates the StartAt.
        // SpringMongoSubscriptionModel serializes that evaluation against its own cancelSubscription under one
        // shared monitor (#subscribe, #restartOnce, #cancelSubscription). NativeMongoSubscriptionModel queues the
        // same evaluation onto its dispatcher executor without that serialization, so its cancelSubscription can
        // still race a checkpoint write there. Any CheckpointAwareSubscriptionModel delegate that does not already
        // serialize this itself carries that race.
        synchronized (lockFor(subscriptionId)) {
            StartAt startAtToUse = generateStartAtPositionFrom(subscriptionId, startAt);
            if (startAtToUse == null) {
                // Not allowed to start, delegate to the wrapped subscription instead. Whether it was already
                // marked is captured before marking it, so a duplicate attempt against an already-active,
                // opted-out id releases nothing on failure.
                boolean alreadyMarked = notCheckpointedSubscriptions.contains(subscriptionId);
                notCheckpointedSubscriptions.add(subscriptionId);
                try {
                    return getWrappedSubscriptionModel().subscribe(subscriptionId, filter, startAt, action);
                } catch (Throwable t) {
                    if (!alreadyMarked) {
                        notCheckpointedSubscriptions.remove(subscriptionId);
                    }
                    throw t;
                }
            }

            return subscriptionModel.subscribe(subscriptionId, filter, startAtToUse, cloudEvent -> {
                        action.accept(cloudEvent);
                        if (config.persistCloudEventPositionPredicate.test(cloudEvent)) {
                            Checkpoint checkpoint = getCheckpointOrThrowIAE(cloudEvent);
                            storage.save(subscriptionId, checkpoint, writeConditionFor(subscriptionId));
                        }
                    }
            );
        }
    }

    // A version from writeVersionSource stamps notOlderThan. An empty answer or no source stamps any(). Always the
    // 3-arg save, never a choice between two.
    private CheckpointWriteCondition writeConditionFor(String subscriptionId) {
        if (writeVersionSource == null) {
            return CheckpointWriteCondition.any();
        }
        OptionalLong version = writeVersionSource.writeVersion(subscriptionId);
        return version.isPresent() ? CheckpointWriteCondition.notOlderThan(version.getAsLong()) : CheckpointWriteCondition.any();
    }

    @Nullable
    private StartAt generateStartAtPositionFrom(String subscriptionId, StartAt originalStartAt) {
        final StartAt startAtToUse;
        if (originalStartAt.isDefault()) {
            StartAt startAtIfNoSubscriptionFound = StartAt.subscriptionModelDefault();
            startAtToUse = StartAt.dynamic(() -> {
                // Read inside the supplier so a retry picks up the latest checkpoint, not a stale one
                Checkpoint checkpoint = storage.read(subscriptionId);
                if (checkpoint == null) {
                    Checkpoint globalCheckpoint = subscriptionModel.globalCheckpoint();
                    if (globalCheckpoint != null) {
                        checkpoint = storage.save(subscriptionId, globalCheckpoint, writeConditionFor(subscriptionId));
                    }
                }

                return checkpoint == null ? startAtIfNoSubscriptionFound : StartAt.checkpoint(checkpoint);
            });
        } else if (originalStartAt.isDynamic()) {
            var subscriptionModelContext = new SubscriptionModelContext(DurableSubscriptionModel.class);
            var nextStartAt = originalStartAt.get(subscriptionModelContext);
            if (nextStartAt != null) {
                return generateStartAtPositionFrom(subscriptionId, nextStartAt);
            }
            return null;
        } else {
            startAtToUse = originalStartAt;
        }
        return startAtToUse;
    }

    @Override
    public void stop() {
        getWrappedSubscriptionModel().stop();
    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        getWrappedSubscriptionModel().start(resumeSubscriptionsAutomatically);
    }

    @Override
    public boolean isRunning() {
        return getWrappedSubscriptionModel().isRunning();
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return getWrappedSubscriptionModel().isRunning(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return getWrappedSubscriptionModel().isPaused(subscriptionId);
    }

    /**
     * Resume a paused subscription from the checkpoint stored for it, rather than from the position the wrapped
     * model itself last read to. Those two agree for a subscription only this model ever drives, but not for one a
     * {@code CompetingConsumerSubscriptionModel} pauses and resumes on lease handover, where another node can have
     * moved the checkpoint forward while this node held no lease at all, and its own wrapped model has no way to
     * know that happened.
     * <p>
     * Falls back to the wrapped model's own {@link SubscriptionModelLifeCycle#resumeSubscription(String)} when no
     * checkpoint is stored yet, when the wrapped model does not implement {@link RepositionableSubscriptions},
     * or when the subscription opted out of this model's checkpoint management in the first place (see
     * {@link #subscribe(String, SubscriptionFilter, StartAt, Consumer)}). The fallback is deliberately the wrapped
     * model's own tracked position, never {@link StartAt#subscriptionModelDefault()}, which resolves to the
     * present and would silently drop whatever was published while this subscription was paused.
     */
    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        // Held for the whole decision, reposition call included, so a concurrent subscribe or cancelSubscription
        // for this id cannot land between the marker check and acting on it.
        synchronized (lockFor(subscriptionId)) {
            if (!notCheckpointedSubscriptions.contains(subscriptionId)) {
                Optional<RepositionableSubscriptions> repositionable = RepositionableSubscriptions.findIn(getWrappedSubscriptionModel());
                if (repositionable.isPresent()) {
                    Checkpoint checkpoint = storage.read(subscriptionId);
                    if (checkpoint != null) {
                        return repositionable.get().resumeSubscription(subscriptionId, StartAt.checkpoint(checkpoint));
                    }
                }
            }
            return getWrappedSubscriptionModel().resumeSubscription(subscriptionId);
        }
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
        getWrappedSubscriptionModel().pauseSubscription(subscriptionId);
    }

    /**
     * Cancel a subscription. This means that it'll no longer receive events as they are persisted to the event store.
     * The checkpoint that is persisted in the {@link CheckpointStorage} will also be removed.
     *
     * @param subscriptionId The subscription id to cancel
     */
    @Override
    public void cancelSubscription(String subscriptionId) {
        synchronized (lockFor(subscriptionId)) {
            subscriptionModel.cancelSubscription(subscriptionId);
            storage.delete(subscriptionId);
            notCheckpointedSubscriptions.remove(subscriptionId);
        }
    }

    @Override
    @PreDestroy
    public void shutdown() {
        subscriptionModel.shutdown();
    }

    @Nullable
    @Override
    public Checkpoint globalCheckpoint() {
        return subscriptionModel.globalCheckpoint();
    }

    @Override
    public CheckpointAwareSubscriptionModel getWrappedSubscriptionModel() {
        return subscriptionModel;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DurableSubscriptionModel.class.getSimpleName() + "[", "]")
                .add("subscriptionModel=" + subscriptionModel)
                .add("storage=" + storage)
                .add("config=" + config)
                .toString();
    }
}