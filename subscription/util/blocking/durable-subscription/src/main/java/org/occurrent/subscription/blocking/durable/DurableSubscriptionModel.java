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

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
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
    // subscribe(..) counts a subscription id in here for as long as an attempt with its StartAt resolved to null is
    // in flight or has succeeded, opting it out of this model's checkpoint management (the same "not allowed to
    // start" case CompetingConsumerSubscriptionModel has its own set for). A count rather than a plain marker, so a
    // failing duplicate subscribe for an id that is already active only releases its own share, never the marker a
    // still-active or still in-flight attempt for the same id owns. resumeSubscription reads this so it forwards
    // such a subscription unchanged too, rather than resuming it from a checkpoint this model was never asked to
    // manage.
    private final ConcurrentMap<String, AtomicInteger> notCheckpointedSubscriptions = new ConcurrentHashMap<>();

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

        StartAt startAtToUse = generateStartAtPositionFrom(subscriptionId, startAt);
        if (startAtToUse == null) {
            // Not allowed to start, delegate to the wrapped subscription instead. If the wrapped model already
            // knows this id, this is a duplicate attempt against an existing, differently-managed subscription
            // rather than a fresh registration, so it is never counted in at all: the delegate is left to reject
            // it on its own, and this id's real owner (whatever marker state that owner already has) is never
            // touched. Skipping the count-in here is what a concurrent resumeSubscription for that real owner
            // depends on, since counting in first would make it look opted out for as long as this doomed call
            // takes to fail.
            //
            // Otherwise, counted in before the delegate call so a concurrent resumeSubscription sees the opt-out
            // too, released again if the delegate throws so a failing attempt leaves no marker behind for a later
            // resubscribe to inherit, and reinstated if a concurrent cancelSubscription removed it while the
            // delegate call was still running, since a subscription this call just started must stay opted out
            // regardless, but only once the wrapped model still shows it live, or a cancel that landed after this
            // call's own delegate.subscribe() already succeeded would otherwise be resurrected by this reinstate.
            // The increment has to run inside compute(), not after a separate computeIfAbsent() returns the
            // counter, or a decrement for the same id can remove the entry in between and strand the increment on
            // a counter the map no longer holds.
            // "mine" identifies this call's own counter object, so a cancelSubscription-then-resubscribe cycle that
            // replaced it with a fresh one is never mistaken for this call's own share on either path below: the
            // wrapped model refuses a second live registration for the same id, so anything other than "mine" or
            // absent cannot belong to a subscription that is also active right now.
            if (isAlreadyKnownToWrappedModel(subscriptionId)) {
                return getWrappedSubscriptionModel().subscribe(subscriptionId, filter, startAt, action);
            }
            AtomicInteger mine = notCheckpointedSubscriptions.compute(subscriptionId, (id, count) -> {
                AtomicInteger current = count == null ? new AtomicInteger() : count;
                current.incrementAndGet();
                return current;
            });
            try {
                Subscription subscription = getWrappedSubscriptionModel().subscribe(subscriptionId, filter, startAt, action);
                notCheckpointedSubscriptions.compute(subscriptionId, (id, count) -> count == null && isAlreadyKnownToWrappedModel(subscriptionId) ? mine : count);
                return subscription;
            } catch (Throwable t) {
                notCheckpointedSubscriptions.computeIfPresent(subscriptionId, (id, count) -> count != mine ? count : (count.decrementAndGet() <= 0 ? null : count));
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

    // Whether the wrapped model already has a live (running or paused) registration for this id, checked before
    // counting an opt-out attempt in and again before reinstating one after success. Not atomic with what follows
    // it, so it narrows the surrounding races rather than closing them outright. Fully closing them needs same-id
    // subscribe/cancel/resume serialization, a bigger change than this check.
    private boolean isAlreadyKnownToWrappedModel(String subscriptionId) {
        return getWrappedSubscriptionModel().isRunning(subscriptionId) || getWrappedSubscriptionModel().isPaused(subscriptionId);
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
        if (!notCheckpointedSubscriptions.containsKey(subscriptionId)) {
            Optional<RepositionableSubscriptions> repositionable = RepositionableSubscriptions.findIn(getWrappedSubscriptionModel());
            if (repositionable.isPresent()) {
                Checkpoint checkpoint = storage.read(subscriptionId);
                // Re-checked here, not only above, since a concurrent opt-out subscribe can be accepted while the
                // checkpoint read above was still running, and that acceptance must win over a reposition decision
                // this method already made before it knew about it.
                if (checkpoint != null && !notCheckpointedSubscriptions.containsKey(subscriptionId)) {
                    return repositionable.get().resumeSubscription(subscriptionId, StartAt.checkpoint(checkpoint));
                }
            }
        }
        return getWrappedSubscriptionModel().resumeSubscription(subscriptionId);
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
        // Snapshotted before the delegate call, not read again afterward. A fresh subscribe joining the same
        // counter while this delegate call is running changes its count, so the value comparison below leaves
        // that counter alone instead of erasing it the way an unconditional remove used to.
        AtomicInteger before = notCheckpointedSubscriptions.get(subscriptionId);
        int countBefore = before == null ? 0 : before.get();
        subscriptionModel.cancelSubscription(subscriptionId);
        storage.delete(subscriptionId);
        notCheckpointedSubscriptions.computeIfPresent(subscriptionId, (id, count) -> count == before && count.get() == countBefore ? null : count);
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