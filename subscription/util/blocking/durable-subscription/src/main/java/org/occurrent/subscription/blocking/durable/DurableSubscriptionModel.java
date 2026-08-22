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
import java.util.concurrent.atomic.AtomicReference;
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
 *
 * <p>
 * A subscription that asks for {@link StartAt#subscriptionModelDefault()} and has no checkpoint stored yet is
 * recorded from the wrapped model's {@link CheckpointAwareSubscriptionModel#globalCheckpoint()} before anything
 * is delivered, so a crash before the first checkpoint write resumes from the recorded position instead of
 * starting over from wherever the feed has reached by then. A wrapped model that answers {@code null}, which is
 * how it reports a problem it cannot resolve, refuses the subscription with {@link IllegalStateException} from
 * {@link #subscribe(String, SubscriptionFilter, StartAt, Consumer)} rather than starting it without that promise.
 * Nothing is registered for the id, so subscribe again once the model can answer. A subscription with a
 * checkpoint already stored starts from that checkpoint and is never refused this way, and one subscribing with
 * a {@link StartAt} of its own records no position and is never refused either. This is the same answer
 * {@link ManualStartSubscriptionModel} gives for a {@code null} position source and the same one the reactor
 * {@code ReactorDurableSubscriptionModel} gives for the same registration.
 * {@link DurableSubscriptionModelConfig#startWhenNoStartPositionCanBeRecorded(boolean)} turns the refusal into a
 * start without a recorded position, accepting the loss window it documents.
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

    /**
     * Subscribe to events, persisting the checkpoint after each successful call to {@code action} per this
     * model's {@link DurableSubscriptionModelConfig}.
     *
     * @throws IllegalStateException When {@code startAt} resolves to {@link StartAt#subscriptionModelDefault()},
     *                               no checkpoint is stored for {@code subscriptionId}, and the wrapped model's
     *                               {@link CheckpointAwareSubscriptionModel#globalCheckpoint()} answers
     *                               {@code null}, which is how it reports a problem it cannot resolve. Nothing is
     *                               registered for the id, so subscribe again once the model can answer, pass
     *                               a {@link StartAt} of your own, which records no position and makes no resume
     *                               promise, or configure
     *                               {@link DurableSubscriptionModelConfig#startWhenNoStartPositionCanBeRecorded(boolean)}.
     */
    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, @Nullable StartAt startAt, Consumer<CloudEvent> action) {
        Objects.requireNonNull(startAt, StartAt.class.getSimpleName() + " supplier cannot be null");

        // Held for the whole method, not just the opt-out branch, so subscribe, resumeSubscription and
        // cancelSubscription for the same id stay serialized against notCheckpointedSubscriptions (see the field
        // comment above). SpringMongoSubscriptionModel evaluates the returned StartAt synchronously on the first
        // subscribe, so this lock covers that checkpoint read and write too, and it evaluates the StartAt again on
        // a later restartOnce, serialized against its own cancelSubscription by one shared monitor instead
        // (#subscribe, #restartOnce, #cancelSubscription). NativeMongoSubscriptionModel always defers the
        // evaluation to its dispatcher executor, even on the first subscribe, with no such serialization, so its
        // cancelSubscription can race a checkpoint write.
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

            Subscription subscription = subscriptionModel.subscribe(subscriptionId, filter, startAtToUse, cloudEvent -> {
                        action.accept(cloudEvent);
                        if (config.persistCloudEventPositionPredicate.test(cloudEvent)) {
                            Checkpoint checkpoint = getCheckpointOrThrowIAE(cloudEvent);
                            storage.save(subscriptionId, checkpoint, writeConditionFor(subscriptionId));
                        }
                    }
            );
            // Cleared only now, after the delegate accepted this managed subscription, not before: a previous
            // subscribe may have left this id opted out and still active, and a duplicate id the delegate refuses
            // must leave that active subscription's marker alone rather than losing it to this failed attempt.
            notCheckpointedSubscriptions.remove(subscriptionId);
            return subscription;
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

    // Runs on the subscriber's own thread before the wrapped model is handed anything, so the refusal reaches
    // the caller. Thrown from inside the dynamic supplier it would surface on the wrapped model's own evaluation
    // path instead, which NativeMongoSubscriptionModel runs under a retry wrapper that would re-evaluate forever
    // and tell nobody. Answers the checkpoint it recorded, for the supplier's first evaluation, and null when
    // something was stored already or the override let an unanswerable source through.
    private @Nullable Checkpoint recordFirstPositionOrRefuse(String subscriptionId) {
        Checkpoint checkpoint = storage.read(subscriptionId);
        if (checkpoint != null) {
            return null;
        }
        Checkpoint globalCheckpoint = subscriptionModel.globalCheckpoint();
        if (globalCheckpoint == null) {
            if (config.startWhenNoStartPositionCanBeRecorded) {
                return null;
            }
            throw new IllegalStateException("The wrapped subscription model " + subscriptionModel.getClass().getName() +
                                            " answered nothing when asked for the current position for subscription " +
                                            subscriptionId + ", which is how it reports a problem it cannot resolve, and no " +
                                            "checkpoint is stored for the subscription either. Starting it anyway would begin " +
                                            "wherever the feed has reached, and a crash before the first checkpoint is saved " +
                                            "would then start over from wherever the feed has reached by that time, silently " +
                                            "skipping whatever was delivered and failed in between. The subscription is " +
                                            "therefore refused rather than started, and nothing is registered for its id, so " +
                                            "subscribe again once the model can answer. To start anyway, accepting that loss " +
                                            "window, configure DurableSubscriptionModelConfig." +
                                            "startWhenNoStartPositionCanBeRecorded(true), or set " +
                                            "occurrent.subscription.start-when-no-start-position-can-be-recorded=true when " +
                                            "using the Spring Boot starter. A subscription with a checkpoint already stored " +
                                            "starts from that checkpoint and is never refused this way. Subscribing with a " +
                                            "StartAt of your own records no position and makes no such promise.");
        }
        return storage.save(subscriptionId, globalCheckpoint, writeConditionFor(subscriptionId));
    }

    @Nullable
    private StartAt generateStartAtPositionFrom(String subscriptionId, StartAt originalStartAt) {
        final StartAt startAtToUse;
        if (originalStartAt.isDefault()) {
            // Consumed by the supplier's first evaluation, so the position recorded just now is not read back or,
            // on a storage that answers reads from somewhere the write has not reached, saved a second time.
            AtomicReference<@Nullable Checkpoint> recordedFirstPosition = new AtomicReference<>(recordFirstPositionOrRefuse(subscriptionId));
            StartAt startAtIfNoSubscriptionFound = StartAt.subscriptionModelDefault();
            startAtToUse = StartAt.dynamic(() -> {
                Checkpoint recorded = recordedFirstPosition.getAndSet(null);
                if (recorded != null) {
                    return StartAt.checkpoint(recorded);
                }
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