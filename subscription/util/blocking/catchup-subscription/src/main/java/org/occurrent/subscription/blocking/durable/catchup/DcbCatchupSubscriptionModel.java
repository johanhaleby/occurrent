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

package org.occurrent.subscription.blocking.durable.catchup;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.StartAtCheckpoint;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.UseCheckpointInStorage;
import org.occurrent.subscription.internal.BoundedIdCache;
import org.occurrent.subscription.util.predicate.EveryN;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * The blocking DCB catch-up path (ADR 20): replays historic DCB events ordered by {@code position} then hands over to
 * a live subscription. Split out of {@code CatchupSubscriptionModel} (see ADR 25) alongside
 * {@code StreamCatchupSubscriptionModel} so the two paths can be depended on independently; this class is the one that
 * still needs {@code eventstore-api-dcb} on the classpath.
 * <p>
 * Delivery is at-least-once, with the same catch-up-to-live handover guarantee documented on the dispatcher: the live
 * resume token is read before the bulk replay. A replay longer than the change stream history runs again from its
 * origin instead of handing the lost token over. When 4 replays in a row lose it, the catch-up fails with an
 * {@code IllegalStateException}, which {@code waitUntilStarted()} throws and the model logs at {@code ERROR}.
 */
@NullMarked
class DcbCatchupSubscriptionModel extends AbstractCatchupSubscriptionModel {

    private final DcbEventStore dcbEventStore;
    private final DcbCriteria dcbQuery;

    public DcbCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, DcbEventStore dcbEventStore, DcbCriteria dcbQuery, CatchupSubscriptionModelConfig config) {
        this(subscriptionModel, dcbEventStore, dcbQuery, config, DcbCatchupSubscriptionModel.class);
    }

    /**
     * @param subscriptionModelContextType The class a caller-supplied {@code StartAt.dynamic} sees as
     *                                      {@code SubscriptionModelContext#subscriptionModelType()} when it is first
     *                                      resolved. The {@code CatchupSubscriptionModel} dispatcher passes its own
     *                                      class here so a caller that pattern-matches on the public dispatcher type
     *                                      keeps working regardless of which mode-specific class runs the catch-up.
     */
    public DcbCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, DcbEventStore dcbEventStore, DcbCriteria dcbQuery, CatchupSubscriptionModelConfig config, Class<?> subscriptionModelContextType) {
        this(subscriptionModel, dcbEventStore, dcbQuery, config, subscriptionModelContextType, new AbstractCatchupSubscriptionModel.SharedCatchupState());
    }

    /**
     * @param sharedState Passed straight to {@link AbstractCatchupSubscriptionModel}. The {@code CatchupSubscriptionModel}
     *                     dispatcher passes the same state to every child it constructs, so a same-id attempt
     *                     routed to a different child on a later call still serializes with this one and still sees
     *                     the same current owner for that id; every other caller gets a fresh, private state
     *                     through the other constructors.
     */
    DcbCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, DcbEventStore dcbEventStore, DcbCriteria dcbQuery, CatchupSubscriptionModelConfig config, Class<?> subscriptionModelContextType, AbstractCatchupSubscriptionModel.SharedCatchupState sharedState) {
        super(subscriptionModel, config, subscriptionModelContextType, sharedState);
        this.dcbEventStore = Objects.requireNonNull(dcbEventStore, "dcbEventStore cannot be null");
        this.dcbQuery = Objects.requireNonNull(dcbQuery, "dcbQuery cannot be null");
    }

    @Override
    protected Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, boolean holdPaused) {
        Objects.requireNonNull(startAt, "Start at supplier cannot be null");
        // A dynamic start that resolves to the model default takes the same resume decision as the default itself,
        // so a replay that stopped in the middle resumes from the position it stored instead of going live
        StartAt requestedStartAt = startAt;
        if (startAt.isDynamic()) {
            StartAt startAtGeneratedByDynamic = startAt.get(generateSubscriptionModelContext());
            if (startAtGeneratedByDynamic == null) {
                return subscribeLiveWithoutCatchup(subscriptionId, filter, startAt, action, holdPaused);
            }
            requestedStartAt = startAtGeneratedByDynamic;
        }
        final StartAt firstStartAt;
        if (requestedStartAt.isDefault()) {
            // Resume from the stored position if there is one, otherwise subscribe live (with the DCB query post-filter).
            Checkpoint checkpoint = returnIfCheckpointStorageConfigIs(UseCheckpointInStorage.class, cfg -> cfg.storage().read(subscriptionId)).orElse(null);
            warnIfStoredWithoutLiveStart(subscriptionId, checkpoint);
            if (checkpoint == null) {
                return subscribeLiveWithoutCatchup(subscriptionId, filter, requestedStartAt, action, holdPaused);
            }
            firstStartAt = StartAt.checkpoint(checkpoint);
        } else {
            firstStartAt = requestedStartAt;
        }

        // A non-DCB position means the catch-up already handed over and the live subscription stored a change-stream
        // token (or the caller asked to start live directly). Subscribe live, still applying the DCB query post-filter.
        if (!isDcbCatchupPosition(firstStartAt)) {
            return subscribeLiveWithoutCatchup(subscriptionId, filter, firstStartAt, action, holdPaused);
        }

        // A replay that stop() cut short runs again from the last position it stored, as a restart would, so the
        // stored position does not move back
        Future<Subscription> subscriptionCompletableFuture = startCatchupAsync(subscriptionId,
                lastStored -> startDcbCatchupSubscription(subscriptionId, filter, startAt, action, lastStored == null ? firstStartAt : StartAt.checkpoint(lastStored)), holdPaused);
        return new CatchupSubscription(subscriptionId, subscriptionCompletableFuture);
    }

    @Override
    @Nullable CatchupReplay replayToResume(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, Checkpoint stored) {
        StartAt storedStartAt = StartAt.checkpoint(stored);
        if (!isDcbCatchupPosition(storedStartAt)) {
            return null;
        }
        return lastStored -> startDcbCatchupSubscription(subscriptionId, filter, startAt, action, lastStored == null ? storedStartAt : StartAt.checkpoint(lastStored));
    }

    /**
     * Hands {@code subscriptionId} straight to the live delegate, without a catch-up phase. Cancels any catch-up
     * already running for this id first, under the same per-id lock as a finishing attempt's own handover, so that
     * attempt is told it has been superseded instead of also subscribing the delegate for the id this call just
     * claimed. Distinct from {@link #handOver}'s own use inside a finishing attempt's handover, which has already
     * gone through that lock and that decision and must not cancel itself.
     */
    private Subscription subscribeLiveWithoutCatchup(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, boolean holdPaused) {
        cancelRunningCatchup(subscriptionId);
        return subscribeInTheWrappedModel(subscriptionId, filter, startAt, dcbLiveConsumer(action, null), holdPaused);
    }

    private Consumer<CloudEvent> dcbLiveConsumer(Consumer<CloudEvent> action, @Nullable BoundedIdCache<CatchupEventKey> cache) {
        return cloudEvent -> {
            // The live change stream sees every event, so keep only DCB events matching the query and skip those
            // already delivered during catch-up. DCB events are identified by isDcbEvent (the tags extension), not by
            // position, since stream events now carry a position too.
            if (DcbCloudEvents.isDcbEvent(cloudEvent) && DcbCloudEvents.matches(cloudEvent, dcbQuery)
                    && (cache == null || !cache.contains(CatchupEventKey.of(cloudEvent)))) {
                action.accept(cloudEvent);
            }
        };
    }

    private Subscription startDcbCatchupSubscription(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, StartAt firstStartAt) {
        long windowSize = config.dcbCatchupPositionWindowSize;

        StartAt nextStartAt = firstStartAt.get(generateSubscriptionModelContext());
        Checkpoint checkpoint = ((StartAtCheckpoint) Objects.requireNonNull(nextStartAt)).checkpoint;

        // The live start is read before the bulk replay, or kept from the attempt that stored the checkpoint, so an
        // event committed during the replay is still delivered live. When the delegate reports no live start at all
        // the catch-up fails (captureLiveResumeCheckpoint). A live start that leaves the change stream history during
        // a long replay is not handed over (replayUntilLiveStartHolds).
        Class<? extends SubscriptionModel> delegatedSubscriptionModelType = getWrappedSubscriptionModel().getClass();
        StartAt delegatedStartAt = startAt.get(new SubscriptionModelContext(delegatedSubscriptionModelType));
        PositionReplayStart replayStart = positionReplayStart(subscriptionId, checkpoint, delegatedStartAt);

        // Page through the DCB sequence from the resume position to the head seen at start, in windows so a large
        // rebuild does not load the whole matched set at once, then reconcile until the head stops advancing.
        // Position is monotonic and server-assigned, so this needs no count and no time sort. Anything written
        // after the reconciliation loop stabilises is newer than the live resume position and arrives live.
        BoundedIdCache<CatchupEventKey> catchupPhaseCache = new BoundedIdCache<>(config.cacheSize);
        Predicate<CloudEvent> persistDuringCatchup = persistDuringCatchupForOneAttempt();
        PositionCatchupPipeline.Reader dcbReader = new PositionCatchupPipeline.Reader() {
            @Override
            public long currentHead() {
                return dcbEventStore.read(dcbQuery, DcbReadOptions.between(0, 0)).lastSequencePosition();
            }

            @Override
            public Stream<CloudEvent> readWindow(long fromExclusive, long toInclusive) {
                return dcbEventStore.read(dcbQuery, DcbReadOptions.between(fromExclusive, toInclusive)).stream();
            }
        };
        PositionCatchupPipeline pipeline = new PositionCatchupPipeline(dcbReader, windowSize);
        PositionReplayStart handoverStart = replayUntilLiveStartHolds(subscriptionId, replayStart, delegatedStartAt,
                start -> replayPositions(subscriptionId, action, pipeline, dcbReader, start, catchupPhaseCache, persistDuringCatchup));
        final Checkpoint globalCheckpoint = handoverStart.liveFrom();

        // Locked from the identity decision through the delegate subscribe call below, same reasoning as the
        // blocking stream catch-up. Unlocked, a cancelSubscription or a fresh subscribe for this id could land in
        // the gap after this attempt decided it was still current but before it finished acting on that, either
        // losing the cancellation or having a fresh attempt's checkpoint save wiped out by this attempt's late
        // delete a few lines down.
        try (HandoverLock ignored = lockHandover(subscriptionId)) {
            // endReplayIfStillCurrent checks stopped and shuttingDown as well as whether this attempt is still the
            // current one, so a stop() before this point parks this attempt's replay instead of handing it over. A
            // later attempt can have taken over just before this call, and only an attempt that ends its own
            // ownership here counts as completed rather than superseded.
            final boolean subscriptionsWasCancelledOrShutdown = !endReplayIfStillCurrent(subscriptionId);

            // Gated on the atomic decision above, not just checked ahead of it: a superseded attempt reaching this
            // late must not delete a later attempt's own temporary position.
            if (delegatedStartAt == null && !subscriptionsWasCancelledOrShutdown) {
                returnIfCheckpointStorageConfigIs(UseCheckpointInStorage.class, cfg -> {
                    cfg.storage().delete(subscriptionId);
                    return null;
                });
            }

            StartAt startAtToUse = StartAt.dynamic(this.<Supplier<StartAt>, UseCheckpointInStorage>returnIfCheckpointStorageConfigIs(UseCheckpointInStorage.class,
                            cfg -> () -> {
                                Checkpoint position = cfg.storage().read(subscriptionId);
                                // If nothing is stored, or the stored position is a DCB position or a time (written by a
                                // catch-up of any kind), save the live change-stream position so the wrapped subscription
                                // resumes from there.
                                if ((position == null || isCatchupCheckpoint(position)) && globalCheckpoint != null) {
                                    position = cfg.storage().save(subscriptionId, globalCheckpoint, writeConditionFor(cfg, subscriptionId));
                                } else if (position == null) {
                                    return delegatedStartAt == null ? startAt : StartAt.subscriptionModelDefault();
                                }
                                return StartAt.checkpoint(position);
                            })
                    .orElse(() -> {
                        if (globalCheckpoint == null) {
                            return delegatedStartAt == null ? startAt : StartAt.subscriptionModelDefault();
                        } else {
                            return StartAt.checkpoint(globalCheckpoint);
                        }
                    }));

            final Subscription subscription;
            if (subscriptionsWasCancelledOrShutdown) {
                // startAtToUse is not resolved here, since resolving it stores the live position, which lies past the
                // history a cancel, a stop or a shutdown kept this replay from reading. A restart would then skip that
                // history, and a cancel would get back the position it deleted.
                subscription = new CancelledSubscription(subscriptionId);
            } else {
                subscription = handOver(subscriptionId, filter, startAtToUse, dcbLiveConsumer(action, catchupPhaseCache));
            }
            return subscription;
        }
    }

    // One pass of the position replay from replayStart
    private void replayPositions(String subscriptionId, Consumer<CloudEvent> action, PositionCatchupPipeline pipeline, PositionCatchupPipeline.Reader reader,
                                 PositionReplayStart replayStart, BoundedIdCache<CatchupEventKey> catchupPhaseCache, Predicate<CloudEvent> persistDuringCatchup) {
        // A resume from a stored live start replays only up to the head the attempt that read it read next. Every
        // event above that head was written after the live start and arrives live, so replaying it too would deliver
        // it twice.
        Long storedReplayTo = replayStart.replayTo();
        long replayTo = storedReplayTo == null ? reader.currentHead() : storedReplayTo;
        pipeline.replay(replayStart.replayFrom(), replayTo, storedReplayTo == null, () -> shouldKeepReplaying(subscriptionId),
                (events, cache) -> deliverCatchupEvents(events, subscriptionId, action, cache, persistDuringCatchup, replayStart, replayTo), catchupPhaseCache,
                () -> historyRead(subscriptionId));
    }

    /**
     * Delivers catch-up events to {@code action}, optionally deduping against {@code cache}, and persists the DCB
     * subscription position for events matching {@code persistDuringCatchup}, the attempt's own predicate, so its count
     * runs on across windows.
     */
    private void deliverCatchupEvents(Stream<CloudEvent> cloudEvents, String subscriptionId, Consumer<CloudEvent> action, @Nullable BoundedIdCache<CatchupEventKey> cache, Predicate<CloudEvent> persistDuringCatchup, PositionReplayStart replayStart, long replayTo) {
        // try-with-resources closes the source stream even when takeWhile short-circuits on shutdown, so a
        // resource-backed read does not leak its cursor.
        try (cloudEvents) {
            Stream<CloudEvent> takeWhile = cloudEvents.takeWhile(__ -> shouldKeepReplaying(subscriptionId));
            if (cache != null) {
                // Skip events already delivered in an earlier reconciliation pass (the delta is re-read until it
                // stabilises, so passes overlap) and record the rest so the live subscription can skip them at the
                // handover seam. Without the filter the overlapping re-reads would deliver duplicates.
                takeWhile = takeWhile.filter(e -> !cache.contains(CatchupEventKey.of(e))).peek(e -> cache.add(CatchupEventKey.of(e)));
            }
            takeWhile
                    .peek(action)
                    // Rechecked here, not just by takeWhile before action ran: see the blocking stream catch-up's
                    // identical reasoning, action can outlast this attempt's ownership. Not shouldKeepReplaying, for
                    // the same reason. The action has completed, so a stop or a shutdown does not keep its position
                    // from being stored, and a replay that runs again after a stop starts from there.
                    .filter(e -> isSafeToPersistFor(subscriptionId))
                    .filter(persistDuringCatchup)
                    .forEach(e -> doIfCheckpointStorageConfigIs(CheckpointStorageConfig.PersistCheckpointDuringCatchupPhase.class,
                            cfg -> saveCatchupCheckpoint(subscriptionId, cfg, replayStart.checkpointAt(OccurrentCloudEventExtension.getPosition(e), replayTo))));
        }
    }

    // One per catch-up attempt, shared by all its windows, so an EveryN configured for the whole model counts this
    // attempt's events only and still fires when n is larger than a window
    private Predicate<CloudEvent> persistDuringCatchupForOneAttempt() {
        return returnIfCheckpointStorageConfigIs(CheckpointStorageConfig.PersistCheckpointDuringCatchupPhase.class, CheckpointStorageConfig.PersistCheckpointDuringCatchupPhase::persistCloudEventPositionPredicate).map(EveryN::forOneSubscription).orElse(__ -> false);
    }

    // firstStartAt is already resolved (non-dynamic) by the time this runs, so the context class used to call get()
    // again is a no-op; generateSubscriptionModelContext() is used anyway for consistency with the other call sites.
    private boolean isDcbCatchupPosition(StartAt startAt) {
        StartAt start = startAt.get(generateSubscriptionModelContext());
        if (!(start instanceof StartAtCheckpoint position)) {
            return false;
        }
        return GlobalCheckpoint.isGlobalCheckpoint(position.checkpoint);
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        cancelRunningCatchup(subscriptionId);
        subscriptionModel.cancelSubscription(subscriptionId);
        deletePositionFromStorage(subscriptionId);
    }

    @Override
    public void shutdown() {
        markShuttingDown();
        subscriptionModel.shutdown();
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DcbCatchupSubscriptionModel.class.getSimpleName() + "[", "]")
                .add("subscriptionModel=" + subscriptionModel)
                .add("dcbEventStore=" + dcbEventStore)
                .add("dcbQuery=" + dcbQuery)
                .add("config=" + config)
                .add("runningCatchupSubscriptions=" + runningCatchupSubscriptions)
                .add("shuttingDown=" + shuttingDown)
                .toString();
    }
}
