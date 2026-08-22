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
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.*;
import org.occurrent.subscription.StartAt.StartAtCheckpoint;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.PersistCheckpointDuringCatchupPhase;
import org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.UseCheckpointInStorage;
import org.occurrent.subscription.internal.BoundedIdCache;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.occurrent.condition.Condition.gt;
import static org.occurrent.eventstore.api.SortBy.SortDirection.DESCENDING;
import static org.occurrent.filter.Filter.time;
import static org.occurrent.time.internal.RFC3339.RFC_3339_DATE_TIME_FORMATTER;

/**
 * The blocking stream catch-up path: replays historic stream events (by {@code position} when the store writes one,
 * otherwise by the legacy time/{@code $natural} order) then hands over to a live subscription. Split out of
 * {@code CatchupSubscriptionModel} (ADR 25) so a stream-only application does not need {@code eventstore-api-dcb}.
 * The dispatcher {@code CatchupSubscriptionModel} wraps this class for its stream routing.
 * <p>
 * Only ever replays and delivers stream-capability events. On a store with both {@code STREAM} and {@code DCB}
 * capabilities enabled, this is enforced, not descriptive: a
 * {@link Filter#capability(EventStoreCapability) STREAM-capability filter} is ANDed into both catch-up reads and
 * the live-subscription filter, so a DCB-tagged event never reaches a subscriber of this class (ADR 50). A caller
 * filter is still honored, with the capability guard composed on top.
 * <p>
 * Delivery is at-least-once, with the same handover and clock-skew-safe reconciliation guarantees documented on
 * the dispatcher.
 */
@NullMarked
public class StreamCatchupSubscriptionModel extends AbstractCatchupSubscriptionModel {

    // Guards every read and live handover so a DCB-tagged event never reaches a stream subscriber, even when both
    // capabilities are enabled (ADR 50). Store-agnostic: each Filter-conversion implementation maps this capability
    // to its own storage artifact.
    private static final Filter STREAM_CAPABILITY_FILTER = Filter.capability(EventStoreCapability.STREAM);

    private final EventStoreQueries eventStoreQueries;
    // The capability guard ANDed into every read and live handover: STREAM_CAPABILITY_FILTER for a stream
    // subscription, null for a capability-agnostic one, which then delivers events of every capability.
    private final @Nullable Filter capabilityScope;

    public StreamCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, EventStoreQueries eventStoreQueries, CatchupSubscriptionModelConfig config) {
        this(subscriptionModel, eventStoreQueries, config, StreamCatchupSubscriptionModel.class);
    }

    /**
     * @param subscriptionModelContextType The class a caller-supplied {@code StartAt.dynamic} sees as
     *                                      {@code SubscriptionModelContext#subscriptionModelType()} when it is first
     *                                      resolved. The {@code CatchupSubscriptionModel} dispatcher passes its own
     *                                      class here so a caller that pattern-matches on the public dispatcher type
     *                                      keeps working regardless of which mode-specific class runs the catch-up.
     */
    StreamCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, EventStoreQueries eventStoreQueries, CatchupSubscriptionModelConfig config, Class<?> subscriptionModelContextType) {
        this(subscriptionModel, eventStoreQueries, config, subscriptionModelContextType, STREAM_CAPABILITY_FILTER);
    }

    /**
     * @param sharedState Passed straight to {@link AbstractCatchupSubscriptionModel}, with the stream capability
     *                     scope. Lets {@code CatchupSubscriptionModel} share one state with this instance without
     *                     reaching {@link #STREAM_CAPABILITY_FILTER}, which is private to this class.
     */
    StreamCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, EventStoreQueries eventStoreQueries, CatchupSubscriptionModelConfig config, Class<?> subscriptionModelContextType, AbstractCatchupSubscriptionModel.SharedCatchupState sharedState) {
        this(subscriptionModel, eventStoreQueries, config, subscriptionModelContextType, STREAM_CAPABILITY_FILTER, sharedState);
    }

    /**
     * @param capabilityScope The capability {@link Filter} ANDed into every catch-up read and every live handover.
     *                        Pass {@link #STREAM_CAPABILITY_FILTER} for a stream subscription, or {@code null} for a
     *                        capability-agnostic subscription that delivers events of every capability, filtered only by
     *                        the caller's plain {@link Filter}.
     */
    StreamCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, EventStoreQueries eventStoreQueries, CatchupSubscriptionModelConfig config, Class<?> subscriptionModelContextType, @Nullable Filter capabilityScope) {
        this(subscriptionModel, eventStoreQueries, config, subscriptionModelContextType, capabilityScope, new AbstractCatchupSubscriptionModel.SharedCatchupState());
    }

    /**
     * @param sharedState Passed straight to {@link AbstractCatchupSubscriptionModel}. The {@code CatchupSubscriptionModel}
     *                     dispatcher passes the same state to every child it constructs, so a same-id attempt
     *                     routed to a different child on a later call still serializes with this one and still sees
     *                     the same current owner for that id; every other caller gets a fresh, private state
     *                     through the other constructors.
     */
    StreamCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, EventStoreQueries eventStoreQueries, CatchupSubscriptionModelConfig config, Class<?> subscriptionModelContextType, @Nullable Filter capabilityScope, AbstractCatchupSubscriptionModel.SharedCatchupState sharedState) {
        super(subscriptionModel, config, subscriptionModelContextType, sharedState);
        this.eventStoreQueries = Objects.requireNonNull(eventStoreQueries, "eventStoreQueries cannot be null");
        this.capabilityScope = capabilityScope;
    }

    /**
     * Shortcut to start subscribing to events matching the supplied filter from beginning of time.
     */
    public Subscription subscribeFromBeginningOfTime(String subscriptionId, SubscriptionFilter filter, Consumer<CloudEvent> action) {
        return subscribe(subscriptionId, filter, StartAtTime.beginningOfTime(), action);
    }

    /**
     * Shortcut to start subscribing to <i>all</i> events from beginning of time.
     */
    public Subscription subscribeFromBeginningOfTime(String subscriptionId, Consumer<CloudEvent> action) {
        return subscribe(subscriptionId, StartAtTime.beginningOfTime(), action);
    }

    /**
     * Whether the wrapped store carries a global position. A store without {@link PositionOrderedReader}, or one that
     * reports {@code writesPosition()==false}, stays on the time-ordered catch-up path.
     */
    private boolean streamStoreWritesPosition() {
        return eventStoreQueries instanceof PositionOrderedReader positionOrderedReader && positionOrderedReader.writesPosition();
    }

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        Objects.requireNonNull(startAt, "Start at supplier cannot be null");
        // Position/time catch-up converts the filter into an Occurrent Filter for the historical query, so only the
        // filter types that wrap a plain Filter are supported here: StreamSubscriptionFilter (stream) and
        // AgnosticSubscriptionFilter (capability-agnostic). The DCB path accepts a DcbSubscriptionFilter and passes it
        // to its own model.
        if (filter != null && !(filter instanceof StreamSubscriptionFilter) && !(filter instanceof AgnosticSubscriptionFilter)) {
            throw new UnsupportedSubscriptionFilterException(filter.getClass(), "Only StreamSubscriptionFilter or AgnosticSubscriptionFilter is supported!");
        }
        boolean positionMode = streamStoreWritesPosition();
        final StartAt firstStartAt;
        if (startAt.isDefault()) {
            // Resume from a stored position if there is one, otherwise delegate to the parent subscription model.
            Checkpoint checkpoint = returnIfCheckpointStorageConfigIs(UseCheckpointInStorage.class, cfg -> cfg.storage().read(subscriptionId)).orElse(null);
            if (checkpoint == null) {
                // Resumed straight to live without a catch-up phase, so scope the delegated subscription the same way
                // the handover would, keeping DCB events out.
                return subscribeLiveWithoutCatchup(subscriptionId, withCapabilityScope(filter), startAt, action);
            } else if (positionMode && isTimeBasedCheckpoint(checkpoint)) {
                // The store now writes position, but this stored token predates that and is time-based. Reading it as a
                // position would misinterpret a timestamp or replay from an unrelated cursor, so re-resolve to the
                // model default instead.
                return subscribeLiveWithoutCatchup(subscriptionId, withCapabilityScope(filter), StartAt.subscriptionModelDefault(), action);
            } else {
                firstStartAt = StartAt.checkpoint(checkpoint);
            }
        } else if (startAt.isDynamic()) {
            StartAt startAtGeneratedByDynamic = startAt.get(generateSubscriptionModelContext());
            if (startAtGeneratedByDynamic == null) {
                // Not allowed to start this subscription model, defer to parent
                return subscribeLiveWithoutCatchup(subscriptionId, withCapabilityScope(filter), startAt, action);
            } else {
                firstStartAt = startAtGeneratedByDynamic;
            }
        } else {
            firstStartAt = startAt;
        }

        StreamStart streamStart = classifyStreamStart(firstStartAt, subscriptionModelContextType);
        if (positionMode) {
            return switch (streamStart) {
                // Beginning-of-time maps to position 0 so the position catch-up replays all history.
                case BEGINNING_OF_TIME -> streamPositionCatchup(subscriptionId, filter, startAt, action, StartAt.checkpoint(GlobalCheckpoint.of(0)));
                case GLOBAL_POSITION -> streamPositionCatchup(subscriptionId, filter, startAt, action, firstStartAt);
                // A specific wall-clock time has no position to map to, so replay it through the legacy time-based
                // catch-up even on a position store.
                case SPECIFIC_TIME -> streamTimeCatchup(subscriptionId, filter, startAt, action, firstStartAt);
                case LIVE -> subscribeLiveWithoutCatchup(subscriptionId, withCapabilityScope(filter), firstStartAt, action);
            };
        }
        return switch (streamStart) {
            case BEGINNING_OF_TIME, SPECIFIC_TIME -> streamTimeCatchup(subscriptionId, filter, startAt, action, firstStartAt);
            case GLOBAL_POSITION, LIVE -> subscribeLiveWithoutCatchup(subscriptionId, withCapabilityScope(filter), firstStartAt, action);
        };
    }

    /**
     * Hands {@code subscriptionId} straight to the live delegate, without a catch-up phase. Cancels any catch-up
     * already running for this id first, under the same per-id lock as a finishing attempt's own handover, so that
     * attempt is told it has been superseded instead of also subscribing the delegate for the id this call just
     * claimed. Distinct from the delegate subscribe call inside a finishing attempt's own handover, which has
     * already gone through that lock and that decision and must not cancel itself.
     */
    private Subscription subscribeLiveWithoutCatchup(String subscriptionId, StreamSubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        cancelRunningCatchup(subscriptionId);
        return getWrappedSubscriptionModel().subscribe(subscriptionId, filter, startAt, action);
    }

    // Resolved start kinds for a stream subscription. Classifying once keeps the routing above an exhaustive switch,
    // so a new kind forces both switches to handle it instead of silently falling through to live delivery, which
    // once dropped specific-time replay on a position store.
    private enum StreamStart {BEGINNING_OF_TIME, SPECIFIC_TIME, GLOBAL_POSITION, LIVE}

    // Resolve the start once, then branch on the resolved checkpoint. Beginning-of-time must be checked before
    // specific-time because the beginning-of-time checkpoint is itself a TimeBasedCheckpoint.
    private static StreamStart classifyStreamStart(StartAt startAt, Class<?> contextType) {
        StartAt resolved = startAt.get(new SubscriptionModelContext(contextType));
        if (resolved instanceof StartAtCheckpoint start) {
            Checkpoint checkpoint = start.checkpoint;
            if (isBeginningOfTime(checkpoint)) {
                return StreamStart.BEGINNING_OF_TIME;
            }
            if (isTimeBasedCheckpoint(checkpoint)) {
                return StreamStart.SPECIFIC_TIME;
            }
            if (GlobalCheckpoint.isGlobalCheckpoint(checkpoint)) {
                return StreamStart.GLOBAL_POSITION;
            }
        }
        return StreamStart.LIVE;
    }

    /**
     * Whether a resolved {@code startAt} is an explicit {@link GlobalCheckpoint}. Shared with the dispatcher's
     * routing, which needs the same check before any mode-specific class is even chosen; {@code contextType} is the
     * class the dispatcher (or this class, standalone) reports to a caller-supplied {@code StartAt.dynamic}.
     */
    static boolean startsAtExplicitGlobalPosition(StartAt startAt, Class<?> contextType) {
        StartAt start = startAt.get(new SubscriptionModelContext(contextType));
        return start instanceof StartAtCheckpoint position
                && GlobalCheckpoint.isGlobalCheckpoint(position.checkpoint);
    }

    private Subscription streamPositionCatchup(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, StartAt positionStartAt) {
        Future<Subscription> future = startCatchupAsync(subscriptionId, () -> startPositionCatchupSubscriptionForStream(subscriptionId, filter, startAt, action, positionStartAt));
        return new CatchupSubscription(subscriptionId, future);
    }

    private Subscription streamTimeCatchup(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, StartAt firstStartAt) {
        Future<Subscription> future = startCatchupAsync(subscriptionId, () -> startCatchupSubscription(subscriptionId, filter, startAt, action, firstStartAt));
        return new CatchupSubscription(subscriptionId, future);
    }

    private Subscription startCatchupSubscription(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, StartAt firstStartAt) {
        StartAt nextStartAt = firstStartAt.get(generateSubscriptionModelContext());
        Checkpoint checkpoint = ((StartAtCheckpoint) Objects.requireNonNull(nextStartAt)).checkpoint;

        Filter catchupFilter = deriveFilterToUseDuringCatchupPhase(filter, checkpoint);

        long numberOfEventsBeforeStartingCatchupSubscription = eventStoreQueries.count(catchupFilter);

        // Perform the catchup
        runCatchupForStream(eventStoreQueries.query(catchupFilter, config.catchupPhaseSortBy), subscriptionId, action, null);

        // The delegated subscription model may be configured to never store its position durably, e.g. @Subscription
        // with startAt=BEGINNING_OF_TIME and resume=SAME_AS_START_AT: since every restart replays from beginning of
        // time anyway, no position needs to be stored. This lets in-memory projections/views/policies catch up.
        //
        // The wrapping subscription is forced to be a CheckpointAwareSubscriptionModel to capture where live
        // delivery should resume. Captured *after* the bulk replay, not before, so the token stays fresh: capturing
        // it before a long replay risks it ageing out of the change stream (e.g. MongoDB's oplog) before handover.
        // Events written during the replay are not covered by this checkpoint; they are reconciled separately by
        // the insertion-order delta below. A null resume token fails loudly (captureLiveResumeCheckpoint) instead
        // of silently resuming live at "now" and dropping events committed during the replay; the position path
        // captures its checkpoint before its replay instead, for the same guarantee.
        Class<? extends SubscriptionModel> delegatedSubscriptionModelType = getWrappedSubscriptionModel().getClass();
        StartAt delegatedStartAt = startAt.get(new SubscriptionModelContext(delegatedSubscriptionModelType));
        final Checkpoint globalCheckpoint = captureLiveResumeCheckpoint(delegatedStartAt);

        // Cache to avoid re-delivering events already streamed during catch-up when they arrive again live.
        BoundedIdCache catchupPhaseCache = new BoundedIdCache(config.cacheSize);

        // Reconcile events written after the bulk replay started but at or before the live resume position
        // (globalCheckpoint): read the newest N in insertion order (SortBy.natural descending + limit, no skip)
        // and reverse for delivery.
        //
        // Selecting by insertion order rather than the configurable catchupPhaseSortBy is what keeps this loss-free
        // under clock skew (ADR 0014): a during-catch-up event whose time sorts before the already-processed
        // boundary would be missed by both a time-sorted reconcile and by live delivery. Insertion order also reads
        // only the recent tail instead of the whole backlog.
        //
        // The count to read comes from a count query, but more events can be written before the read runs, shifting
        // the window and pushing an old during-catch-up event out. Re-read until the matching count stops growing:
        // a pass with no new event has delivered them all. Re-reads are deduped by the cache (at-least-once).
        // Anything written after a pass is newer than globalCheckpoint and is covered by live delivery regardless.
        // Everything the bulk replay was going to deliver has been delivered by now, so what follows is the events
        // written since it started. A recording projection records those and skips the history above it. Skipped when
        // the replay was truncated, since a history that stopped part way through is not a history that was read.
        if (shouldKeepReplaying(subscriptionId)) {
            historyRead(subscriptionId);
        }

        long reconciledThroughCount = numberOfEventsBeforeStartingCatchupSubscription;
        long matchingEventCount = eventStoreQueries.count(catchupFilter);
        while (matchingEventCount > reconciledThroughCount && shouldKeepReplaying(subscriptionId)) {
            long numberOfEventsToReconcile = matchingEventCount - numberOfEventsBeforeStartingCatchupSubscription;
            // Read the delta in bounded windows, newest-window-first (skip counts down from the full delta), instead
            // of materializing the whole delta in one ArrayList, mirroring the position path's window delivery. Each
            // window is still read and reversed in natural-order-descending, so events within and across windows are
            // delivered oldest first.
            long remaining = numberOfEventsToReconcile;
            while (remaining > 0 && shouldKeepReplaying(subscriptionId)) {
                long windowCountAsLong = Math.min(remaining, Math.min(config.dcbCatchupPositionWindowSize, Integer.MAX_VALUE));
                int windowCount = (int) windowCountAsLong;
                long skip = remaining - windowCount;
                List<CloudEvent> window = new ArrayList<>(eventStoreQueries.query(catchupFilter, Math.toIntExact(skip), windowCount, SortBy.natural(DESCENDING)).toList());
                Collections.reverse(window);
                runCatchupForStream(window.stream(), subscriptionId, action, catchupPhaseCache);
                remaining -= windowCount;
            }
            reconciledThroughCount = matchingEventCount;
            matchingEventCount = eventStoreQueries.count(catchupFilter);
        }

        // Locked from the identity decision through the delegate subscribe call below. Unlocked, a
        // cancelSubscription or a fresh subscribe for this id could land in the gap after this attempt decided it
        // was still current but before it finished acting on that, either losing the cancellation or, for a fresh
        // attempt, having its own checkpoint save wiped out by this attempt's late delete a few lines down.
        try (HandoverLock ignored = lockHandover(subscriptionId)) {
            // endReplayIfStillCurrent gates on stopped/shuttingDown as well as identity, so a stop() that lands
            // before this point still leaves this attempt's marker in place instead of removing it (mirrors the
            // pre-#737 behavior for that case), and it is the atomic decision itself: a later attempt can still
            // have taken over in the narrow window right before this call, and only actually ending this attempt's
            // ownership here may count as a normal completion rather than being superseded.
            final boolean subscriptionsWasCancelledOrShutdown = !endReplayIfStillCurrent(subscriptionId);

            // If the delegate is not allowed to subscribe, remove the temporary position written during catch-up now
            // that it's done. Gated on the same atomic decision above (not just checked ahead of it), so a superseded
            // attempt reaching this late cannot delete a later attempt's own temporary position.
            if (delegatedStartAt == null && !subscriptionsWasCancelledOrShutdown) {
                returnIfCheckpointStorageConfigIs(UseCheckpointInStorage.class, cfg -> {
                    cfg.storage().delete(subscriptionId);
                    return null;
                });
            }

            // Store the global position once catch-up is ready so a subscription that got no new events during
            // replay still resumes from it after a restart, instead of replaying history again. Uses
            // UseCheckpointInStorage rather than PersistCheckpointDuringCatchupPhase because using storage at all
            // implies the wrapped subscription should continue from where this left off.
            StartAt startAtToUse = StartAt.dynamic(this.<Supplier<StartAt>, UseCheckpointInStorage>returnIfCheckpointStorageConfigIs(UseCheckpointInStorage.class,
                            cfg -> () -> {
                                // Read inside the supplier so a retry picks up the latest checkpoint
                                Checkpoint position = cfg.storage().read(subscriptionId);
                                // Nothing stored, or a time-based position from catch-up: save globalCheckpoint, since
                                // the wrapped subscription may not support time-based positions.
                                if ((position == null || isTimeBasedCheckpoint(position)) && globalCheckpoint != null) {
                                    position = cfg.storage().save(subscriptionId, globalCheckpoint, writeConditionFor(cfg, subscriptionId));
                                } else if (position == null) {
                                    // globalCheckpoint is also null: start at subscriptionModelDefault if the delegate may subscribe
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

            Consumer<CloudEvent> liveConsumer = cloudEvent -> {
                if (!catchupPhaseCache.contains(cloudEvent.getId())) {
                    action.accept(cloudEvent);
                }
            };
            return startDelegatedSubscription(subscriptionId, filter, subscriptionsWasCancelledOrShutdown, startAtToUse, liveConsumer);
        }
    }

    private Subscription startDelegatedSubscription(String subscriptionId, @Nullable SubscriptionFilter filter, boolean subscriptionsWasCancelledOrShutdown, StartAt startAtToUse, Consumer<CloudEvent> liveConsumer) {
        final Subscription subscription;
        if (subscriptionsWasCancelledOrShutdown) {
            // Priming startAtToUse is skipped for an explicit cancellation of this exact id, since its get() call
            // saves globalCheckpoint as a side effect, which would recreate the position cancelSubscription's own
            // deletePositionFromStorage call just deleted. A stop() or shutdown deletes nothing, so priming it for
            // those still leaves a resumable position for the next restart, same as before this id had per-attempt
            // identity.
            if (!wasCancelled()) {
                doIfCheckpointStorageConfigIs(UseCheckpointInStorage.class, cfg -> {
                    // Only get position if using storage and no position has been stored
                    if (!cfg.storage().exists(subscriptionId)) {
                        startAtToUse.get(generateSubscriptionModelContext());
                    }
                });
            }
            subscription = new CancelledSubscription(subscriptionId);
        } else {
            subscription = getWrappedSubscriptionModel().subscribe(subscriptionId, withCapabilityScope(filter), startAtToUse, liveConsumer);
            applyPendingPauseIfAny(subscriptionId);
        }
        return subscription;
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Stream position mode: replay historic stream events by position and resume by position, reading through
    // PositionOrderedReader instead of the legacy time-ordered path above.
    // ---------------------------------------------------------------------------------------------------------------

    private Subscription startPositionCatchupSubscriptionForStream(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, StartAt firstStartAt) {
        PositionOrderedReader positionOrderedReader = (PositionOrderedReader) eventStoreQueries;
        Filter streamFilter = withCapabilityScope(plainFilterOf(filter));
        long windowSize = config.dcbCatchupPositionWindowSize;

        StartAt nextStartAt = firstStartAt.get(generateSubscriptionModelContext());
        Checkpoint checkpoint = ((StartAtCheckpoint) Objects.requireNonNull(nextStartAt)).checkpoint;
        long startPosition = GlobalCheckpoint.positionOf(checkpoint);

        // Capture the live resume token before the bulk replay so an event committed during the replay is still
        // delivered live, like the DCB handover. Fails loudly instead of falling back to "now" when the delegate
        // reports no resume token (captureLiveResumeCheckpoint).
        Class<? extends SubscriptionModel> delegatedSubscriptionModelType = getWrappedSubscriptionModel().getClass();
        StartAt delegatedStartAt = startAt.get(new SubscriptionModelContext(delegatedSubscriptionModelType));
        final Checkpoint globalCheckpoint = captureLiveResumeCheckpoint(delegatedStartAt);

        // Page through the position sequence from the resume position to the head seen at the start, in windows so a
        // large rebuild does not load the whole matched set at once, then reconcile until the head stops advancing.
        // Re-reads of overlapping windows are deduped by the cache (delivery is at-least-once).
        BoundedIdCache catchupPhaseCache = new BoundedIdCache(config.cacheSize);
        PositionCatchupPipeline.Reader streamReader = new PositionCatchupPipeline.Reader() {
            @Override
            public long currentHead() {
                return positionOrderedReader.currentPosition();
            }

            @Override
            public Stream<CloudEvent> readWindow(long fromExclusive, long toInclusive) {
                return positionOrderedReader.readInPositionOrder(streamFilter, PositionRange.between(fromExclusive, toInclusive));
            }
        };
        PositionCatchupPipeline pipeline = new PositionCatchupPipeline(streamReader, windowSize);
        pipeline.replay(startPosition, () -> shouldKeepReplaying(subscriptionId),
                (events, cache) -> deliverCatchupEvents(events, subscriptionId, action, cache, e -> GlobalCheckpoint.of(OccurrentCloudEventExtension.getPosition(e))),
                catchupPhaseCache, () -> historyRead(subscriptionId));

        // Locked from the identity decision through the delegate subscribe call below, same reasoning as the
        // time-based path above. An unlocked gap here is observable two ways, a lost cancellation, or a fresh
        // attempt's checkpoint save wiped out by this attempt's late delete.
        try (HandoverLock ignored = lockHandover(subscriptionId)) {
            // See the time-based path's identical reasoning for why this is the atomic decision itself.
            final boolean subscriptionsWasCancelledOrShutdown = !endReplayIfStillCurrent(subscriptionId);

            // Gated on the atomic decision above, not just checked ahead of it, for the same reason as the time-based
            // path: a superseded attempt reaching this late must not delete a later attempt's own temporary position.
            if (delegatedStartAt == null && !subscriptionsWasCancelledOrShutdown) {
                returnIfCheckpointStorageConfigIs(UseCheckpointInStorage.class, cfg -> {
                    cfg.storage().delete(subscriptionId);
                    return null;
                });
            }

            StartAt startAtToUse = StartAt.dynamic(this.<Supplier<StartAt>, UseCheckpointInStorage>returnIfCheckpointStorageConfigIs(UseCheckpointInStorage.class,
                            cfg -> () -> {
                                Checkpoint position = cfg.storage().read(subscriptionId);
                                // If nothing is stored, or the stored position is a global position (written by this
                                // catch-up), save the live change-stream position so the wrapped subscription resumes
                                // from there.
                                if ((position == null || GlobalCheckpoint.isGlobalCheckpoint(position)) && globalCheckpoint != null) {
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

            Consumer<CloudEvent> liveConsumer = cloudEvent -> {
                if (!catchupPhaseCache.contains(cloudEvent.getId())) {
                    action.accept(cloudEvent);
                }
            };
            return startDelegatedSubscription(subscriptionId, filter, subscriptionsWasCancelledOrShutdown, startAtToUse, liveConsumer);
        }
    }

    private Filter deriveFilterToUseDuringCatchupPhase(@Nullable SubscriptionFilter filter, Checkpoint checkpoint) {
        final Filter timeFilter;
        if (isBeginningOfTime(checkpoint)) {
            timeFilter = Filter.all();
        } else {
            OffsetDateTime offsetDateTime = OffsetDateTime.parse(checkpoint.asString(), RFC_3339_DATE_TIME_FORMATTER);
            timeFilter = time(gt(offsetDateTime));
        }

        final Filter catchupFilter;
        if (filter == null) {
            catchupFilter = timeFilter;
        } else {
            Filter userSuppliedFilter = plainFilterOf(filter);
            catchupFilter = timeFilter.and(userSuppliedFilter);
        }
        return withCapabilityScope(catchupFilter);
    }

    // Unwraps the plain Filter from the (possibly null) subscription filter, accepting both the stream marker
    // (StreamSubscriptionFilter) and the capability-agnostic marker (AgnosticSubscriptionFilter). A null filter means
    // "no constraint", i.e. Filter.all().
    private static Filter plainFilterOf(@Nullable SubscriptionFilter filter) {
        if (filter == null) {
            return Filter.all();
        } else if (filter instanceof StreamSubscriptionFilter streamSubscriptionFilter) {
            return streamSubscriptionFilter.filter();
        } else if (filter instanceof AgnosticSubscriptionFilter agnosticSubscriptionFilter) {
            return agnosticSubscriptionFilter.filter();
        }
        throw new UnsupportedSubscriptionFilterException(filter.getClass(), "Only StreamSubscriptionFilter or AgnosticSubscriptionFilter is supported!");
    }

    // ANDs the capability scope onto the caller's filter, so a stream subscription on a store that also has the DCB
    // capability never returns a DCB-tagged event. When the scope is null (a capability-agnostic subscription) the
    // caller's filter is returned unchanged, so events of every capability are delivered. Since Filter.all() means "no
    // constraint", ANDing the scope onto it is exactly the scope filter alone.
    private Filter withCapabilityScope(Filter filter) {
        if (capabilityScope == null) {
            return filter;
        }
        return filter instanceof Filter.All ? capabilityScope : filter.and(capabilityScope);
    }

    // Wraps the caller's (possibly null) subscription filter into a capability-scoped StreamSubscriptionFilter to
    // hand to the delegated live subscription, so live delivery after handover applies the same capability scope as the
    // replay. It always produces a StreamSubscriptionFilter (never the agnostic marker) because the live subscription
    // models only understand StreamSubscriptionFilter and DcbSubscriptionFilter.
    private StreamSubscriptionFilter withCapabilityScope(@Nullable SubscriptionFilter filter) {
        return StreamSubscriptionFilter.filter(withCapabilityScope(plainFilterOf(filter)));
    }

    private void runCatchupForStream(Stream<CloudEvent> cloudEvents, String subscriptionId, Consumer<CloudEvent> action, @Nullable BoundedIdCache cache) {
        deliverCatchupEvents(cloudEvents, subscriptionId, action, cache, e -> TimeBasedCheckpoint.from(e.getTime()));
    }

    /**
     * Delivers catch-up events to {@code action}, optionally deduping against {@code cache}, and persists the
     * subscription position for events matching the catch-up persist predicate. The position to persist is derived per
     * event by {@code positionToPersist}, which differs between the time-based path (time based) and the position path
     * (global position).
     */
    private void deliverCatchupEvents(Stream<CloudEvent> cloudEvents, String subscriptionId, Consumer<CloudEvent> action, @Nullable BoundedIdCache cache, Function<CloudEvent, Checkpoint> positionToPersist) {
        // try-with-resources closes the source stream even when takeWhile short-circuits on shutdown, so a
        // resource-backed read (the Spring Mongo bulk replay wraps a server cursor) does not leak its cursor.
        try (cloudEvents) {
            Stream<CloudEvent> takeWhile = cloudEvents.takeWhile(__ -> shouldKeepReplaying(subscriptionId));
            if (cache != null) {
                // Skip events already delivered in an earlier reconciliation pass (the delta is re-read until it
                // stabilises, so passes overlap) and record the rest so the live subscription can skip them at the
                // handover seam. Without the filter the overlapping re-reads would deliver duplicates.
                takeWhile = takeWhile.filter(e -> !cache.contains(e.getId())).peek(e -> cache.add(e.getId()));
            }
            takeWhile
                    .peek(action)
                    // Rechecked here, not just by takeWhile before action ran: action is caller code and can take
                    // long enough for this attempt to be superseded while it runs, and persisting a stale attempt's
                    // position after that would regress a newer attempt's already-more-advanced one, since the
                    // default write condition is any(). Identity only, not shouldKeepReplaying: a stop or shutdown
                    // this same event's action triggered must not suppress persisting the position it just reached.
                    .filter(e -> isSafeToPersistFor(subscriptionId))
                    .filter(returnIfCheckpointStorageConfigIs(PersistCheckpointDuringCatchupPhase.class, PersistCheckpointDuringCatchupPhase::persistCloudEventPositionPredicate).orElse(__ -> false))
                    .forEach(e -> doIfCheckpointStorageConfigIs(PersistCheckpointDuringCatchupPhase.class, cfg -> cfg.storage().save(subscriptionId, positionToPersist.apply(e), writeConditionFor(cfg, subscriptionId))));
        }
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

    static boolean isTimeBasedCheckpoint(StartAt startAt, Class<?> contextType) {
        StartAt start = startAt.get(new SubscriptionModelContext(contextType));
        if (!(start instanceof StartAtCheckpoint position)) {
            return false;
        }

        Checkpoint checkpoint = position.checkpoint;
        return isTimeBasedCheckpoint(checkpoint);
    }

    static boolean isTimeBasedCheckpoint(Checkpoint checkpoint) {
        return checkpoint instanceof TimeBasedCheckpoint ||
                (checkpoint instanceof StringBasedCheckpoint && isRfc3339Timestamp(checkpoint.asString()));
    }

    private static boolean isRfc3339Timestamp(String string) {
        try {
            OffsetDateTime.parse(string, RFC_3339_DATE_TIME_FORMATTER);
            return true;
        } catch (Exception exception) {
            return false;
        }
    }

    private static boolean isBeginningOfTime(Checkpoint checkpoint) {
        return checkpoint instanceof TimeBasedCheckpoint && ((TimeBasedCheckpoint) checkpoint).isBeginningOfTime();
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", StreamCatchupSubscriptionModel.class.getSimpleName() + "[", "]")
                .add("subscriptionModel=" + subscriptionModel)
                .add("eventStoreQueries=" + eventStoreQueries)
                .add("config=" + config)
                .add("runningCatchupSubscriptions=" + runningCatchupSubscriptions)
                .add("shuttingDown=" + shuttingDown)
                .toString();
    }
}
