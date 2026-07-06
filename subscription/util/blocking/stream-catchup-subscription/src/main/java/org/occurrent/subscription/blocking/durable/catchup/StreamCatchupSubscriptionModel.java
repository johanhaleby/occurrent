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
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.StartAtCheckpoint;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.PersistCheckpointDuringCatchupPhase;
import org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.UseCheckpointInStorage;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
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
 * otherwise by the legacy time/{@code $natural} order) then hands over to a live subscription. This is the stream
 * counterpart split out of {@code CatchupSubscriptionModel} (see ADR 25) so a stream-only application does
 * not need to depend on {@code eventstore-api-dcb}. The dispatcher {@code CatchupSubscriptionModel} in the
 * {@code catchup-subscription} module wraps this class for its stream routing.
 * <p>
 * This class only ever replays and delivers stream-capability events. On a store that has both the {@code STREAM} and
 * {@code DCB} capabilities enabled at once, that promise is enforced, not merely descriptive: a
 * {@link Filter#capability(EventStoreCapability) STREAM-capability filter} is ANDed into both the catch-up-phase reads
 * and the filter handed to the delegated live subscription, so a DCB-tagged event never reaches a subscriber of this
 * class in either phase (see ADR 50). A caller filter is still honored; the capability guard is composed on top of it.
 * <p>
 * Delivery is at-least-once, with the same catch-up-to-live handover and clock-skew-safe reconciliation guarantees
 * documented on the dispatcher.
 */
@NullMarked
public class StreamCatchupSubscriptionModel extends AbstractCatchupSubscriptionModel {

    // Guards every read and every live handover this class performs so a DCB-tagged event is never delivered to a
    // stream subscriber, even on a store that has both capabilities enabled (see ADR 50). It is store-agnostic: each
    // Filter-conversion implementation maps this capability to its own storage artifact.
    private static final Filter STREAM_CAPABILITY_FILTER = Filter.capability(EventStoreCapability.STREAM);

    private final EventStoreQueries eventStoreQueries;
    // The capability guard ANDed into every read and every live handover. It is {@link #STREAM_CAPABILITY_FILTER} for a
    // stream subscription (so a DCB-tagged event never reaches a stream subscriber, see ADR 50), and {@code null} for a
    // capability-agnostic subscription, which then filters only by the caller's plain Filter and so delivers events of
    // every capability.
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
    public StreamCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, EventStoreQueries eventStoreQueries, CatchupSubscriptionModelConfig config, Class<?> subscriptionModelContextType) {
        this(subscriptionModel, eventStoreQueries, config, subscriptionModelContextType, STREAM_CAPABILITY_FILTER);
    }

    /**
     * @param capabilityScope The capability {@link Filter} ANDed into every catch-up read and every live handover.
     *                        Pass {@link #STREAM_CAPABILITY_FILTER} for a stream subscription, or {@code null} for a
     *                        capability-agnostic subscription that delivers events of every capability, filtered only by
     *                        the caller's plain {@link Filter}.
     */
    public StreamCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, EventStoreQueries eventStoreQueries, CatchupSubscriptionModelConfig config, Class<?> subscriptionModelContextType, @Nullable Filter capabilityScope) {
        super(subscriptionModel, config, subscriptionModelContextType);
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
    public boolean streamStoreWritesPosition() {
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
            throw new IllegalArgumentException("Only StreamSubscriptionFilter or AgnosticSubscriptionFilter is supported!");
        }
        boolean positionMode = streamStoreWritesPosition();
        final StartAt firstStartAt;
        if (startAt.isDefault()) {
            // By default, we check if there's a subscription position stored for this subscription, if so we resume from there, otherwise,
            // delegate to the parent subscription model.
            Checkpoint checkpoint = returnIfCheckpointStorageConfigIs(UseCheckpointInStorage.class, cfg -> cfg.storage().read(subscriptionId)).orElse(null);
            if (checkpoint == null) {
                // Resumed straight to live without a catch-up phase, so scope the delegated subscription the same way
                // the handover would, keeping DCB events out.
                return getDelegatedSubscriptionModel().subscribe(subscriptionId, withCapabilityScope(filter), startAt, action);
            } else if (positionMode && isTimeBasedCheckpoint(checkpoint)) {
                // The store now writes position, but this stored token predates that and is time-based. Reading it as a
                // position would misinterpret a timestamp or replay from an unrelated cursor, so re-resolve to the
                // model default instead.
                return getDelegatedSubscriptionModel().subscribe(subscriptionId, withCapabilityScope(filter), StartAt.subscriptionModelDefault(), action);
            } else {
                firstStartAt = StartAt.checkpoint(checkpoint);
            }
        } else if (startAt.isDynamic()) {
            StartAt startAtGeneratedByDynamic = startAt.get(generateSubscriptionModelContext());
            if (startAtGeneratedByDynamic == null) {
                // We're not allowed to start this subscription model, defer to parent!
                return getDelegatedSubscriptionModel().subscribe(subscriptionId, withCapabilityScope(filter), startAt, action);
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
                case LIVE -> subscriptionModel.subscribe(subscriptionId, withCapabilityScope(filter), firstStartAt, action);
            };
        }
        return switch (streamStart) {
            case BEGINNING_OF_TIME, SPECIFIC_TIME -> streamTimeCatchup(subscriptionId, filter, startAt, action, firstStartAt);
            case GLOBAL_POSITION, LIVE -> subscriptionModel.subscribe(subscriptionId, withCapabilityScope(filter), firstStartAt, action);
        };
    }

    // The kinds of resolved start a stream subscription can have. Classifying once keeps the routing above an
    // exhaustive switch, so a new kind forces both switches to handle it and no start can silently fall through to
    // live delivery, which is the class of bug that once dropped specific-time replay on a position store.
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
    public static boolean startsAtExplicitGlobalPosition(StartAt startAt, Class<?> contextType) {
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
        runningCatchupSubscriptions.put(subscriptionId, true);

        StartAt nextStartAt = firstStartAt.get(generateSubscriptionModelContext());
        Checkpoint checkpoint = ((StartAtCheckpoint) Objects.requireNonNull(nextStartAt)).checkpoint;

        Filter catchupFilter = deriveFilterToUseDuringCatchupPhase(filter, checkpoint);

        long numberOfEventsBeforeStartingCatchupSubscription = eventStoreQueries.count(catchupFilter);

        // Perform the catchup
        runCatchupForStream(eventStoreQueries.query(catchupFilter, config.catchupPhaseSortBy), subscriptionId, action, null);

        // Here we check if the delegated subscription model is allowed to execute. The reason for doing this is that
        // in certain scenarios, such as when using the @Subscription annotation with settings {@code startAt=BEGINNING_OF_TIME} and
        // {@code resume=SAME_AS_START_AT}, we instruct the DurableSubscriptionModel (which is typically the delegated subscription model here)
        // to NOT store the position durably. This because we start at "beginning of time" and we also want to resume at
        // "beginning of time" and thus we never need to store ANY subscription position (because we always start from "beginning of time"
        // when application is rebooted). This allows for catching up in-memory projections/views/policies.
        Class<? extends SubscriptionModel> delegatedSubscriptionModelType = getDelegatedSubscriptionModel().getClass();
        StartAt delegatedStartAt = startAt.get(new SubscriptionModelContext(delegatedSubscriptionModelType));
        final Checkpoint globalCheckpoint;
        if (delegatedStartAt == null) {
            // The delegated subscription model is not allowed to subscribe, so we don't need to get the global position.
            globalCheckpoint = null;
        } else {
            // We force the wrapping subscription to be a CheckpointAwareSubscriptionModel so that we can capture
            // where the live subscription should resume. This position is captured *after* the bulk replay so it
            // stays fresh: capturing it before a long replay would risk the resume token ageing out of the
            // database change stream (e.g. MongoDB's oplog) before the handover, making the live subscription
            // unresumable. Events written during the replay are not covered by this position (they were written
            // before it). They are reconciled separately by the insertion-order delta below.
            globalCheckpoint = subscriptionModel.globalCheckpoint();
        }

        // We generate a cache so that events that are streamed at the same time as streaming the events missed
        // during the catch-up phase are not streamed again.
        FixedSizeCache catchupPhaseCache = new FixedSizeCache(config.cacheSize);

        // Reconcile events that arrived during the catch-up phase, i.e. those written after the bulk replay started
        // but at or before the live subscription's resume position (globalCheckpoint). They are, by
        // definition, the most-recently-inserted matching events, so we read the newest ones in *insertion order*
        // (SortBy.natural, descending + limit, no skip) and reverse them back to insertion order for delivery.
        //
        // Selecting by insertion order rather than the configurable, time-based catchupPhaseSortBy is what makes this
        // reconciliation loss-free under clock skew: a during-catch-up event whose "time" is earlier than the replay
        // cursor's already-passed position would otherwise sort before the already-processed boundary (missed here)
        // and sit below the live subscription's resume position (missed there too), and would be silently lost.
        // Reading the newest N in insertion order also reads only the recent tail instead of skipping the whole
        // backlog, which matters on large event stores.
        //
        // The number to read is derived from a count, but more events can be written between that count and the read.
        // Such a write inflates the store and shifts the "newest N" window forward, pushing the oldest during-catch-up
        // event out of the read; being at or before globalCheckpoint it would not be re-delivered by the live
        // subscription either, and would be lost. To close that window we re-read until the matching count stops
        // growing: each pass reads every event after the pre-catch-up boundary, so a pass during which no new event is
        // written has necessarily delivered them all. Re-reads re-deliver already-seen events, which are deduped by the
        // cache (delivery is at-least-once). Any event written after a pass is, by definition, newer than
        // globalCheckpoint and is therefore covered by the live subscription regardless.
        long reconciledThroughCount = numberOfEventsBeforeStartingCatchupSubscription;
        long matchingEventCount = eventStoreQueries.count(catchupFilter);
        while (matchingEventCount > reconciledThroughCount) {
            long numberOfEventsToReconcile = matchingEventCount - numberOfEventsBeforeStartingCatchupSubscription;
            List<CloudEvent> eventsWrittenDuringCatchup = new ArrayList<>(eventStoreQueries.query(catchupFilter, 0, Math.toIntExact(numberOfEventsToReconcile), SortBy.natural(DESCENDING)).toList());
            Collections.reverse(eventsWrittenDuringCatchup);
            runCatchupForStream(eventsWrittenDuringCatchup.stream(), subscriptionId, action, catchupPhaseCache);
            reconciledThroughCount = matchingEventCount;
            matchingEventCount = eventStoreQueries.count(catchupFilter);
        }

        // We check if the delegated subscription model is not allowed to subscribe. If so, we remove any temporary subscription position written during the catchup phase
        // since we're now done with the catch-up.
        if (delegatedStartAt == null) {
            returnIfCheckpointStorageConfigIs(UseCheckpointInStorage.class, cfg -> {
                cfg.storage().delete(subscriptionId);
                return null;
            });
        }

        final boolean subscriptionsWasCancelledOrShutdown;
        if (!shuttingDown && runningCatchupSubscriptions.containsKey(subscriptionId)) {
            subscriptionsWasCancelledOrShutdown = false;
            runningCatchupSubscriptions.remove(subscriptionId);
        } else {
            // When runningCatchupSubscriptions doesn't contain the key at this stage it means that it has been explicitly cancelled.
            subscriptionsWasCancelledOrShutdown = true;
        }

        // When the catch-up subscription is ready, we store the global position in the position storage so that subscriptions
        // that have not received _any_ new events during replay will start at the global position if the application is restarted.
        // Otherwise, nothing will be stored in the "storage" and replay of historic events will take place again on application restart
        // which is not what we want! The reason for doing this with UseCheckpointInStorage (as opposed to just
        // PersistCheckpointDuringCatchupPhase) is that if using a "storage" at all in the config, is to accommodate
        // that the wrapping subscription continues from where we left off.
        StartAt startAtToUse = StartAt.dynamic(this.<Supplier<StartAt>, UseCheckpointInStorage>returnIfCheckpointStorageConfigIs(UseCheckpointInStorage.class,
                        cfg -> () -> {
                            // It's important that we find the document inside the supplier so that we look up the latest resume token on retry
                            Checkpoint position = cfg.storage().read(subscriptionId);
                            // If there is no position stored in storage, or if the stored position is time-based
                            // (i.e. written by the catch-up subscription), we save the globalCheckpoint.
                            // The reason that we need to write the time-based subscription position in this case
                            // is that the wrapped subscription might not support time-based subscriptions.
                            if ((position == null || isTimeBasedCheckpoint(position)) && globalCheckpoint != null) {
                                position = cfg.storage().save(subscriptionId, globalCheckpoint);
                            } else if (position == null) {
                                // Position can still be null here if globalCheckpoint is null, if so, we start at the "subscriptionModelDefault",
                                // given that the delegated subscription model is allowed to subscribe (i.e. delegatedStartAt != null).
                                return delegatedStartAt == null ? startAt : StartAt.subscriptionModelDefault();
                            }
                            return StartAt.checkpoint(position);
                        })
                .orElse(() -> {
                    if (globalCheckpoint == null) {
                        // We check if the delegated subscription model is allowed to subscribe (delegatedStartAt != null),
                        // if so we instruct the subscription model to start from default, otherwise just return the original
                        // startAt supplied by the user.
                        return delegatedStartAt == null ? startAt : StartAt.subscriptionModelDefault();
                    } else {
                        return StartAt.checkpoint(globalCheckpoint);
                    }
                }));

        Consumer<CloudEvent> liveConsumer = cloudEvent -> {
            if (!catchupPhaseCache.isCached(cloudEvent.getId())) {
                action.accept(cloudEvent);
            }
        };
        return startDelegatedSubscription(subscriptionId, filter, subscriptionsWasCancelledOrShutdown, startAtToUse, liveConsumer);
    }

    private Subscription startDelegatedSubscription(String subscriptionId, @Nullable SubscriptionFilter filter, boolean subscriptionsWasCancelledOrShutdown, StartAt startAtToUse, Consumer<CloudEvent> liveConsumer) {
        final Subscription subscription;
        if (subscriptionsWasCancelledOrShutdown) {
            doIfCheckpointStorageConfigIs(UseCheckpointInStorage.class, cfg -> {
                // Only get position if using storage and no position has been stored!
                if (!cfg.storage().exists(subscriptionId)) {
                    startAtToUse.get(generateSubscriptionModelContext());
                }
            });
            subscription = new CancelledSubscription(subscriptionId);
        } else {
            subscription = getDelegatedSubscriptionModel().subscribe(subscriptionId, withCapabilityScope(filter), startAtToUse, liveConsumer);
        }
        return subscription;
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Stream position mode: replay historic stream events by position and resume by position, reading through
    // PositionOrderedReader instead of the legacy time-ordered path above.
    // ---------------------------------------------------------------------------------------------------------------

    private Subscription startPositionCatchupSubscriptionForStream(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, StartAt firstStartAt) {
        runningCatchupSubscriptions.put(subscriptionId, true);
        PositionOrderedReader positionOrderedReader = (PositionOrderedReader) eventStoreQueries;
        Filter streamFilter = withCapabilityScope(plainFilterOf(filter));
        long windowSize = config.dcbCatchupPositionWindowSize;

        StartAt nextStartAt = firstStartAt.get(generateSubscriptionModelContext());
        Checkpoint checkpoint = ((StartAtCheckpoint) Objects.requireNonNull(nextStartAt)).checkpoint;
        long startPosition = GlobalCheckpoint.positionOf(checkpoint);

        // Capture the live resume token before the bulk replay so an event committed during the replay is still
        // delivered live, like the DCB handover.
        Class<? extends SubscriptionModel> delegatedSubscriptionModelType = getDelegatedSubscriptionModel().getClass();
        StartAt delegatedStartAt = startAt.get(new SubscriptionModelContext(delegatedSubscriptionModelType));
        final Checkpoint globalCheckpoint = delegatedStartAt == null ? null : subscriptionModel.globalCheckpoint();

        // Page through the position sequence from the resume position to the head seen at the start, in windows so a
        // large rebuild does not load the whole matched set at once.
        long bulkHead = positionOrderedReader.currentPosition();
        long cursor = deliverPositionWindows(positionOrderedReader, streamFilter, startPosition, bulkHead, windowSize, subscriptionId, action, null);

        FixedSizeCache catchupPhaseCache = new FixedSizeCache(config.cacheSize);

        // Reconcile events written during the bulk replay (positions beyond bulkHead) by continuing to page until the
        // head stops advancing. Re-reads of overlapping windows are deduped by the cache (delivery is at-least-once).
        long head = positionOrderedReader.currentPosition();
        while (head > cursor && !shuttingDown && runningCatchupSubscriptions.containsKey(subscriptionId)) {
            cursor = deliverPositionWindows(positionOrderedReader, streamFilter, cursor, head, windowSize, subscriptionId, action, catchupPhaseCache);
            head = positionOrderedReader.currentPosition();
        }

        if (delegatedStartAt == null) {
            returnIfCheckpointStorageConfigIs(UseCheckpointInStorage.class, cfg -> {
                cfg.storage().delete(subscriptionId);
                return null;
            });
        }

        final boolean subscriptionsWasCancelledOrShutdown;
        if (!shuttingDown && runningCatchupSubscriptions.containsKey(subscriptionId)) {
            subscriptionsWasCancelledOrShutdown = false;
            runningCatchupSubscriptions.remove(subscriptionId);
        } else {
            subscriptionsWasCancelledOrShutdown = true;
        }

        StartAt startAtToUse = StartAt.dynamic(this.<Supplier<StartAt>, UseCheckpointInStorage>returnIfCheckpointStorageConfigIs(UseCheckpointInStorage.class,
                        cfg -> () -> {
                            Checkpoint position = cfg.storage().read(subscriptionId);
                            // If nothing is stored, or the stored position is a global position (written by this
                            // catch-up), save the live change-stream position so the wrapped subscription resumes
                            // from there.
                            if ((position == null || GlobalCheckpoint.isGlobalCheckpoint(position)) && globalCheckpoint != null) {
                                position = cfg.storage().save(subscriptionId, globalCheckpoint);
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
            if (!catchupPhaseCache.isCached(cloudEvent.getId())) {
                action.accept(cloudEvent);
            }
        };
        return startDelegatedSubscription(subscriptionId, filter, subscriptionsWasCancelledOrShutdown, startAtToUse, liveConsumer);
    }

    /**
     * Delivers stream events in {@code (fromExclusive, toInclusive]} by paging through position windows, and returns
     * the position the cursor reached. Stops early on shutdown or cancellation.
     */
    private long deliverPositionWindows(PositionOrderedReader positionOrderedReader, Filter filter, long fromExclusive, long toInclusive, long windowSize, String subscriptionId, Consumer<CloudEvent> action, @Nullable FixedSizeCache cache) {
        long cursor = fromExclusive;
        while (cursor < toInclusive && !shuttingDown && runningCatchupSubscriptions.containsKey(subscriptionId)) {
            long upTo = Math.min(cursor + windowSize, toInclusive);
            Stream<CloudEvent> slice = positionOrderedReader.readInPositionOrder(filter, PositionRange.between(cursor, upTo));
            deliverCatchupEvents(slice, subscriptionId, action, cache, e -> GlobalCheckpoint.of(OccurrentCloudEventExtension.getPosition(e)));
            cursor = upTo;
        }
        return cursor;
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
        throw new IllegalArgumentException("Only StreamSubscriptionFilter or AgnosticSubscriptionFilter is supported!");
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

    private void runCatchupForStream(Stream<CloudEvent> cloudEvents, String subscriptionId, Consumer<CloudEvent> action, @Nullable FixedSizeCache cache) {
        deliverCatchupEvents(cloudEvents, subscriptionId, action, cache, e -> TimeBasedCheckpoint.from(e.getTime()));
    }

    /**
     * Delivers catch-up events to {@code action}, optionally deduping against {@code cache}, and persists the
     * subscription position for events matching the catch-up persist predicate. The position to persist is derived per
     * event by {@code positionToPersist}, which differs between the time-based path (time based) and the position path
     * (global position).
     */
    private void deliverCatchupEvents(Stream<CloudEvent> cloudEvents, String subscriptionId, Consumer<CloudEvent> action, @Nullable FixedSizeCache cache, Function<CloudEvent, Checkpoint> positionToPersist) {
        // try-with-resources closes the source stream even when takeWhile short-circuits on shutdown, so a
        // resource-backed read (the Spring Mongo bulk replay wraps a server cursor) does not leak its cursor.
        try (cloudEvents) {
            Stream<CloudEvent> takeWhile = cloudEvents.takeWhile(__ -> !shuttingDown && runningCatchupSubscriptions.containsKey(subscriptionId));
            if (cache != null) {
                // Skip events already delivered in an earlier reconciliation pass (the delta is re-read until it
                // stabilises, so passes overlap) and record the rest so the live subscription can skip them at the
                // handover seam. Without the filter the overlapping re-reads would deliver duplicates.
                takeWhile = takeWhile.filter(e -> !cache.isCached(e.getId())).peek(e -> cache.put(e.getId()));
            }
            takeWhile
                    .peek(action)
                    .filter(returnIfCheckpointStorageConfigIs(PersistCheckpointDuringCatchupPhase.class, PersistCheckpointDuringCatchupPhase::persistCloudEventPositionPredicate).orElse(__ -> false))
                    .forEach(e -> doIfCheckpointStorageConfigIs(PersistCheckpointDuringCatchupPhase.class, cfg -> cfg.storage().save(subscriptionId, positionToPersist.apply(e))));
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

    public static boolean isTimeBasedCheckpoint(StartAt startAt, Class<?> contextType) {
        StartAt start = startAt.get(new SubscriptionModelContext(contextType));
        if (!(start instanceof StartAtCheckpoint)) {
            return false;
        }

        Checkpoint checkpoint = ((StartAtCheckpoint) start).checkpoint;
        return isTimeBasedCheckpoint(checkpoint);
    }

    public static boolean isTimeBasedCheckpoint(Checkpoint checkpoint) {
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
