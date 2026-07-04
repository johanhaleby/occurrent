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
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.GlobalSubscriptionPosition;
import org.occurrent.subscription.OccurrentSubscriptionFilter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.StartAtSubscriptionPosition;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StringBasedSubscriptionPosition;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionPosition;
import org.occurrent.subscription.api.blocking.DelegatingSubscriptionModel;
import org.occurrent.subscription.api.blocking.PositionAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.SubscriptionPositionStorageConfig.PersistSubscriptionPositionDuringCatchupPhase;
import org.occurrent.subscription.blocking.durable.catchup.SubscriptionPositionStorageConfig.UseSubscriptionPositionInStorage;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
 * counterpart split out of {@code CatchupSubscriptionModel} (see ADR 25 / Wave 2b) so a stream-only application does
 * not need to depend on {@code eventstore-api-dcb}. The dispatcher {@code CatchupSubscriptionModel} in the
 * {@code dcb-catchup-subscription} module wraps this class for its stream routing.
 * <p>
 * Delivery is at-least-once, with the same catch-up-to-live handover and clock-skew-safe reconciliation guarantees
 * documented on the dispatcher.
 */
@NullMarked
public class StreamCatchupSubscriptionModel implements SubscriptionModel, DelegatingSubscriptionModel {

    private final PositionAwareSubscriptionModel subscriptionModel;
    private final EventStoreQueries eventStoreQueries;
    private final CatchupSubscriptionModelConfig config;
    private final Class<?> subscriptionModelContextType;
    private final ConcurrentMap<String, Boolean> runningCatchupSubscriptions = new ConcurrentHashMap<>();
    private volatile boolean shuttingDown = false;

    public StreamCatchupSubscriptionModel(PositionAwareSubscriptionModel subscriptionModel, EventStoreQueries eventStoreQueries, CatchupSubscriptionModelConfig config) {
        this(subscriptionModel, eventStoreQueries, config, StreamCatchupSubscriptionModel.class);
    }

    /**
     * @param subscriptionModelContextType The class a caller-supplied {@code StartAt.dynamic} sees as
     *                                      {@code SubscriptionModelContext#subscriptionModelType()} when it is first
     *                                      resolved. The {@code CatchupSubscriptionModel} dispatcher passes its own
     *                                      class here so a caller that pattern-matches on the public dispatcher type
     *                                      keeps working regardless of which mode-specific class runs the catch-up.
     */
    public StreamCatchupSubscriptionModel(PositionAwareSubscriptionModel subscriptionModel, EventStoreQueries eventStoreQueries, CatchupSubscriptionModelConfig config, Class<?> subscriptionModelContextType) {
        this.subscriptionModel = Objects.requireNonNull(subscriptionModel, "subscriptionModel cannot be null");
        this.eventStoreQueries = Objects.requireNonNull(eventStoreQueries, "eventStoreQueries cannot be null");
        this.config = Objects.requireNonNull(config, "config cannot be null");
        this.subscriptionModelContextType = Objects.requireNonNull(subscriptionModelContextType, "subscriptionModelContextType cannot be null");
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
        // Stream catch-up converts the filter into an Occurrent Filter for the historical query, so only the stream
        // filter type is supported here. The DCB path accepts a DcbSubscriptionFilter and passes it to its own model.
        if (filter != null && !(filter instanceof OccurrentSubscriptionFilter)) {
            throw new IllegalArgumentException("Only OccurrentSubscriptionFilter is supported!");
        }
        boolean positionMode = streamStoreWritesPosition();
        final StartAt firstStartAt;
        if (startAt.isDefault()) {
            // By default, we check if there's a subscription position stored for this subscription, if so we resume from there, otherwise,
            // delegate to the parent subscription model.
            SubscriptionPosition subscriptionPosition = returnIfSubscriptionPositionStorageConfigIs(UseSubscriptionPositionInStorage.class, cfg -> cfg.storage().read(subscriptionId)).orElse(null);
            if (subscriptionPosition == null) {
                return getDelegatedSubscriptionModel().subscribe(subscriptionId, filter, startAt, action);
            } else if (positionMode && isTimeBasedSubscriptionPosition(subscriptionPosition)) {
                // The store now writes position, but this stored token predates that and is time-based. Reading it as a
                // position would misinterpret a timestamp or replay from an unrelated cursor, so re-resolve to the
                // model default instead.
                return getDelegatedSubscriptionModel().subscribe(subscriptionId, filter, StartAt.subscriptionModelDefault(), action);
            } else {
                firstStartAt = StartAt.subscriptionPosition(subscriptionPosition);
            }
        } else if (startAt.isDynamic()) {
            StartAt startAtGeneratedByDynamic = startAt.get(generateSubscriptionModelContext());
            if (startAtGeneratedByDynamic == null) {
                // We're not allowed to start this subscription model, defer to parent!
                return getDelegatedSubscriptionModel().subscribe(subscriptionId, filter, startAt, action);
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
                case BEGINNING_OF_TIME -> streamPositionCatchup(subscriptionId, filter, startAt, action, StartAt.subscriptionPosition(GlobalSubscriptionPosition.of(0)));
                case GLOBAL_POSITION -> streamPositionCatchup(subscriptionId, filter, startAt, action, firstStartAt);
                // A specific wall-clock time has no position to map to, so replay it through the legacy time-based
                // catch-up even on a position store.
                case SPECIFIC_TIME -> streamTimeCatchup(subscriptionId, filter, startAt, action, firstStartAt);
                case LIVE -> subscriptionModel.subscribe(subscriptionId, filter, firstStartAt, action);
            };
        }
        return switch (streamStart) {
            case BEGINNING_OF_TIME, SPECIFIC_TIME -> streamTimeCatchup(subscriptionId, filter, startAt, action, firstStartAt);
            case GLOBAL_POSITION, LIVE -> subscriptionModel.subscribe(subscriptionId, filter, firstStartAt, action);
        };
    }

    // The kinds of resolved start a stream subscription can have. Classifying once keeps the routing above an
    // exhaustive switch, so a new kind forces both switches to handle it and no start can silently fall through to
    // live delivery, which is the class of bug that once dropped specific-time replay on a position store.
    private enum StreamStart {BEGINNING_OF_TIME, SPECIFIC_TIME, GLOBAL_POSITION, LIVE}

    private static StreamStart classifyStreamStart(StartAt startAt, Class<?> contextType) {
        if (startsAtBeginningOfTime(startAt, contextType)) {
            return StreamStart.BEGINNING_OF_TIME;
        }
        if (isTimeBasedSubscriptionPosition(startAt, contextType)) {
            return StreamStart.SPECIFIC_TIME;
        }
        if (startsAtExplicitGlobalPosition(startAt, contextType)) {
            return StreamStart.GLOBAL_POSITION;
        }
        return StreamStart.LIVE;
    }

    /**
     * Whether a resolved {@code startAt} is an explicit {@link GlobalSubscriptionPosition}. Shared with the dispatcher's
     * routing, which needs the same check before any mode-specific class is even chosen; {@code contextType} is the
     * class the dispatcher (or this class, standalone) reports to a caller-supplied {@code StartAt.dynamic}.
     */
    public static boolean startsAtExplicitGlobalPosition(StartAt startAt, Class<?> contextType) {
        StartAt start = startAt.get(new SubscriptionModelContext(contextType));
        return start instanceof StartAtSubscriptionPosition position
                && GlobalSubscriptionPosition.isGlobalSubscriptionPosition(position.subscriptionPosition);
    }

    private Subscription streamPositionCatchup(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, StartAt positionStartAt) {
        Future<Subscription> future = CompletableFuture.supplyAsync(() -> startPositionCatchupSubscriptionForStream(subscriptionId, filter, startAt, action, positionStartAt));
        return new CatchupSubscription(subscriptionId, future);
    }

    private Subscription streamTimeCatchup(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, StartAt firstStartAt) {
        Future<Subscription> future = CompletableFuture.supplyAsync(() -> startCatchupSubscription(subscriptionId, filter, startAt, action, firstStartAt));
        return new CatchupSubscription(subscriptionId, future);
    }

    private Subscription startCatchupSubscription(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, StartAt firstStartAt) {
        runningCatchupSubscriptions.put(subscriptionId, true);

        StartAt nextStartAt = firstStartAt.get(generateSubscriptionModelContext());
        SubscriptionPosition subscriptionPosition = ((StartAtSubscriptionPosition) Objects.requireNonNull(nextStartAt)).subscriptionPosition;

        Filter catchupFilter = deriveFilterToUseDuringCatchupPhase(filter, subscriptionPosition);

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
        final SubscriptionPosition globalSubscriptionPosition;
        if (delegatedStartAt == null) {
            // The delegated subscription model is not allowed to subscribe, so we don't need to get the global position.
            globalSubscriptionPosition = null;
        } else {
            // We force the wrapping subscription to be a PositionAwareSubscriptionModel so that we can capture
            // where the live subscription should resume. This position is captured *after* the bulk replay so it
            // stays fresh: capturing it before a long replay would risk the resume token ageing out of the
            // database change stream (e.g. MongoDB's oplog) before the handover, making the live subscription
            // unresumable. Events written during the replay are not covered by this position (they were written
            // before it). They are reconciled separately by the insertion-order delta below.
            globalSubscriptionPosition = subscriptionModel.globalSubscriptionPosition();
        }

        // We generate a cache so that events that are streamed at the same time as streaming the events missed
        // during the catch-up phase are not streamed again.
        FixedSizeCache catchupPhaseCache = new FixedSizeCache(config.cacheSize);

        // Reconcile events that arrived during the catch-up phase, i.e. those written after the bulk replay started
        // but at or before the live subscription's resume position (globalSubscriptionPosition). They are, by
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
        // event out of the read; being at or before globalSubscriptionPosition it would not be re-delivered by the live
        // subscription either, and would be lost. To close that window we re-read until the matching count stops
        // growing: each pass reads every event after the pre-catch-up boundary, so a pass during which no new event is
        // written has necessarily delivered them all. Re-reads re-deliver already-seen events, which are deduped by the
        // cache (delivery is at-least-once). Any event written after a pass is, by definition, newer than
        // globalSubscriptionPosition and is therefore covered by the live subscription regardless.
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
            returnIfSubscriptionPositionStorageConfigIs(UseSubscriptionPositionInStorage.class, cfg -> {
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
        // which is not what we want! The reason for doing this with UseSubscriptionPositionInStorage (as opposed to just
        // PersistSubscriptionPositionDuringCatchupPhase) is that if using a "storage" at all in the config, is to accommodate
        // that the wrapping subscription continues from where we left off.
        StartAt startAtToUse = StartAt.dynamic(this.<Supplier<StartAt>, UseSubscriptionPositionInStorage>returnIfSubscriptionPositionStorageConfigIs(UseSubscriptionPositionInStorage.class,
                        cfg -> () -> {
                            // It's important that we find the document inside the supplier so that we look up the latest resume token on retry
                            SubscriptionPosition position = cfg.storage().read(subscriptionId);
                            // If there is no position stored in storage, or if the stored position is time-based
                            // (i.e. written by the catch-up subscription), we save the globalSubscriptionPosition.
                            // The reason that we need to write the time-based subscription position in this case
                            // is that the wrapped subscription might not support time-based subscriptions.
                            if ((position == null || isTimeBasedSubscriptionPosition(position)) && globalSubscriptionPosition != null) {
                                position = cfg.storage().save(subscriptionId, globalSubscriptionPosition);
                            } else if (position == null) {
                                // Position can still be null here if globalSubscriptionPosition is null, if so, we start at the "subscriptionModelDefault",
                                // given that the delegated subscription model is allowed to subscribe (i.e. delegatedStartAt != null).
                                return delegatedStartAt == null ? startAt : StartAt.subscriptionModelDefault();
                            }
                            return StartAt.subscriptionPosition(position);
                        })
                .orElse(() -> {
                    if (globalSubscriptionPosition == null) {
                        // We check if the delegated subscription model is allowed to subscribe (delegatedStartAt != null),
                        // if so we instruct the subscription model to start from default, otherwise just return the original
                        // startAt supplied by the user.
                        return delegatedStartAt == null ? startAt : StartAt.subscriptionModelDefault();
                    } else {
                        return StartAt.subscriptionPosition(globalSubscriptionPosition);
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
            doIfSubscriptionPositionStorageConfigIs(UseSubscriptionPositionInStorage.class, cfg -> {
                // Only get position if using storage and no position has been stored!
                if (!cfg.storage().exists(subscriptionId)) {
                    startAtToUse.get(generateSubscriptionModelContext());
                }
            });
            subscription = new CancelledSubscription(subscriptionId);
        } else {
            subscription = getDelegatedSubscriptionModel().subscribe(subscriptionId, filter, startAtToUse, liveConsumer);
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
        Filter streamFilter = filter == null ? Filter.all() : ((OccurrentSubscriptionFilter) filter).filter();
        long windowSize = config.dcbCatchupPositionWindowSize;

        StartAt nextStartAt = firstStartAt.get(generateSubscriptionModelContext());
        SubscriptionPosition subscriptionPosition = ((StartAtSubscriptionPosition) Objects.requireNonNull(nextStartAt)).subscriptionPosition;
        long startPosition = GlobalSubscriptionPosition.positionOf(subscriptionPosition);

        // Capture the live resume token before the bulk replay so an event committed during the replay is still
        // delivered live, like the DCB handover.
        Class<? extends SubscriptionModel> delegatedSubscriptionModelType = getDelegatedSubscriptionModel().getClass();
        StartAt delegatedStartAt = startAt.get(new SubscriptionModelContext(delegatedSubscriptionModelType));
        final SubscriptionPosition globalSubscriptionPosition = delegatedStartAt == null ? null : subscriptionModel.globalSubscriptionPosition();

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
            returnIfSubscriptionPositionStorageConfigIs(UseSubscriptionPositionInStorage.class, cfg -> {
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

        StartAt startAtToUse = StartAt.dynamic(this.<Supplier<StartAt>, UseSubscriptionPositionInStorage>returnIfSubscriptionPositionStorageConfigIs(UseSubscriptionPositionInStorage.class,
                        cfg -> () -> {
                            SubscriptionPosition position = cfg.storage().read(subscriptionId);
                            // If nothing is stored, or the stored position is a global position (written by this
                            // catch-up), save the live change-stream position so the wrapped subscription resumes
                            // from there.
                            if ((position == null || GlobalSubscriptionPosition.isGlobalSubscriptionPosition(position)) && globalSubscriptionPosition != null) {
                                position = cfg.storage().save(subscriptionId, globalSubscriptionPosition);
                            } else if (position == null) {
                                return delegatedStartAt == null ? startAt : StartAt.subscriptionModelDefault();
                            }
                            return StartAt.subscriptionPosition(position);
                        })
                .orElse(() -> {
                    if (globalSubscriptionPosition == null) {
                        return delegatedStartAt == null ? startAt : StartAt.subscriptionModelDefault();
                    } else {
                        return StartAt.subscriptionPosition(globalSubscriptionPosition);
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
            deliverCatchupEvents(slice, subscriptionId, action, cache, e -> GlobalSubscriptionPosition.of(OccurrentCloudEventExtension.getPosition(e)));
            cursor = upTo;
        }
        return cursor;
    }

    private static Filter deriveFilterToUseDuringCatchupPhase(@Nullable SubscriptionFilter filter, SubscriptionPosition subscriptionPosition) {
        final Filter timeFilter;
        if (isBeginningOfTime(subscriptionPosition)) {
            timeFilter = Filter.all();
        } else {
            OffsetDateTime offsetDateTime = OffsetDateTime.parse(subscriptionPosition.asString(), RFC_3339_DATE_TIME_FORMATTER);
            timeFilter = time(gt(offsetDateTime));
        }

        final Filter catchupFilter;
        if (filter == null) {
            catchupFilter = timeFilter;
        } else {
            Filter userSuppliedFilter = ((OccurrentSubscriptionFilter) filter).filter();
            catchupFilter = timeFilter.and(userSuppliedFilter);
        }
        return catchupFilter;
    }

    private void runCatchupForStream(Stream<CloudEvent> cloudEvents, String subscriptionId, Consumer<CloudEvent> action, @Nullable FixedSizeCache cache) {
        deliverCatchupEvents(cloudEvents, subscriptionId, action, cache, e -> TimeBasedSubscriptionPosition.from(e.getTime()));
    }

    /**
     * Delivers catch-up events to {@code action}, optionally deduping against {@code cache}, and persists the
     * subscription position for events matching the catch-up persist predicate. The position to persist is derived per
     * event by {@code positionToPersist}, which differs between the time-based path (time based) and the position path
     * (global position).
     */
    private void deliverCatchupEvents(Stream<CloudEvent> cloudEvents, String subscriptionId, Consumer<CloudEvent> action, @Nullable FixedSizeCache cache, Function<CloudEvent, SubscriptionPosition> positionToPersist) {
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
                    .filter(returnIfSubscriptionPositionStorageConfigIs(PersistSubscriptionPositionDuringCatchupPhase.class, PersistSubscriptionPositionDuringCatchupPhase::persistCloudEventPositionPredicate).orElse(__ -> false))
                    .forEach(e -> doIfSubscriptionPositionStorageConfigIs(PersistSubscriptionPositionDuringCatchupPhase.class, cfg -> cfg.storage().save(subscriptionId, positionToPersist.apply(e))));
        }
    }

    // Reports subscriptionModelContextType (CatchupSubscriptionModel when wrapped by the dispatcher) so a
    // StartAt.dynamic supplied by a caller that pattern matches on the public dispatcher type keeps working
    // regardless of which mode-specific class ends up running the catch-up underneath it.
    SubscriptionModelContext generateSubscriptionModelContext() {
        return new SubscriptionModelContext(subscriptionModelContextType);
    }

    @Override
    public void stop() {
        getDelegatedSubscriptionModel().stop();
    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        getDelegatedSubscriptionModel().start(resumeSubscriptionsAutomatically);
    }

    @Override
    public boolean isRunning() {
        return getDelegatedSubscriptionModel().isRunning();
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return getDelegatedSubscriptionModel().isRunning(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return getDelegatedSubscriptionModel().isPaused(subscriptionId);
    }

    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        return getDelegatedSubscriptionModel().resumeSubscription(subscriptionId);
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
        getDelegatedSubscriptionModel().pauseSubscription(subscriptionId);
    }

    /**
     * Cancel a stream catch-up running for {@code subscriptionId}. A no-op if this class has no catch-up running for
     * that id (for example because it belongs to the DCB path in a dual-mode dispatcher). Does not touch the shared
     * live delegate or position storage; the dispatcher owns those since both paths share the same delegate.
     */
    public void cancelRunningCatchup(String subscriptionId) {
        runningCatchupSubscriptions.remove(subscriptionId);
    }

    /**
     * Mark this model as shutting down so any in-flight catch-up stops as soon as possible. Does not touch the shared
     * live delegate; the dispatcher owns that.
     */
    public void markShuttingDown() {
        shuttingDown = true;
        runningCatchupSubscriptions.clear();
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        cancelRunningCatchup(subscriptionId);
        subscriptionModel.cancelSubscription(subscriptionId);
        deletePositionFromStorage(subscriptionId);
    }

    /**
     * Delete {@code subscriptionId}'s position from the configured position storage, if any. Exposed so the
     * dispatcher can delete it exactly once when cancelling a subscription that could belong to either mode, since
     * the position storage config (and the storage instance it wraps) is shared, not owned per mode.
     */
    public void deletePositionFromStorage(String subscriptionId) {
        doIfSubscriptionPositionStorageConfigIs(UseSubscriptionPositionInStorage.class, cfg -> cfg.storage().delete(subscriptionId));
    }

    @Override
    public void shutdown() {
        markShuttingDown();
        subscriptionModel.shutdown();
    }

    public static boolean isTimeBasedSubscriptionPosition(StartAt startAt, Class<?> contextType) {
        StartAt start = startAt.get(new SubscriptionModelContext(contextType));
        if (!(start instanceof StartAtSubscriptionPosition)) {
            return false;
        }

        SubscriptionPosition subscriptionPosition = ((StartAtSubscriptionPosition) start).subscriptionPosition;
        return isTimeBasedSubscriptionPosition(subscriptionPosition);
    }

    public static boolean isTimeBasedSubscriptionPosition(SubscriptionPosition subscriptionPosition) {
        return subscriptionPosition instanceof TimeBasedSubscriptionPosition ||
                (subscriptionPosition instanceof StringBasedSubscriptionPosition && isRfc3339Timestamp(subscriptionPosition.asString()));
    }

    private static boolean isRfc3339Timestamp(String string) {
        try {
            OffsetDateTime.parse(string, RFC_3339_DATE_TIME_FORMATTER);
            return true;
        } catch (Exception exception) {
            return false;
        }
    }

    private static boolean isBeginningOfTime(SubscriptionPosition subscriptionPosition) {
        return subscriptionPosition instanceof TimeBasedSubscriptionPosition && ((TimeBasedSubscriptionPosition) subscriptionPosition).isBeginningOfTime();
    }

    // Whether a resolved StartAt is a "replay from the beginning of time" request. On a position store this maps to a
    // position-from-beginning start.
    private static boolean startsAtBeginningOfTime(StartAt startAt, Class<?> contextType) {
        StartAt start = startAt.get(new SubscriptionModelContext(contextType));
        return start instanceof StartAtSubscriptionPosition position && isBeginningOfTime(position.subscriptionPosition);
    }

    @Override
    public SubscriptionModel getDelegatedSubscriptionModel() {
        return subscriptionModel;
    }

    private <T, C extends SubscriptionPositionStorageConfig> Optional<T> returnIfSubscriptionPositionStorageConfigIs(Class<C> cls, Function<C, @Nullable T> fn) {
        if (cls.isInstance(config.subscriptionStorageConfig)) {
            return Optional.ofNullable(fn.apply(cls.cast(config.subscriptionStorageConfig)));
        }
        return Optional.empty();
    }

    private <C extends SubscriptionPositionStorageConfig> void doIfSubscriptionPositionStorageConfigIs(Class<C> cls, Consumer<C> consumer) {
        if (cls.isInstance(config.subscriptionStorageConfig)) {
            consumer.accept(cls.cast(config.subscriptionStorageConfig));
        }
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
