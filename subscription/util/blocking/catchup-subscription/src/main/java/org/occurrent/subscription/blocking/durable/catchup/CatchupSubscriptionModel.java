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
import jakarta.annotation.PreDestroy;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.*;
import org.occurrent.subscription.StartAt.StartAtSubscriptionPosition;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.api.blocking.*;
import org.occurrent.subscription.blocking.durable.catchup.SubscriptionPositionStorageConfig.PersistSubscriptionPositionDuringCatchupPhase;
import org.occurrent.subscription.blocking.durable.catchup.SubscriptionPositionStorageConfig.UseSubscriptionPositionInStorage;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.*;
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
 * A {@link SubscriptionModel} that can read historic cloud events from the all event streams (see {@link EventStoreQueries#all()}) until caught up with the
 * {@link PositionAwareSubscriptionModel#globalSubscriptionPosition()} of the {@code subscription} (you probably want to narrow the historic set events of events
 * by using a {@link Filter} when subscribing). It'll automatically switch over to the wrapped {@code subscription model} when all history events are read and the subscription has caught-up.
 * <br><b>Important:</b>&nbsp;The subscription model will only stream historic events if started with a {@link TimeBasedSubscriptionPosition}, by default (i.e. if {@code StartAt.subscriptionModelDefault() is used}),
 * it'll NOT replay historic events, but instead delegate to the wrapped subscription model. Thus, to start the {@link CatchupSubscriptionModel} and make it replay historic events you can start it like this:
 * <pre>
 * var subscriptionModel = new CatchupSubscriptionModel(..);
 * // All examples below are equivalent:
 * subscriptionModel.subscribeFromBeginningOfTime("subscriptionId", e -> System.out.println("Event: " + e);
 * subscriptionModel.subscribe("subscriptionId", StartAtTime.beginningOfTime(), e -> System.out.println("Event: " + e);
 * subscriptionModel.subscribe("subscriptionId", StartAt.subscriptionPosition(TimeBasedSubscription.beginningOfTime()), e -> System.out.println("Event: " + e);
 * </pre>
 * <p>
 * If you're using Kotlin you can import the extension functions from {@code org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelExtensions.kt} and do:
 * <pre>
 * subscriptionModel.subscribe("subscriptionId", StartAt.beginningOfTime()) { e ->
 *      println("Event: $e")
 * }
 * </pre>
 *
 * <p>
 * Note that the implementation uses an in-memory cache (default size is {@value #DEFAULT_CACHE_SIZE} but this can be configured using a {@link CatchupSubscriptionModelConfig})
 * to reduce the number of duplicate event when switching from historic events to the current cloud event position. It's highly recommended that the application logic is idempotent if the
 * cache size doesn't cover all duplicate events.
 * </p>
 * <br>
 * <p>
 * Delivery is at-least-once. Events written while the catch-up phase runs are reconciled and delivered, including events whose {@code time} is clock-skewed earlier than the replay cursor.
 * This reconciliation assumes the set of events matching the filter only grows while catching up, which holds for append-only stores. If events are deleted from the store while a catch-up
 * replay is running, the reconciliation can under-count and miss some events written during that replay. Avoid deleting events that match a running catch-up subscription's filter until it
 * has caught up.
 * </p>
 * <br>
 * <p>
 * Also note that the if a the subscription crashes during catch-up mode it'll continue where it left-off on restart, given the no specific `StartAt` position is supplied (i.e. if {@code StartAt.subscriptionModelDefault() is used}).
 * For this to work, the subscription must store the subscription position in a {@link SubscriptionPositionStorage} implementation periodically. It's possible to configure
 * how often this should happen in the {@link CatchupSubscriptionModelConfig}.
 * </p>
 */
@NullMarked
public class CatchupSubscriptionModel implements SubscriptionModel, DelegatingSubscriptionModel {

    private static final int DEFAULT_CACHE_SIZE = 100;

    private final PositionAwareSubscriptionModel subscriptionModel;
    private final @Nullable EventStoreQueries eventStoreQueries;
    private final @Nullable DcbEventStore dcbEventStore;
    private final @Nullable DcbQuery dcbQuery;
    private final CatchupSubscriptionModelConfig config;
    private final ConcurrentMap<String, Boolean> runningCatchupSubscriptions = new ConcurrentHashMap<>();
    private volatile boolean shuttingDown = false;

    /**
     * Create a new instance of {@link CatchupSubscriptionModel} the uses a default {@link CatchupSubscriptionModelConfig} with a cache size of
     * {@value #DEFAULT_CACHE_SIZE} but store the subscription position during the <i>catch-up</i> phase (i.e. if the application crashes or is shutdown during the
     * catch-up phase then the subscription will start from the beginning on application restart). After the catch-up phase has completed, the {@link PositionAwareSubscriptionModel}
     * will dictate how often the subscription position is stored.
     *
     * @param subscriptionModel The subscription that'll be used to subscribe to new events <i>after</i> catch-up is completed.
     * @param eventStoreQueries The API that will be used for catch-up
     */
    public CatchupSubscriptionModel(PositionAwareSubscriptionModel subscriptionModel, EventStoreQueries eventStoreQueries) {
        this(subscriptionModel, eventStoreQueries, new CatchupSubscriptionModelConfig(DEFAULT_CACHE_SIZE));
    }

    /**
     * Create a new instance of {@link CatchupSubscriptionModel} the uses the supplied {@link CatchupSubscriptionModelConfig}.
     * After catch-up mode has completed, the {@link PositionAwareSubscriptionModel} will dictate how often the subscription position is stored.
     *
     * @param subscriptionModel The subscription that'll be used to subscribe to new events <i>after</i> catch-up is completed.
     * @param eventStoreQueries The API that will be used for catch-up
     * @param config            The configuration to use
     */
    public CatchupSubscriptionModel(PositionAwareSubscriptionModel subscriptionModel, EventStoreQueries eventStoreQueries, CatchupSubscriptionModelConfig config) {
        this.subscriptionModel = Objects.requireNonNull(subscriptionModel, "subscriptionModel cannot be null");
        this.eventStoreQueries = Objects.requireNonNull(eventStoreQueries, "eventStoreQueries cannot be null");
        this.dcbEventStore = null;
        this.dcbQuery = null;
        this.config = Objects.requireNonNull(config, "config cannot be null");
    }

    /**
     * Create a new instance of {@link CatchupSubscriptionModel} in DCB mode using a default {@link CatchupSubscriptionModelConfig}.
     * In DCB mode the catch-up phase replays historic DCB events ordered by their {@code dcbposition} (rather than stream
     * events ordered by time), and the subscription resumes by {@code dcbposition}. Only events matching {@code dcbQuery}
     * are delivered, in both the replay and the live phase. See ADR 20.
     *
     * @param subscriptionModel The subscription that'll be used to subscribe to new events <i>after</i> catch-up is completed.
     * @param dcbEventStore     The DCB event store that will be used for the DCB catch-up replay.
     * @param dcbQuery          The DCB query that selects the events this subscription delivers.
     */
    public CatchupSubscriptionModel(PositionAwareSubscriptionModel subscriptionModel, DcbEventStore dcbEventStore, DcbQuery dcbQuery) {
        this(subscriptionModel, dcbEventStore, dcbQuery, new CatchupSubscriptionModelConfig(DEFAULT_CACHE_SIZE));
    }

    /**
     * Create a new instance of {@link CatchupSubscriptionModel} in DCB mode using the supplied {@link CatchupSubscriptionModelConfig}.
     * In DCB mode the catch-up phase replays historic DCB events ordered by their {@code dcbposition} (rather than stream
     * events ordered by time), and the subscription resumes by {@code dcbposition}. Only events matching {@code dcbQuery}
     * are delivered, in both the replay and the live phase. See ADR 20.
     *
     * @param subscriptionModel The subscription that'll be used to subscribe to new events <i>after</i> catch-up is completed.
     * @param dcbEventStore     The DCB event store that will be used for the DCB catch-up replay.
     * @param dcbQuery          The DCB query that selects the events this subscription delivers.
     * @param config            The configuration to use.
     */
    public CatchupSubscriptionModel(PositionAwareSubscriptionModel subscriptionModel, DcbEventStore dcbEventStore, DcbQuery dcbQuery, CatchupSubscriptionModelConfig config) {
        this.subscriptionModel = Objects.requireNonNull(subscriptionModel, "subscriptionModel cannot be null");
        this.eventStoreQueries = null;
        this.dcbEventStore = Objects.requireNonNull(dcbEventStore, "dcbEventStore cannot be null");
        this.dcbQuery = Objects.requireNonNull(dcbQuery, "dcbQuery cannot be null");
        this.config = Objects.requireNonNull(config, "config cannot be null");
    }

    /**
     * Create a dual-mode instance that catches up both stream subscriptions (by time, over {@code eventStoreQueries})
     * and DCB subscriptions (by {@code dcbposition}, over {@code dcbEventStore}). Each subscription is routed by its
     * filter and start position, so a single model serves a STREAM-and-DCB application. See ADR 25.
     *
     * @param subscriptionModel The subscription that'll be used to subscribe to new events <i>after</i> catch-up is completed.
     * @param eventStoreQueries The API that will be used for stream catch-up.
     * @param dcbEventStore     The DCB event store that will be used for DCB catch-up replay.
     * @param dcbQuery          The DCB query that selects the events a DCB subscription delivers.
     * @param config            The configuration to use.
     */
    public CatchupSubscriptionModel(PositionAwareSubscriptionModel subscriptionModel, EventStoreQueries eventStoreQueries, DcbEventStore dcbEventStore, DcbQuery dcbQuery, CatchupSubscriptionModelConfig config) {
        this.subscriptionModel = Objects.requireNonNull(subscriptionModel, "subscriptionModel cannot be null");
        this.eventStoreQueries = Objects.requireNonNull(eventStoreQueries, "eventStoreQueries cannot be null");
        this.dcbEventStore = Objects.requireNonNull(dcbEventStore, "dcbEventStore cannot be null");
        this.dcbQuery = Objects.requireNonNull(dcbQuery, "dcbQuery cannot be null");
        this.config = Objects.requireNonNull(config, "config cannot be null");
    }

    /**
     * Shortcut to start subscribing to events matching the supplied filter from begging of time. Same as doing:
     *
     * <pre>
     * subscriptionModel.subscribe(&lt;subscriptionId&gt;, &lt;filter&gt;, StartAtTime.beginningOfTime(), &lt;action&gt;);
     * </pre>
     */
    public Subscription subscribeFromBeginningOfTime(String subscriptionId, SubscriptionFilter filter, Consumer<CloudEvent> action) {
        return subscribe(subscriptionId, filter, StartAtTime.beginningOfTime(), action);
    }

    /**
     * Shortcut to start subscribing to <i>all</i> events from begging of time. Same as doing:
     *
     * <pre>
     * subscriptionModel.subscribe(&lt;subscriptionId&gt;, StartAtTime.beginningOfTime(), &lt;action&gt;);
     * </pre>
     */
    public Subscription subscribeFromBeginningOfTime(String subscriptionId, Consumer<CloudEvent> action) {
        return subscribe(subscriptionId, StartAtTime.beginningOfTime(), action);
    }

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, @Nullable StartAt startAt, Consumer<CloudEvent> action) {
        Objects.requireNonNull(startAt, "Start at supplier cannot be null");
        return routesToDcb(filter, startAt)
                ? subscribeDcb(subscriptionId, filter, startAt, action)
                : subscribeStream(subscriptionId, filter, startAt, action);
    }

    // Decide whether a subscription is a DCB or a stream subscription. A single-mode model has only one store, so it
    // always routes there (preserving the original behavior). A dual-mode model routes a DCB subscription, which either
    // carries a DcbSubscriptionFilter (set by the DcbSubscriptions DSL, the DcbSubscriptionModel facade, and the
    // @DcbSubscription annotation) or starts at an explicit DcbSubscriptionPosition, to the DCB path. Only an
    // already-resolved start position is inspected, never a dynamic one, so routing reads no subscription-position
    // storage.
    private boolean routesToDcb(@Nullable SubscriptionFilter filter, StartAt startAt) {
        if (dcbEventStore == null) {
            return false;
        }
        if (eventStoreQueries == null) {
            return true;
        }
        return filter instanceof DcbSubscriptionFilter || startsAtExplicitDcbPosition(startAt);
    }

    private static boolean startsAtExplicitDcbPosition(StartAt startAt) {
        return startAt instanceof StartAtSubscriptionPosition position
                && DcbSubscriptionPosition.isDcbSubscriptionPosition(position.subscriptionPosition);
    }

    private Subscription subscribeStream(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        // Stream catch-up converts the filter into an Occurrent Filter for the historical query, so only the stream
        // filter type is supported here. The DCB path accepts a DcbSubscriptionFilter and passes it to the inner model.
        if (filter != null && !(filter instanceof OccurrentSubscriptionFilter)) {
            throw new IllegalArgumentException("Only OccurrentSubscriptionFilter is supported!");
        }
        final StartAt firstStartAt;
        if (startAt.isDefault()) {
            // By default, we check if there's a subscription position stored for this subscription, if so we resume from there, otherwise,
            // delegate to the parent subscription model. 
            SubscriptionPosition subscriptionPosition = returnIfSubscriptionPositionStorageConfigIs(UseSubscriptionPositionInStorage.class, cfg -> cfg.storage().read(subscriptionId)).orElse(null);
            if (subscriptionPosition == null) {
                return getDelegatedSubscriptionModel().subscribe(subscriptionId, filter, startAt, action);
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

        // We want to continue from the wrapping subscription if it has something stored in its position storage.
        if (!isTimeBasedSubscriptionPosition(firstStartAt)) {
            return subscriptionModel.subscribe(subscriptionId, filter, firstStartAt, action);
        }

        Future<Subscription> subscriptionCompletableFuture = CompletableFuture.supplyAsync(() -> startCatchupSubscription(subscriptionId, filter, startAt, action, firstStartAt));
        return new CatchupSubscription(subscriptionId, subscriptionCompletableFuture);
    }

    private Subscription startCatchupSubscription(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, StartAt firstStartAt) {
        runningCatchupSubscriptions.put(subscriptionId, true);

        StartAt nextStartAt = firstStartAt.get(generateSubscriptionModelContext());
        SubscriptionPosition subscriptionPosition = ((StartAtSubscriptionPosition) Objects.requireNonNull(nextStartAt)).subscriptionPosition;

        Filter catchupFilter = deriveFilterToUseDuringCatchupPhase(filter, subscriptionPosition);

        EventStoreQueries eventStoreQueries = Objects.requireNonNull(this.eventStoreQueries, "eventStoreQueries");
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
    // DCB mode: replay historic DCB events ordered by dcbposition and resume by dcbposition (see ADR 20). Additive to
    // the stream path above; an instance is in exactly one mode (selected by constructor).
    // ---------------------------------------------------------------------------------------------------------------

    private Subscription subscribeDcb(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        final StartAt firstStartAt;
        if (startAt.isDefault()) {
            // Resume from the stored position if there is one, otherwise subscribe live (with the DCB query post-filter).
            SubscriptionPosition subscriptionPosition = returnIfSubscriptionPositionStorageConfigIs(UseSubscriptionPositionInStorage.class, cfg -> cfg.storage().read(subscriptionId)).orElse(null);
            if (subscriptionPosition == null) {
                return startLiveDcbSubscription(subscriptionId, filter, startAt, action, null);
            } else {
                firstStartAt = StartAt.subscriptionPosition(subscriptionPosition);
            }
        } else if (startAt.isDynamic()) {
            StartAt startAtGeneratedByDynamic = startAt.get(generateSubscriptionModelContext());
            if (startAtGeneratedByDynamic == null) {
                return startLiveDcbSubscription(subscriptionId, filter, startAt, action, null);
            } else {
                firstStartAt = startAtGeneratedByDynamic;
            }
        } else {
            firstStartAt = startAt;
        }

        // A non-DCB position means the catch-up already handed over and the live subscription stored a change-stream
        // token (or the caller asked to start live directly). Subscribe live, still applying the DCB query post-filter.
        if (!isDcbCatchupPosition(firstStartAt)) {
            return startLiveDcbSubscription(subscriptionId, filter, firstStartAt, action, null);
        }

        Future<Subscription> subscriptionCompletableFuture = CompletableFuture.supplyAsync(() -> startDcbCatchupSubscription(subscriptionId, filter, startAt, action, firstStartAt));
        return new CatchupSubscription(subscriptionId, subscriptionCompletableFuture);
    }

    private Subscription startLiveDcbSubscription(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAtToUse, Consumer<CloudEvent> action, @Nullable FixedSizeCache cache) {
        return subscriptionModel.subscribe(subscriptionId, filter, startAtToUse, dcbLiveConsumer(action, cache));
    }

    private Consumer<CloudEvent> dcbLiveConsumer(Consumer<CloudEvent> action, @Nullable FixedSizeCache cache) {
        DcbQuery query = Objects.requireNonNull(this.dcbQuery);
        return cloudEvent -> {
            // The live change stream sees every CloudEvent, so post-filter to the DCB events matching the query and
            // skip those already delivered during the catch-up phase (the handover seam).
            if (DcbCloudEvents.getPosition(cloudEvent) > 0 && DcbCloudEvents.matches(cloudEvent, query)
                    && (cache == null || !cache.isCached(cloudEvent.getId()))) {
                action.accept(cloudEvent);
            }
        };
    }

    private Subscription startDcbCatchupSubscription(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, StartAt firstStartAt) {
        runningCatchupSubscriptions.put(subscriptionId, true);
        DcbEventStore dcbEventStore = Objects.requireNonNull(this.dcbEventStore, "dcbEventStore");
        DcbQuery query = Objects.requireNonNull(this.dcbQuery, "dcbQuery");
        long windowSize = config.dcbCatchupPositionWindowSize;

        StartAt nextStartAt = firstStartAt.get(generateSubscriptionModelContext());
        SubscriptionPosition subscriptionPosition = ((StartAtSubscriptionPosition) Objects.requireNonNull(nextStartAt)).subscriptionPosition;
        long startPosition = DcbSubscriptionPosition.dcbPositionOf(subscriptionPosition);

        // Bulk replay: page through the DCB sequence from the resume position up to the head observed at the start, in
        // position windows so a large rebuild does not materialize the whole matched set at once. dcbposition is
        // monotonic and server-assigned, so this needs no count and no time sort, sidestepping both the clock-skew loss
        // and the estimatedDocumentCount undercount that the stream delta has to defend against (see ADR 20).
        long bulkHead = dcbEventStore.read(query, DcbReadOptions.between(startPosition, startPosition)).lastSequencePosition();
        long cursor = deliverDcbWindows(dcbEventStore, query, startPosition, bulkHead, windowSize, subscriptionId, action, null);

        // Capture the live resume position *after* the bulk replay so the change-stream token stays fresh, the same
        // reason as the stream path: a token captured before a long replay could age out of the change stream before
        // the handover.
        Class<? extends SubscriptionModel> delegatedSubscriptionModelType = getDelegatedSubscriptionModel().getClass();
        StartAt delegatedStartAt = startAt.get(new SubscriptionModelContext(delegatedSubscriptionModelType));
        final SubscriptionPosition globalSubscriptionPosition = delegatedStartAt == null ? null : subscriptionModel.globalSubscriptionPosition();

        FixedSizeCache catchupPhaseCache = new FixedSizeCache(config.cacheSize);

        // Reconcile events written during the bulk replay (positions beyond bulkHead) by continuing to page until the
        // head stops advancing. Re-reads of overlapping windows are deduped by the cache (delivery is at-least-once).
        // Any event written after the loop is newer than globalSubscriptionPosition and is covered by the live
        // subscription regardless.
        long head = dcbEventStore.read(query, DcbReadOptions.between(cursor, cursor)).lastSequencePosition();
        while (head > cursor && !shuttingDown && runningCatchupSubscriptions.containsKey(subscriptionId)) {
            cursor = deliverDcbWindows(dcbEventStore, query, cursor, head, windowSize, subscriptionId, action, catchupPhaseCache);
            head = dcbEventStore.read(query, DcbReadOptions.between(cursor, cursor)).lastSequencePosition();
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
                            // If nothing is stored, or the stored position is a DCB position (written by this catch-up),
                            // save the live change-stream position so the wrapped subscription resumes from there.
                            if ((position == null || DcbSubscriptionPosition.isDcbSubscriptionPosition(position)) && globalSubscriptionPosition != null) {
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

        return startDelegatedSubscription(subscriptionId, filter, subscriptionsWasCancelledOrShutdown, startAtToUse, dcbLiveConsumer(action, catchupPhaseCache));
    }

    /**
     * Delivers DCB events in {@code (fromExclusive, toInclusive]} by paging through position windows, and returns the
     * position the cursor reached. Stops early on shutdown or cancellation.
     */
    private long deliverDcbWindows(DcbEventStore dcbEventStore, DcbQuery query, long fromExclusive, long toInclusive, long windowSize, String subscriptionId, Consumer<CloudEvent> action, @Nullable FixedSizeCache cache) {
        long cursor = fromExclusive;
        while (cursor < toInclusive && !shuttingDown && runningCatchupSubscriptions.containsKey(subscriptionId)) {
            long upTo = Math.min(cursor + windowSize, toInclusive);
            DcbEventStream slice = dcbEventStore.read(query, DcbReadOptions.between(cursor, upTo));
            deliverCatchupEvents(slice.stream(), subscriptionId, action, cache, e -> DcbSubscriptionPosition.of(DcbCloudEvents.getPosition(e)));
            cursor = upTo;
        }
        return cursor;
    }

    private static boolean isDcbCatchupPosition(StartAt startAt) {
        StartAt start = startAt.get(generateSubscriptionModelContext());
        if (!(start instanceof StartAtSubscriptionPosition)) {
            return false;
        }
        return DcbSubscriptionPosition.isDcbSubscriptionPosition(((StartAtSubscriptionPosition) start).subscriptionPosition);
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
     * event by {@code positionToPersist}, which differs between stream mode (time based) and DCB mode (dcbposition).
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

    private static SubscriptionModelContext generateSubscriptionModelContext() {
        return new SubscriptionModelContext(CatchupSubscriptionModel.class);
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

    @Override
    public void cancelSubscription(String subscriptionId) {
        runningCatchupSubscriptions.remove(subscriptionId);
        subscriptionModel.cancelSubscription(subscriptionId);
        doIfSubscriptionPositionStorageConfigIs(UseSubscriptionPositionInStorage.class, cfg -> cfg.storage().delete(subscriptionId));
    }

    @PreDestroy
    @Override
    public void shutdown() {
        shuttingDown = true;
        runningCatchupSubscriptions.clear();
        subscriptionModel.shutdown();
    }

    public static boolean isTimeBasedSubscriptionPosition(StartAt startAt) {
        StartAt start = startAt.get(generateSubscriptionModelContext());
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

    @Override
    public SubscriptionModel getDelegatedSubscriptionModel() {
        return subscriptionModel;
    }

    private static class FixedSizeCache {
        private final LinkedHashMap<String, @Nullable String> cacheContent;

        FixedSizeCache(int size) {
            cacheContent = new LinkedHashMap<>() {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                    return this.size() > size;
                }
            };
        }

        private void put(String value) {
            cacheContent.put(value, null);
        }

        public boolean isCached(String key) {
            return cacheContent.containsKey(key);
        }
    }

    private <T, C extends SubscriptionPositionStorageConfig> Optional<@Nullable T> returnIfSubscriptionPositionStorageConfigIs(Class<C> cls, Function<C, @Nullable T> fn) {
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

    private record CancelledSubscription(String subscriptionId) implements Subscription {

        @Override
        public String id() {
            return subscriptionId;
        }

        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", CatchupSubscriptionModel.class.getSimpleName() + "[", "]")
                .add("subscriptionModel=" + subscriptionModel)
                .add("eventStoreQueries=" + eventStoreQueries)
                .add("dcbEventStore=" + dcbEventStore)
                .add("dcbQuery=" + dcbQuery)
                .add("config=" + config)
                .add("runningCatchupSubscriptions=" + runningCatchupSubscriptions)
                .add("shuttingDown=" + shuttingDown)
                .toString();
    }
}