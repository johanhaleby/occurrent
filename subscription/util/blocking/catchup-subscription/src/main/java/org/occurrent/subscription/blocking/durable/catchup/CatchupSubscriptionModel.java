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
import jakarta.annotation.Nullable;
import jakarta.annotation.PreDestroy;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
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
 * Also note that the if a the subscription crashes during catch-up mode it'll continue where it left-off on restart, given the no specific `StartAt` position is supplied (i.e. if {@code StartAt.subscriptionModelDefault() is used}).
 * For this to work, the subscription must store the subscription position in a {@link SubscriptionPositionStorage} implementation periodically. It's possible to configure
 * how often this should happen in the {@link CatchupSubscriptionModelConfig}.
 * </p>
 */
public class CatchupSubscriptionModel implements SubscriptionModel, DelegatingSubscriptionModel {

    private static final int DEFAULT_CACHE_SIZE = 100;

    private final PositionAwareSubscriptionModel subscriptionModel;
    private final EventStoreQueries eventStoreQueries;
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
        this.subscriptionModel = subscriptionModel;
        this.eventStoreQueries = eventStoreQueries;
        this.config = config;
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
    public Subscription subscribe(String subscriptionId, SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        Objects.requireNonNull(startAt, "Start at supplier cannot be null");
        if (filter != null && !(filter instanceof OccurrentSubscriptionFilter)) {
            throw new IllegalArgumentException("Unsupported!");
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

    private Subscription startCatchupSubscription(String subscriptionId, SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, StartAt firstStartAt) {
        runningCatchupSubscriptions.put(subscriptionId, true);

        SubscriptionPosition subscriptionPosition = ((StartAtSubscriptionPosition) firstStartAt.get(generateSubscriptionModelContext())).subscriptionPosition;

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
            // Here's the reason why we're forcing the wrapping subscription to be a PositionAwareBlockingSubscription.
            // This is in order to be 100% safe since we need to take events that are published meanwhile the EventStoreQuery
            // is executed. Thus, we need the global position of the subscription at the time of starting the query.
            globalSubscriptionPosition = subscriptionModel.globalSubscriptionPosition();
        }

        // Here we check if new events have arrived during catchup phase, if so we stream/catch-up these events as well.
        long numberOfEventsAfterCatchupSubscriptionCompleted = eventStoreQueries.count(catchupFilter);
        long numberOfEventsNotConsumed = numberOfEventsAfterCatchupSubscriptionCompleted - numberOfEventsBeforeStartingCatchupSubscription;

        // We generate a cache so that events that are streamed at the same time as streaming the events missed
        // during the catch-up phase are not streamed again.
        FixedSizeCache catchupPhaseCache = new FixedSizeCache(config.cacheSize);
        if (numberOfEventsNotConsumed > 0) {
            var cloudEvents = eventStoreQueries.query(catchupFilter, Math.toIntExact(numberOfEventsBeforeStartingCatchupSubscription), Math.toIntExact(numberOfEventsNotConsumed), config.catchupPhaseSortBy);
            runCatchupForStream(cloudEvents, subscriptionId, action, catchupPhaseCache);
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

        return startDelegatedSubscription(subscriptionId, filter, action, subscriptionsWasCancelledOrShutdown, startAtToUse, catchupPhaseCache);
    }

    private Subscription startDelegatedSubscription(String subscriptionId, SubscriptionFilter filter, Consumer<CloudEvent> action, boolean subscriptionsWasCancelledOrShutdown, StartAt startAtToUse, FixedSizeCache catchupPhaseEventCache) {
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
            subscription = getDelegatedSubscriptionModel().subscribe(subscriptionId, filter, startAtToUse, cloudEvent -> {
                if (!catchupPhaseEventCache.isCached(cloudEvent.getId())) {
                    action.accept(cloudEvent);
                }
            });
        }
        return subscription;
    }

    private static Filter deriveFilterToUseDuringCatchupPhase(SubscriptionFilter filter, SubscriptionPosition subscriptionPosition) {
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
        Stream<CloudEvent> takeWhile = cloudEvents.takeWhile(__ -> !shuttingDown && runningCatchupSubscriptions.containsKey(subscriptionId));
        if (cache != null) {
            takeWhile = takeWhile.peek(e -> cache.put(e.getId()));
        }
        takeWhile
                .peek(action)
                .filter(returnIfSubscriptionPositionStorageConfigIs(PersistSubscriptionPositionDuringCatchupPhase.class, PersistSubscriptionPositionDuringCatchupPhase::persistCloudEventPositionPredicate).orElse(__ -> false))
                .forEach(e -> doIfSubscriptionPositionStorageConfigIs(PersistSubscriptionPositionDuringCatchupPhase.class, cfg -> cfg.storage().save(subscriptionId, TimeBasedSubscriptionPosition.from(e.getTime()))));
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
        private final LinkedHashMap<String, String> cacheContent;

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

    private <T, C extends SubscriptionPositionStorageConfig> Optional<T> returnIfSubscriptionPositionStorageConfigIs(Class<C> cls, Function<C, T> fn) {
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
                .add("config=" + config)
                .add("runningCatchupSubscriptions=" + runningCatchupSubscriptions)
                .add("shuttingDown=" + shuttingDown)
                .toString();
    }
}