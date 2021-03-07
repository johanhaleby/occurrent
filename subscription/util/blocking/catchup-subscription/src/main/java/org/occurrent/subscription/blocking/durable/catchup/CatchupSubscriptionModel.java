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
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.*;
import org.occurrent.subscription.StartAt.StartAtSubscriptionPosition;
import org.occurrent.subscription.api.blocking.*;
import org.occurrent.subscription.blocking.durable.catchup.SubscriptionPositionStorageConfig.UseSubscriptionPositionInStorage;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.occurrent.condition.Condition.gt;
import static org.occurrent.filter.Filter.time;
import static org.occurrent.functionalsupport.internal.FunctionalSupport.takeWhile;
import static org.occurrent.time.internal.RFC3339.RFC_3339_DATE_TIME_FORMATTER;

/**
 * A {@link SubscriptionModel} that reads historic cloud events from the all event streams (see {@link EventStoreQueries#all()}) until caught up with the
 * {@link PositionAwareSubscriptionModel#globalSubscriptionPosition()} of the {@code subscription} (you probably want to narrow the historic set events of events
 * by using a {@link Filter} when subscribing). It'll automatically switch over to the supplied {@code subscription} when all history events are read and the subscription has caught-up.
 * <br>
 * <br>
 * <p>
 * Note that the implementation uses an in-memory cache (default size is {@value #DEFAULT_CACHE_SIZE} but this can be configured using a {@link CatchupSubscriptionModelConfig})
 * to reduce the number of duplicate event when switching from historic events to the current cloud event position. It's highly recommended that the application logic is idempotent if the
 * cache size doesn't cover all duplicate events.
 * </p>
 * <br>
 * <p>
 * Also note that the if a the subscription crashes during catch-up mode it'll continue where it left-off on restart, given the no specific `StartAt` position is supplied.
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

    @Override
    public Subscription subscribe(String subscriptionId, SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        Objects.requireNonNull(startAt, "Start at supplier cannot be null");
        if (filter != null && !(filter instanceof OccurrentSubscriptionFilter)) {
            throw new IllegalArgumentException("Unsupported!");
        }

        runningCatchupSubscriptions.put(subscriptionId, true);

        final StartAt firstStartAt;
        if (startAt.isDefault()) {
            firstStartAt = StartAt.dynamic(() -> {
                SubscriptionPosition subscriptionPosition = returnIfSubscriptionPositionStorageConfigIs(UseSubscriptionPositionInStorage.class, cfg -> cfg.storage.read(subscriptionId)).orElse(null);
                return subscriptionPosition == null ? StartAt.subscriptionPosition(TimeBasedSubscriptionPosition.beginningOfTime()) : StartAt.subscriptionPosition(subscriptionPosition);
            });
        } else {
            firstStartAt = startAt;
        }

        // We want to continue from the wrapping subscription if it has something stored in it's position storage.
        if (!isTimeBasedSubscriptionPosition(firstStartAt)) {
            return subscriptionModel.subscribe(subscriptionId, filter, firstStartAt, action);
        }

        SubscriptionPosition subscriptionPosition = ((StartAtSubscriptionPosition) firstStartAt.get()).subscriptionPosition;

        final Filter timeFilter;
        if (isBeginningOfTime(subscriptionPosition)) {
            timeFilter = Filter.all();
        } else {
            OffsetDateTime offsetDateTime = OffsetDateTime.parse(subscriptionPosition.asString(), RFC_3339_DATE_TIME_FORMATTER);
            timeFilter = time(gt(offsetDateTime));
        }

        // Here's the reason why we're forcing the wrapping subscription to be a PositionAwareBlockingSubscription.
        // This is in order to be 100% safe since we need to take events that are published meanwhile the EventStoreQuery
        // is executed. Thus we need the global position of the subscription at the time of starting the query.
        SubscriptionPosition globalSubscriptionPosition = subscriptionModel.globalSubscriptionPosition();

        FixedSizeCache cache = new FixedSizeCache(config.cacheSize);
        final Stream<CloudEvent> stream;
        if (filter == null) {
            stream = eventStoreQueries.query(timeFilter, config.catchupPhaseSortBy);
        } else {
            Filter userSuppliedFilter = ((OccurrentSubscriptionFilter) filter).filter;
            stream = eventStoreQueries.query(timeFilter.and(userSuppliedFilter), config.catchupPhaseSortBy);
        }

        takeWhile(stream, __ -> !shuttingDown && runningCatchupSubscriptions.containsKey(subscriptionId))
                .peek(action)
                .peek(e -> cache.put(e.getId()))
                .filter(returnIfSubscriptionPositionStorageConfigIs(SubscriptionPositionStorageConfig.PersistSubscriptionPositionDuringCatchupPhase.class, cfg -> cfg.persistCloudEventPositionPredicate).orElse(__ -> false))
                .forEach(e -> doIfSubscriptionPositionStorageConfigIs(SubscriptionPositionStorageConfig.PersistSubscriptionPositionDuringCatchupPhase.class, cfg -> cfg.storage.save(subscriptionId, TimeBasedSubscriptionPosition.from(e.getTime()))));

        final boolean subscriptionsWasCancelledOrShutdown;
        if (!shuttingDown && runningCatchupSubscriptions.containsKey(subscriptionId)) {
            subscriptionsWasCancelledOrShutdown = false;
            runningCatchupSubscriptions.remove(subscriptionId);
        } else {
            // When runningCatchupSubscriptions doesn't contain the key at this stage it means that it has been explicitly cancelled.
            subscriptionsWasCancelledOrShutdown = true;
        }

        // TODO Should we remove the position from storage?! For example if the wrapping subscription is not storing the position?

        // Be careful since the wrapping subscription has not yet saved the global position here...
        // We need a durable cache in order to be 100% safe.

        // When the catch-up subscription is ready we store the global position in the position storage so that subscriptions
        // that have not received _any_ new events during replay will start at the global position if the application is restarted.
        // Otherwise nothing will be stored in the "storage" and replay of historic events will take place again on application restart
        // which is not what we want! The reason for doing this with UseSubscriptionPositionInStorage (as opposed to just
        // PersistSubscriptionPositionDuringCatchupPhase) is that if using a "storage" at all in the config, is to accommodate
        // that the wrapping subscription continues from where we left off.
        StartAt startAtSupplierToUse = StartAt.dynamic(this.<Supplier<StartAt>, UseSubscriptionPositionInStorage>returnIfSubscriptionPositionStorageConfigIs(UseSubscriptionPositionInStorage.class,
                cfg -> () -> {
                    // It's important that we find the document inside the supplier so that we lookup the latest resume token on retry
                    SubscriptionPosition position = cfg.storage.read(subscriptionId);
                    // If there is no position stored in storage, or if the stored position is time-based
                    // (i.e. written by the catch-up subscription), we save the globalSubscriptionPosition.
                    // The reason that we need to write the time-based subscription position in this case
                    // is that the wrapped subscription might not support time-based subscriptions.
                    if ((position == null || isTimeBasedSubscriptionPosition(position)) && globalSubscriptionPosition != null) {
                        position = cfg.storage.save(subscriptionId, globalSubscriptionPosition);
                    }
                    return StartAt.subscriptionPosition(position);
                }).orElse(() -> globalSubscriptionPosition == null ? StartAt.now() : StartAt.subscriptionPosition(globalSubscriptionPosition)));

        final Subscription subscription;
        if (subscriptionsWasCancelledOrShutdown) {
            doIfSubscriptionPositionStorageConfigIs(UseSubscriptionPositionInStorage.class, cfg -> {
                // Only store position if using storage and no position has been stored!
                if (!cfg.storage.exists(subscriptionId)) {
                    startAtSupplierToUse.get();
                }
            });
            subscription = new CancelledSubscription(subscriptionId);
        } else {
            subscription = this.subscriptionModel.subscribe(subscriptionId, filter, startAtSupplierToUse, cloudEvent -> {
                if (!cache.isCached(cloudEvent.getId())) {
                    action.accept(cloudEvent);
                }
            });
        }

        return subscription;
    }

    @Override
    public void stop() {
        getDelegatedSubscriptionModel().stop();
    }

    @Override
    public void start() {
        getDelegatedSubscriptionModel().start();
    }

    @Override
    public boolean isRunning() {
        return getDelegatedSubscriptionModel().isRunning();
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return getDelegatedSubscriptionModel().isRunning();
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
        doIfSubscriptionPositionStorageConfigIs(UseSubscriptionPositionInStorage.class, cfg -> cfg.storage.delete(subscriptionId));
    }

    @PreDestroy
    @Override
    public void shutdown() {
        shuttingDown = true;
        runningCatchupSubscriptions.clear();
        subscriptionModel.shutdown();
    }

    public static boolean isTimeBasedSubscriptionPosition(StartAt startAt) {
        StartAt start = startAt.get();
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
            cacheContent = new LinkedHashMap<String, String>() {
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

    private static class CancelledSubscription implements Subscription {
        private final String subscriptionId;

        public CancelledSubscription(String subscriptionId) {
            this.subscriptionId = subscriptionId;
        }

        @Override
        public String id() {
            return subscriptionId;
        }

        @Override
        public void waitUntilStarted() {

        }

        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }
}