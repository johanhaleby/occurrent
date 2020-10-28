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

package org.occurrent.subscription.util.blocking.catchup.subscription;

import io.cloudevents.CloudEvent;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.*;
import org.occurrent.subscription.StartAt.StartAtSubscriptionPosition;
import org.occurrent.subscription.api.blocking.BlockingSubscription;
import org.occurrent.subscription.api.blocking.BlockingSubscriptionPositionStorage;
import org.occurrent.subscription.api.blocking.PositionAwareBlockingSubscription;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.util.blocking.catchup.subscription.SubscriptionPositionStorageConfig.PersistSubscriptionPositionDuringCatchupPhase;
import org.occurrent.subscription.util.blocking.catchup.subscription.SubscriptionPositionStorageConfig.UseSubscriptionPositionInStorage;

import javax.annotation.PreDestroy;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.occurrent.condition.Condition.gt;
import static org.occurrent.eventstore.api.blocking.EventStoreQueries.SortBy.TIME_ASC;
import static org.occurrent.filter.Filter.time;
import static org.occurrent.functionalsupport.internal.FunctionalSupport.takeWhile;
import static org.occurrent.time.internal.RFC3339.RFC_3339_DATE_TIME_FORMATTER;

/**
 * A {@link BlockingSubscription} that reads historic cloud events from the all event streams (see {@link EventStoreQueries#all()}) until caught up with the
 * {@link PositionAwareBlockingSubscription#globalSubscriptionPosition()} of the {@code subscription} (you probably want to narrow the historic set events of events
 * by using a {@link Filter} when subscribing). It'll automatically switch over to the supplied {@code subscription} when all history events are read and the subscription has caught-up.
 * <br>
 * <br>
 * <p>
 * Note that the implementation uses an in-memory cache (default size is {@value #DEFAULT_CACHE_SIZE} but this can be configured using a {@link CatchupSupportingBlockingSubscriptionConfig})
 * to reduce the number of duplicate event when switching from historic events to the current cloud event position. It's highly recommended that the application logic is idempotent if the
 * cache size doesn't cover all duplicate events.
 * </p>
 * <br>
 * <p>
 * Also note that the if a the subscription crashes during catch-up mode it'll continue where it left-off on restart, given the no specific `StartAt` position is supplied.
 * For this to work, the subscription must store the subscription position in a {@link BlockingSubscriptionPositionStorage} implementation periodically. It's possible to configure
 * how often this should happen in the {@link CatchupSupportingBlockingSubscriptionConfig}.
 * </p>
 */
public class CatchupSupportingBlockingSubscription implements BlockingSubscription {

    private static final int DEFAULT_CACHE_SIZE = 100;

    private final PositionAwareBlockingSubscription subscription;
    private final EventStoreQueries eventStoreQueries;
    private final CatchupSupportingBlockingSubscriptionConfig config;
    private final ConcurrentMap<String, Boolean> runningCatchupSubscriptions = new ConcurrentHashMap<>();

    /**
     * Create a new instance of {@link CatchupSupportingBlockingSubscription} the uses a default {@link CatchupSupportingBlockingSubscriptionConfig} with a cache size of
     * {@value #DEFAULT_CACHE_SIZE} but store the subscription position during the <i>catch-up</i> phase (i.e. if the application crashes or is shutdown during the
     * catch-up phase then the subscription will start from the beginning on application restart). After the catch-up phase has completed, the {@link PositionAwareBlockingSubscription}
     * will dictate how often the subscription position is stored.
     *
     * @param subscription      The subscription that'll be used to subscribe to new events <i>after</i> catch-up is completed.
     * @param eventStoreQueries The API that will be used for catch-up
     */
    public CatchupSupportingBlockingSubscription(PositionAwareBlockingSubscription subscription, EventStoreQueries eventStoreQueries) {
        this(subscription, eventStoreQueries, new CatchupSupportingBlockingSubscriptionConfig(DEFAULT_CACHE_SIZE));
    }

    /**
     * Create a new instance of {@link CatchupSupportingBlockingSubscription} the uses the supplied {@link CatchupSupportingBlockingSubscriptionConfig}.
     * After catch-up mode has completed, the {@link PositionAwareBlockingSubscription} will dictate how often the subscription position is stored.
     *
     * @param subscription      The subscription that'll be used to subscribe to new events <i>after</i> catch-up is completed.
     * @param eventStoreQueries The API that will be used for catch-up
     * @param config            The configuration to use
     */
    public CatchupSupportingBlockingSubscription(PositionAwareBlockingSubscription subscription, EventStoreQueries eventStoreQueries, CatchupSupportingBlockingSubscriptionConfig config) {
        this.subscription = subscription;
        this.eventStoreQueries = eventStoreQueries;
        this.config = config;
    }

    @Override
    public Subscription subscribe(String subscriptionId, SubscriptionFilter filter, Supplier<StartAt> startAtSupplier, Consumer<CloudEvent> action) {
        if (filter != null && !(filter instanceof OccurrentSubscriptionFilter)) {
            throw new IllegalArgumentException("Unsupported!");
        }

        runningCatchupSubscriptions.put(subscriptionId, true);

        final StartAt startAt;
        if (startAtSupplier == null) {
            startAt = StartAt.subscriptionPosition(TimeBasedSubscriptionPosition.beginningOfTime());
        } else {
            startAt = startAtSupplier.get();
        }

        // TODO!! We want to continue from the wrapping subscription if it has something stored in it's position storage!!!!
        if (!isTimeBasedSubscriptionPosition(startAt)) {
            return subscription.subscribe(subscriptionId, filter, startAtSupplier, action::accept);
        }

        SubscriptionPosition subscriptionPosition = ((StartAtSubscriptionPosition) startAt).subscriptionPosition;

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
        final StartAt wrappingSubscriptionStartPosition = StartAt.subscriptionPosition(subscription.globalSubscriptionPosition());

        FixedSizeCache cache = new FixedSizeCache(config.cacheSize);
        final Stream<CloudEvent> stream;
        if (filter == null) {
            stream = eventStoreQueries.query(timeFilter, TIME_ASC);
        } else {
            Filter userSuppliedFilter = ((OccurrentSubscriptionFilter) filter).filter;
            stream = eventStoreQueries.query(timeFilter.and(userSuppliedFilter), TIME_ASC);
        }

        takeWhile(stream, __ -> runningCatchupSubscriptions.containsKey(subscriptionId))
                .peek(action)
                .peek(e -> cache.put(e.getId()))
                .filter(returnIfSubscriptionPositionStorageConfigIs(PersistSubscriptionPositionDuringCatchupPhase.class, cfg -> cfg.persistCloudEventPositionPredicate).orElse(__ -> false))
                .forEach(e -> doIfSubscriptionPositionStorageConfigIs(PersistSubscriptionPositionDuringCatchupPhase.class, cfg -> cfg.storage.save(subscriptionId, TimeBasedSubscriptionPosition.from(e.getTime()))));

        runningCatchupSubscriptions.remove(subscriptionId);
        // TODO Should we remove the position from storage?! For example if the wrapping subscription is not storing the position?
        // Be careful since the wrapping subscription has not yet saved the global position here...
        return subscription.subscribe(subscriptionId, filter, wrappingSubscriptionStartPosition, cloudEvent -> {
            if (!cache.isCached(cloudEvent.getId())) {
                action.accept(cloudEvent);
            }
        });
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        runningCatchupSubscriptions.remove(subscriptionId);
        subscription.cancelSubscription(subscriptionId);
        doIfSubscriptionPositionStorageConfigIs(UseSubscriptionPositionInStorage.class, cfg -> cfg.storage.delete(subscriptionId));
    }

    @Override
    public Subscription subscribe(String subscriptionId, Consumer<CloudEvent> action) {
        return subscribe(subscriptionId, (SubscriptionFilter) null, action);
    }


    @Override
    public Subscription subscribe(String subscriptionId, SubscriptionFilter filter, Consumer<CloudEvent> action) {
        return subscribe(subscriptionId, filter, () -> {
            SubscriptionPosition subscriptionPosition = returnIfSubscriptionPositionStorageConfigIs(UseSubscriptionPositionInStorage.class, cfg -> cfg.storage.read(subscriptionId)).orElse(null);
            // We use null instead of StartAt.now() if subscriptionPosition doesn't exist so that the wrapping subscription can determine what to do
            return subscriptionPosition == null ? null : StartAt.subscriptionPosition(subscriptionPosition);
        }, action);
    }

    @PreDestroy
    @Override
    public void shutdown() {
        runningCatchupSubscriptions.clear();
        subscription.shutdown();
    }

    public static boolean isTimeBasedSubscriptionPosition(StartAt startAt) {
        if (!(startAt instanceof StartAtSubscriptionPosition)) {
            return false;
        }

        SubscriptionPosition subscriptionPosition = ((StartAtSubscriptionPosition) startAt).subscriptionPosition;
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

    private <T, C extends SubscriptionPositionStorageConfig> void doIfSubscriptionPositionStorageConfigIs(Class<C> cls, Consumer<C> consumer) {
        if (cls.isInstance(config.subscriptionStorageConfig)) {
            consumer.accept(cls.cast(config.subscriptionStorageConfig));
        }
    }
}