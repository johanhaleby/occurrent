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

import javax.annotation.PreDestroy;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.occurrent.condition.Condition.gt;
import static org.occurrent.eventstore.api.blocking.EventStoreQueries.SortBy.TIME_ASC;
import static org.occurrent.filter.Filter.time;
import static org.occurrent.functionalsupport.internal.FunctionalSupport.takeWhile;
import static org.occurrent.time.internal.RFC3339.RFC_3339_DATE_TIME_FORMATTER;

public class CatchupSupportingBlockingSubscription implements BlockingSubscription<CloudEvent> {

    private final PositionAwareBlockingSubscription subscription;
    private final EventStoreQueries eventStoreQueries;
    private final BlockingSubscriptionPositionStorage storage;
    private final CatchupSupportingBlockingSubscriptionConfig config;
    private final ConcurrentMap<String, Boolean> runningCatchupSubscriptions = new ConcurrentHashMap<>();

    // TODO Strategy catch-up peristence strategy (hur många events innan vi ska spara position)
    public CatchupSupportingBlockingSubscription(PositionAwareBlockingSubscription subscription, EventStoreQueries eventStoreQueries, BlockingSubscriptionPositionStorage storage) {
        this(subscription, eventStoreQueries, storage, new CatchupSupportingBlockingSubscriptionConfig(100, EveryN.every(10)));
    }

    public CatchupSupportingBlockingSubscription(PositionAwareBlockingSubscription subscription, EventStoreQueries eventStoreQueries, BlockingSubscriptionPositionStorage storage,
                                                 CatchupSupportingBlockingSubscriptionConfig config) {
        this.subscription = subscription;
        this.eventStoreQueries = eventStoreQueries;
        this.storage = storage;
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
        // is executed. Thus we need the global position of the stream at the time of starting the query.
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
                .filter(config.persistCloudEventPositionPredicate)
                .forEach(e -> storage.save(subscriptionId, TimeBasedSubscriptionPosition.from(e.getTime())));

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
        storage.delete(subscriptionId);
    }

    @Override
    public Subscription subscribe(String subscriptionId, Consumer<CloudEvent> action) {
        return subscribe(subscriptionId, (SubscriptionFilter) null, action);
    }


    @Override
    public Subscription subscribe(String subscriptionId, SubscriptionFilter filter, Consumer<CloudEvent> action) {
        return subscribe(subscriptionId, filter, () -> {
            SubscriptionPosition subscriptionPosition = storage.read(subscriptionId);
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
}