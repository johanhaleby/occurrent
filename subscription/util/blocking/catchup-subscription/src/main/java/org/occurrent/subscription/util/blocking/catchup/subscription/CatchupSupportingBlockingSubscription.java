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
import org.occurrent.subscription.OccurrentSubscriptionFilter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.StartAtSubscriptionPosition;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionPosition;
import org.occurrent.subscription.api.blocking.BlockingSubscription;
import org.occurrent.subscription.api.blocking.BlockingSubscriptionPositionStorage;
import org.occurrent.subscription.api.blocking.PositionAwareBlockingSubscription;
import org.occurrent.subscription.api.blocking.Subscription;

import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.occurrent.condition.Condition.gte;
import static org.occurrent.filter.Filter.time;
import static org.occurrent.time.internal.RFC3339.RFC_3339_DATE_TIME_FORMATTER;

// Not that we don't implement PositionAwareBlockingSubscription since we don't have a "globalSubscruptionPosition"
public class CatchupSupportingBlockingSubscription implements BlockingSubscription<CloudEvent> {

    private final PositionAwareBlockingSubscription subscription;
    private final EventStoreQueries eventStoreQueries;
    private final BlockingSubscriptionPositionStorage storage;


    // TODO Strategy catch-up peristence strategy (hur många events innan vi ska spara position)
    public CatchupSupportingBlockingSubscription(PositionAwareBlockingSubscription subscription, EventStoreQueries eventStoreQueries, BlockingSubscriptionPositionStorage storage) {
        this.subscription = subscription;
        this.eventStoreQueries = eventStoreQueries;
        this.storage = storage;
    }

    @Override
    public Subscription subscribe(String subscriptionId, SubscriptionFilter filter, Supplier<StartAt> startAtSupplier, Consumer<CloudEvent> action) {
        if (filter != null && !(filter instanceof OccurrentSubscriptionFilter)) {
            throw new IllegalArgumentException("Unsupported!");
        }

        final StartAt startAt;
        if (startAtSupplier == null) {
            startAt = StartAt.subscriptionPosition(TimeBasedSubscriptionPosition.beginningOfTime());
        } else {
            startAt = startAtSupplier.get();
        }

        if (!isTimeBasedSubscriptionPosition(startAt)) {
            return subscription.subscribe(subscriptionId, filter, startAtSupplier, action::accept);
        }

        TimeBasedSubscriptionPosition subscriptionPosition = (TimeBasedSubscriptionPosition) ((StartAtSubscriptionPosition) startAt).subscriptionPosition;


        final Filter timeFilter;
        if (subscriptionPosition.isBeginningOfTime()) {
            timeFilter = Filter.all();
        } else {
            OffsetDateTime offsetDateTime = (OffsetDateTime) RFC_3339_DATE_TIME_FORMATTER.parse(subscriptionPosition.asString());
            timeFilter = time(gte(offsetDateTime));
        }

        // Here's the reason why we're forcing the wrapping subscription to be a PositionAwareBlockingSubscription.
        // This is in order to be 100% safe since we need to take events that are published meanwhile the EventStoreQuery
        // is executed. Thus we need the global position of the stream at the time of starting the query.
        final StartAt wrappingSubscriptionStartPosition = StartAt.subscriptionPosition(subscription.globalSubscriptionPosition());

        // TODO Make configurable
        FixedSizeCache cache = new FixedSizeCache(100);
        final Stream<CloudEvent> stream;
        if (filter == null) {
            stream = eventStoreQueries.query(timeFilter);
        } else {
            Filter userSuppliedFilter = ((OccurrentSubscriptionFilter) filter).filter;
            stream = eventStoreQueries.query(timeFilter.and(userSuppliedFilter));
        }

        stream.peek(e -> cache.put(e.getId())).peek(action).forEach(e -> storage.save(subscriptionId, TimeBasedSubscriptionPosition.from(e.getTime())));
        return subscription.subscribe(subscriptionId, filter, wrappingSubscriptionStartPosition, cloudEvent -> {
            if (!cache.isCached(cloudEvent.getId())) {
                action.accept(cloudEvent);
            }
        });
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        subscription.cancelSubscription(subscriptionId);
        storage.delete(subscriptionId);
    }


    @Override
    public Subscription subscribe(String subscriptionId, SubscriptionFilter filter, Consumer<CloudEvent> action) {
        return subscribe(subscriptionId, filter, () -> {
            SubscriptionPosition subscriptionPosition = storage.read(subscriptionId);
            // We use null instead of StartAt.now() if subscriptionPosition doesn't exist so that the wrapping subscription can determine what to do
            return subscriptionPosition == null ? null : StartAt.subscriptionPosition(subscriptionPosition);
        }, action);
    }

    @Override
    public void shutdown() {
        subscription.shutdown();
    }

    public static boolean isTimeBasedSubscriptionPosition(StartAt startAt) {
        return (startAt instanceof StartAtSubscriptionPosition) && ((StartAtSubscriptionPosition) startAt).subscriptionPosition instanceof TimeBasedSubscriptionPosition;
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

        private String put(String value) {
            return cacheContent.put(value, null);
        }

        public boolean isCached(String key) {
            return cacheContent.containsKey(key);
        }
    }

}