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

package org.occurrent.subscription.inmemory;

import io.cloudevents.CloudEvent;
import org.occurrent.filter.Filter;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.OccurrentSubscriptionFilter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import javax.annotation.PreDestroy;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.occurrent.inmemory.filtermatching.FilterMatcher.matchesFilter;
import static org.occurrent.retry.internal.RetryExecution.convertToDelayStream;
import static org.occurrent.retry.internal.RetryExecution.executeWithRetry;

/**
 * An in-memory subscription model
 */
public class InMemorySubscriptionModel implements SubscriptionModel, Consumer<Stream<CloudEvent>> {

    private final ConcurrentMap<String, Consumer<CloudEvent>> subscriptions;
    private final Executor cloudEventDispatcher;
    private final RetryStrategy retryStrategy;

    private volatile boolean shuttingDown = false;

    /**
     * Create a new {@link InMemorySubscriptionModel} with an unbounded cached thread pool and retry strategy with
     * fixed delay of 200 millis.
     */
    public InMemorySubscriptionModel() {
        this(RetryStrategy.fixed(200));
    }

    /**
     * Create a new {@link InMemorySubscriptionModel} with an unbounded cached thread pool and the supplied {@link RetryStrategy}.
     */
    public InMemorySubscriptionModel(RetryStrategy retryStrategy) {
        this(Executors.newCachedThreadPool(), retryStrategy);
    }

    /**
     * Create an instance of {@link InMemorySubscriptionModel} with the given parameters
     *
     * @param cloudEventDispatcher The {@link Executor} that will be used when dispatching cloud events to subscribers
     * @param retryStrategy        The retry strategy
     */
    public InMemorySubscriptionModel(Executor cloudEventDispatcher, RetryStrategy retryStrategy) {
        if (cloudEventDispatcher == null) {
            throw new IllegalArgumentException("cloudEventDispatcher cannot be null");
        } else if (retryStrategy == null) {
            throw new IllegalArgumentException(RetryStrategy.class.getSimpleName() + " cannot be null");
        }
        this.cloudEventDispatcher = cloudEventDispatcher;
        this.retryStrategy = retryStrategy;
        this.subscriptions = new ConcurrentHashMap<>();
    }


    @Override
    public Subscription subscribe(String subscriptionId, SubscriptionFilter filter, Supplier<StartAt> startAtSupplier, Consumer<CloudEvent> action) {
        if (shuttingDown) {
            throw new IllegalStateException("Cannot subscribe when shutdown");
        } else if (subscriptionId == null) {
            throw new IllegalArgumentException("subscriptionId cannot be null");
        } else if (action == null) {
            throw new IllegalArgumentException("action cannot be null");
        }

        if (startAtSupplier != null) {
            StartAt startAt = startAtSupplier.get();
            if (!startAt.isNow()) {
                throw new IllegalArgumentException(InMemorySubscriptionModel.class.getSimpleName() + " only supports starting from 'now' (StartAt.now())");
            }
        }

        final Filter f = getFilter(filter);
        subscriptions.put(subscriptionId, cloudEvent -> {
            if (matchesFilter(cloudEvent, f)) {
                action.accept(cloudEvent);
            }
        });

        return new InMemorySubscription(subscriptionId);
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        subscriptions.remove(subscriptionId);
    }

    @Override
    public void accept(Stream<CloudEvent> cloudEventStream) {
        List<CloudEvent> cloudEvents = cloudEventStream.collect(Collectors.toList());
        subscriptions.values().forEach(subscription -> {
            Runnable runnable = () -> cloudEvents.forEach(subscription);
            cloudEventDispatcher.execute(executeWithRetry(runnable, __ -> !shuttingDown, convertToDelayStream(retryStrategy)));
        });
    }

    @PreDestroy
    public void shutdown() {
        shuttingDown = true;
    }

    private static Filter getFilter(SubscriptionFilter filter) {
        final Filter f;
        if (filter == null) {
            f = Filter.all();
        } else if (filter instanceof OccurrentSubscriptionFilter) {
            f = ((OccurrentSubscriptionFilter) filter).filter;
        } else {
            throw new IllegalArgumentException(InMemorySubscriptionModel.class.getSimpleName() + " only support filters of type " + OccurrentSubscriptionFilter.class.getName());
        }
        return f;
    }
}
