/*
 * Copyright 2021 Johan Haleby
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
import org.occurrent.subscription.internal.ExecutorShutdown;

import javax.annotation.PreDestroy;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An in-memory subscription model
 */
public class InMemorySubscriptionModel implements SubscriptionModel, Consumer<Stream<CloudEvent>> {

    private final ConcurrentMap<String, InMemorySubscription> subscriptions;
    private final ConcurrentMap<String, Boolean> pausedSubscriptions;
    private final ExecutorService cloudEventDispatcher;
    private final RetryStrategy retryStrategy;
    private final Supplier<BlockingQueue<CloudEvent>> queueSupplier;

    private volatile boolean shutdown = false;
    private volatile boolean running = true;


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
     * @param cloudEventDispatcher The {@link ExecutorService} that will be used when dispatching cloud events to subscribers
     * @param retryStrategy        The retry strategy
     */
    public InMemorySubscriptionModel(ExecutorService cloudEventDispatcher, RetryStrategy retryStrategy) {
        this(cloudEventDispatcher, retryStrategy, LinkedBlockingQueue::new);
    }

    /**
     * Create an instance of {@link InMemorySubscriptionModel} with the given parameters
     *
     * @param cloudEventDispatcher The {@link ExecutorService} that will be used when dispatching cloud events to subscribers
     * @param retryStrategy        The retry strategy
     * @param queue                The blocking queue to use for this instance.
     */
    public InMemorySubscriptionModel(ExecutorService cloudEventDispatcher, RetryStrategy retryStrategy, Supplier<BlockingQueue<CloudEvent>> queue) {
        if (cloudEventDispatcher == null) {
            throw new IllegalArgumentException("cloudEventDispatcher cannot be null");
        } else if (retryStrategy == null) {
            throw new IllegalArgumentException(RetryStrategy.class.getSimpleName() + " cannot be null");
        } else if (queue == null) {
            throw new IllegalArgumentException(BlockingQueue.class.getSimpleName() + " cannot be null");
        }
        this.queueSupplier = queue;
        this.cloudEventDispatcher = cloudEventDispatcher;
        this.retryStrategy = retryStrategy;
        this.subscriptions = new ConcurrentHashMap<>();
        this.pausedSubscriptions = new ConcurrentHashMap<>();
    }

    @Override
    public synchronized Subscription subscribe(String subscriptionId, SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        if (shutdown) {
            throw new IllegalStateException("Cannot subscribe when shutdown");
        } else if (subscriptionId == null) {
            throw new IllegalArgumentException("subscriptionId cannot be null");
        } else if (action == null) {
            throw new IllegalArgumentException("action cannot be null");
        } else if (subscriptions.containsKey(subscriptionId) || pausedSubscriptions.containsKey(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is already defined.");
        } else if (startAt == null) {
            throw new IllegalArgumentException(StartAt.class.getSimpleName() + " cannot be null");
        }

        StartAt startAtToUse = startAt.get();
        if (!startAtToUse.isNow() && !startAtToUse.isDefault()) {
            throw new IllegalArgumentException(InMemorySubscriptionModel.class.getSimpleName() + " only supports starting from 'now' and 'default' (StartAt.now() or StartAt.subscriptionModelDefault())");
        }

        final Filter f = getFilter(filter);

        InMemorySubscription subscription = new InMemorySubscription(subscriptionId, queueSupplier.get(), action, f, retryStrategy);
        subscriptions.put(subscriptionId, subscription);

        if (!running) {
            pausedSubscriptions.put(subscriptionId, true);
        }
        cloudEventDispatcher.execute(subscription);
        return subscription;
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        subscriptions.remove(subscriptionId);
        pausedSubscriptions.remove(subscriptionId);
    }

    @Override
    public void accept(Stream<CloudEvent> cloudEventStream) {
        if (!running) {
            return;
        }
        List<CloudEvent> cloudEvents = cloudEventStream.collect(Collectors.toList());
        subscriptions.values().forEach(subscription -> {
            if (isRunning(subscription.id())) {
                cloudEvents.stream()
                        .filter(subscription::matches)
                        .forEach(subscription::eventAvailable);
            }
        });
    }

    @PreDestroy
    @Override
    public void shutdown() {
        synchronized (subscriptions) {
            shutdown = true;
            subscriptions.values().forEach(InMemorySubscription::shutdown);
            subscriptions.clear();
        }

        pausedSubscriptions.clear();
        ExecutorShutdown.shutdownSafely(cloudEventDispatcher, 5, TimeUnit.SECONDS);
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

    @Override
    public void stop() {
        running = false;
        subscriptions.values().forEach(subscription -> pausedSubscriptions.put(subscription.id(), true));
    }

    @Override
    public void start() {
        running = true;
        pausedSubscriptions.clear();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return running && subscriptions.containsKey(subscriptionId) && !pausedSubscriptions.containsKey(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return pausedSubscriptions.containsKey(subscriptionId);
    }

    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        if (!isPaused(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is not paused");
        }
        running = true;
        pausedSubscriptions.remove(subscriptionId);
        return subscriptions.get(subscriptionId);
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
        if (!isRunning(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is not running");
        }
        pausedSubscriptions.put(subscriptionId, true);
    }
}