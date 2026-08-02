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
import jakarta.annotation.PreDestroy;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.DurationToTimeoutConverter;
import org.occurrent.subscription.DurationToTimeoutConverter.Timeout;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.inmemory.filtermatching.DataFieldReader;
import org.occurrent.subscription.SubscriptionFilterMatcher;
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.internal.ExecutorShutdown;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * An in-memory subscription model
 */
@NullMarked
public class InMemorySubscriptionModel implements SubscriptionModel, IntrospectableSubscriptionModel, Consumer<List<CloudEvent>> {

    private final ConcurrentMap<String, InMemorySubscription> subscriptions;
    private final ConcurrentMap<String, Boolean> pausedSubscriptions;
    private final ExecutorService cloudEventDispatcher;
    private final RetryStrategy retryStrategy;
    private final Supplier<BlockingQueue<CloudEvent>> queueSupplier;

    private volatile boolean shutdown = false;
    private volatile boolean running = true;
    private final DataFieldReader dataFieldReader;


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
        this(cloudEventDispatcher, retryStrategy, queue, DataFieldReader.refusing());
    }

    /**
     * Create an instance of {@link InMemorySubscriptionModel} that can answer a subscription filter on a
     * {@code data} payload field by reading it through the supplied reader. Without one, such a filter is refused
     * rather than silently matching nothing.
     *
     * @param cloudEventDispatcher The {@link ExecutorService} that will be used when dispatching cloud events to subscribers
     * @param retryStrategy        The retry strategy
     * @param queue                The blocking queue to use for this instance.
     * @param dataFieldReader      Reads a field out of an event's payload. Occurrent ships a Jackson-backed one in
     *                             {@code occurrent-common-inmemory-filter-matching-jackson}.
     */
    public InMemorySubscriptionModel(ExecutorService cloudEventDispatcher, RetryStrategy retryStrategy, Supplier<BlockingQueue<CloudEvent>> queue, DataFieldReader dataFieldReader) {
        if (dataFieldReader == null) {
            throw new IllegalArgumentException(DataFieldReader.class.getSimpleName() + " cannot be null");
        }
        this.dataFieldReader = dataFieldReader;
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
    public synchronized Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
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

        StartAt startAtToUse = startAt.get(new SubscriptionModelContext(InMemorySubscriptionModel.class));
        if (startAtToUse == null || (!startAtToUse.isNow() && !startAtToUse.isDefault())) {
            throw new IllegalArgumentException(InMemorySubscriptionModel.class.getSimpleName() + " only supports starting from 'now' and 'default' (StartAt.now() or StartAt.subscriptionModelDefault())");
        }

        final Predicate<CloudEvent> matcher = SubscriptionFilterMatcher.matcherFor(filter, dataFieldReader);

        InMemorySubscription subscription = new InMemorySubscription(subscriptionId, queueSupplier.get(), action, matcher, retryStrategy);
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
    public void accept(List<CloudEvent> cloudEvents) {
        if (!running) {
            return;
        }
        subscriptions.values().forEach(subscription -> {
            if (isRunning(subscription.id())) {
                cloudEvents.stream()
                        .filter(subscription::matches)
                        .forEach(subscription::eventAvailable);
            }
        });
    }

    /**
     * Block until every subscription has handled the events fed to it so far, so a test can write events and then
     * assert on the read model without polling the assertion.
     * <p>
     * A subscription is done when its queue is empty and it is not inside a handler. Pausing does not exclude a
     * subscription from the wait: pausing only stops {@code accept(...)} from queueing anything new, and the
     * subscription's own thread keeps draining whatever was already queued, so a backlog from before the pause still
     * finishes and is still worth waiting for. A handler that keeps throwing is retried by the subscription's
     * {@code RetryStrategy}, and this waits for those retries, which is why it takes a timeout.
     * <p>
     * This exists because delivery here is asynchronous: {@code accept(...)} queues on the caller's thread and a pool
     * thread runs the handler. It has no equivalent on a change-stream model, where the cursor is unbounded and there
     * is no point at which everything written has arrived.
     *
     * @param timeout How long to wait.
     * @return {@code true} if every subscription finished, {@code false} if the timeout expired first.
     */
    public boolean waitUntilAllEventsProcessed(Duration timeout) {
        Timeout safeTimeout = DurationToTimeoutConverter.convertDurationToTimeout(timeout);
        long timeoutNanos = safeTimeout.timeUnit().toNanos(safeTimeout.timeout());
        // Compare elapsed against the budget rather than now against a precomputed deadline. A large timeout would
        // overflow that deadline to a negative value and report an immediate timeout.
        long start = System.nanoTime();
        while (true) {
            if (allSubscriptionsIdle()) {
                return true;
            }
            if (System.nanoTime() - start >= timeoutNanos) {
                return allSubscriptionsIdle();
            }
            try {
                MILLISECONDS.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    /**
     * Block until every subscription has handled the events fed to it so far, waiting up to 10 seconds.
     *
     * @return {@code true} if every subscription finished, {@code false} if the wait expired first.
     * @see #waitUntilAllEventsProcessed(Duration)
     */
    public boolean waitUntilAllEventsProcessed() {
        return waitUntilAllEventsProcessed(Duration.ofSeconds(10));
    }

    private boolean allSubscriptionsIdle() {
        return subscriptions.values().stream().allMatch(InMemorySubscription::isIdle);
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

    @Override
    public Set<String> subscriptionIds() {
        return Set.copyOf(subscriptions.keySet());
    }

    @Override
    public void stop() {
        running = false;
        subscriptions.values().forEach(subscription -> pausedSubscriptions.put(subscription.id(), true));
    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        running = true;
        if (resumeSubscriptionsAutomatically) {
            pausedSubscriptions.clear();
        }
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

    @Override
    public String toString() {
        return new StringJoiner(", ", InMemorySubscriptionModel.class.getSimpleName() + "[", "]")
                .add("subscriptions=" + subscriptions)
                .add("pausedSubscriptions=" + pausedSubscriptions)
                .add("cloudEventDispatcher=" + cloudEventDispatcher)
                .add("retryStrategy=" + retryStrategy)
                .add("queueSupplier=" + queueSupplier)
                .add("shutdown=" + shutdown)
                .add("running=" + running)
                .toString();
    }
}
