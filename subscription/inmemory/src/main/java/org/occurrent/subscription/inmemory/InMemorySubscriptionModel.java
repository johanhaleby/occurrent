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
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.DurationToTimeoutConverter.Timeout;
import org.occurrent.subscription.DurationToTimeoutConverter;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionFilterMatcher;
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions;
import org.occurrent.subscription.api.blocking.SubscriptionHandle;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.internal.ExecutorShutdown;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * An in-memory subscription model
 */
@NullMarked
public class InMemorySubscriptionModel implements SubscriptionModel, IntrospectableSubscriptions, Consumer<List<CloudEvent>> {

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
     * Create a new {@link InMemorySubscriptionModel} with the same defaults as {@link #InMemorySubscriptionModel()},
     * that can also answer a subscription filter on a {@code data} payload field by reading it through
     * {@code dataFieldReader}. Occurrent ships a Jackson-backed one in
     * {@code occurrent-common-inmemory-filter-matching-jackson}.
     */
    public InMemorySubscriptionModel(DataFieldReader dataFieldReader) {
        this(Executors.newCachedThreadPool(), RetryStrategy.fixed(200), LinkedBlockingQueue::new, dataFieldReader);
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
    public synchronized SubscriptionHandle subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        if (shutdown) {
            throw new IllegalStateException("Cannot subscribe when shutdown");
        } else if (subscriptionId == null) {
            throw new IllegalArgumentException("subscriptionId cannot be null");
        } else if (action == null) {
            throw new IllegalArgumentException("action cannot be null");
        } else if (isKnown(subscriptionId)) {
            throw new DuplicateSubscriptionIdException(subscriptionId);
        } else if (startAt == null) {
            throw new IllegalArgumentException(StartAt.class.getSimpleName() + " cannot be null");
        }

        StartAt startAtToUse = startAt.get(new SubscriptionModelContext(InMemorySubscriptionModel.class));
        if (startAtToUse == null || (!startAtToUse.isNow() && !startAtToUse.isDefault())) {
            throw new UnsupportedStartAtException(startAt, InMemorySubscriptionModel.class.getSimpleName() + " only supports starting from 'now' and 'default' (StartAt.now() or StartAt.subscriptionModelDefault())");
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
     * <p>
     * Returning normally is the only way this reports success, so the usual "wait, then assert" shape cannot fall
     * through a timeout into the assertion. A timeout throws and names the subscriptions still busy, which is the
     * failure a caller wants to read: "the projection never advanced", not whatever the assertion happened to say
     * about a read model that was never updated.
     *
     * @param timeout How long to wait.
     * @throws IllegalStateException If the timeout expires before every subscription is done, or if the waiting thread
     *                               is interrupted.
     */
    public void waitUntilAllEventsProcessed(Duration timeout) {
        Timeout safeTimeout = DurationToTimeoutConverter.convertDurationToTimeout(timeout);
        long timeoutNanos = safeTimeout.timeUnit().toNanos(safeTimeout.timeout());
        // Compare elapsed against the budget rather than now against a precomputed deadline. A large timeout would
        // overflow that deadline to a negative value and report an immediate timeout.
        long start = System.nanoTime();
        while (true) {
            if (allSubscriptionsIdle()) {
                return;
            }
            if (System.nanoTime() - start >= timeoutNanos) {
                // One read serves as both the final re-check and the message, so a subscription that finished between
                // the two cannot produce a timeout naming nobody.
                List<String> stillBusy = busySubscriptionIds();
                if (stillBusy.isEmpty()) {
                    return;
                }
                throw new IllegalStateException("Timed out after " + timeout + " waiting for "
                        + InMemorySubscriptionModel.class.getSimpleName() + " to process all events. Still processing: "
                        + String.join(", ", stillBusy) + ".");
            }
            try {
                MILLISECONDS.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for " + InMemorySubscriptionModel.class.getSimpleName()
                        + " to process all events.", e);
            }
        }
    }

    /**
     * Block until every subscription has handled the events fed to it so far, waiting up to 10 seconds.
     *
     * @throws IllegalStateException If the wait expires before every subscription is done, or if the waiting thread is
     *                               interrupted.
     * @see #waitUntilAllEventsProcessed(Duration)
     */
    public void waitUntilAllEventsProcessed() {
        waitUntilAllEventsProcessed(Duration.ofSeconds(10));
    }

    private boolean allSubscriptionsIdle() {
        return subscriptions.values().stream().allMatch(InMemorySubscription::isIdle);
    }

    private List<String> busySubscriptionIds() {
        return subscriptions.values().stream().filter(subscription -> !subscription.isIdle()).map(InMemorySubscription::id).sorted().toList();
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
    public SubscriptionHandle resumeSubscription(String subscriptionId) {
        requireKnown(subscriptionId);
        if (!isPaused(subscriptionId)) {
            throw new SubscriptionAlreadyRunningException(subscriptionId);
        }
        running = true;
        pausedSubscriptions.remove(subscriptionId);
        return subscriptions.get(subscriptionId);
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
        requireKnown(subscriptionId);
        if (!isRunning(subscriptionId)) {
            throw new SubscriptionNotRunningException(subscriptionId);
        }
        pausedSubscriptions.put(subscriptionId, true);
    }

    private boolean isKnown(String subscriptionId) {
        return subscriptions.containsKey(subscriptionId) || pausedSubscriptions.containsKey(subscriptionId);
    }

    // Separates "no such subscription here" from "wrong state for this call", which a caller holding several models
    // needs in order to tell "keep looking" from "this is the owner and the answer is no".
    private void requireKnown(String subscriptionId) {
        if (!isKnown(subscriptionId)) {
            throw new UnknownSubscriptionException(subscriptionId);
        }
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
