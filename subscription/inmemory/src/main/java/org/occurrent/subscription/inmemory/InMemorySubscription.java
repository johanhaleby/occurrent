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
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.DurationToTimeoutConverter;
import org.occurrent.subscription.DurationToTimeoutConverter.Timeout;
import org.occurrent.subscription.api.blocking.Subscription;

import java.time.Duration;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.occurrent.retry.internal.RetryExecution.executeWithRetry;

/**
 * An in-memory subscription
 */
@NullMarked
public class InMemorySubscription implements Subscription, Runnable {
    private final String id;
    private final BlockingQueue<CloudEvent> queue;
    private final Consumer<CloudEvent> consumer;
    private final Predicate<CloudEvent> matcher;
    private final RetryStrategy retryStrategy;

    private volatile boolean shutdown;

    private final CountDownLatch started = new CountDownLatch(1);

    /** Events queued but not yet handled, including the one currently in the handler. */
    private final AtomicInteger outstanding = new AtomicInteger();

    InMemorySubscription(String id, BlockingQueue<CloudEvent> queue, Consumer<CloudEvent> consumer, Predicate<CloudEvent> matcher, RetryStrategy retryStrategy) {
        this.id = id;
        this.queue = queue;
        this.consumer = consumer;
        this.matcher = matcher;
        this.retryStrategy = retryStrategy;
        this.shutdown = false;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public boolean waitUntilStarted(Duration timeout) {
        Timeout safeTimeout = DurationToTimeoutConverter.convertDurationToTimeout(timeout);
        try {
            return started.await(safeTimeout.timeout(), safeTimeout.timeUnit());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (!(o instanceof InMemorySubscription that)) return false;
        return shutdown == that.shutdown && Objects.equals(id, that.id) && Objects.equals(queue, that.queue) && Objects.equals(consumer, that.consumer) && Objects.equals(matcher, that.matcher) && Objects.equals(retryStrategy, that.retryStrategy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, queue, consumer, matcher, retryStrategy, shutdown);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", InMemorySubscription.class.getSimpleName() + "[", "]")
                .add("id='" + id + "'")
                .add("queue=" + queue)
                .add("consumer=" + consumer)
                .add("matcher=" + matcher)
                .add("retryStrategy=" + retryStrategy)
                .add("shutdown=" + shutdown)
                .toString();
    }

    void eventAvailable(CloudEvent cloudEvent) {
        // Counted before the event is visible to the consumer thread, otherwise it could be polled and handled before
        // the count went up, and the count would drop below zero.
        outstanding.incrementAndGet();
        if (!queue.offer(cloudEvent)) {
            outstanding.decrementAndGet();
        }
    }

    /**
     * Whether this subscription has nothing left to do. Counting outstanding events rather than reading the queue is
     * what makes this exact: {@link #run()} takes an event off the queue and only then calls the handler, so an event
     * being handled right now is in neither the queue nor any flag set afterwards.
     */
    boolean isIdle() {
        return outstanding.get() == 0;
    }

    void shutdown() {
        shutdown = true;
    }

    boolean matches(CloudEvent cloudEvent) {
        return matcher.test(cloudEvent);
    }

    @Override
    public void run() {
        started.countDown();
        while (!shutdown) {
            CloudEvent cloudEvent;
            try {
                cloudEvent = queue.poll(500, MILLISECONDS);
            } catch (InterruptedException e) {
                continue;
            }

            if (cloudEvent != null) {
                try {
                    executeWithRetry(consumer, __ -> !shutdown, retryStrategy).accept(cloudEvent);
                } finally {
                    // In a finally so a handler that exhausts its retries and throws cannot leave the count stuck,
                    // which would hang waitUntilAllEventsProcessed forever.
                    outstanding.decrementAndGet();
                }
            }
        }
    }
}