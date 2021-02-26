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
import org.occurrent.subscription.api.blocking.Subscription;

import java.time.Duration;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.occurrent.inmemory.filtermatching.FilterMatcher.matchesFilter;
import static org.occurrent.retry.internal.RetryExecution.executeWithRetry;

/**
 * An in-memory subscription
 */
public class InMemorySubscription implements Subscription, Runnable {
    private final String id;
    private final BlockingQueue<CloudEvent> queue;
    private final Consumer<CloudEvent> consumer;
    private final Filter filter;
    private final RetryStrategy retryStrategy;

    private volatile boolean shutdown;

    private final CountDownLatch started = new CountDownLatch(1);

    InMemorySubscription(String id, BlockingQueue<CloudEvent> queue, Consumer<CloudEvent> consumer, Filter filter, RetryStrategy retryStrategy) {
        this.id = id;
        this.queue = queue;
        this.consumer = consumer;
        this.filter = filter;
        this.retryStrategy = retryStrategy;
        this.shutdown = false;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public void waitUntilStarted() {
        try {
            started.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean waitUntilStarted(Duration timeout) {
        try {
            return started.await(timeout.toMillis(), MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof InMemorySubscription)) return false;
        InMemorySubscription that = (InMemorySubscription) o;
        return shutdown == that.shutdown && Objects.equals(id, that.id) && Objects.equals(queue, that.queue) && Objects.equals(consumer, that.consumer) && Objects.equals(filter, that.filter) && Objects.equals(retryStrategy, that.retryStrategy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, queue, consumer, filter, retryStrategy, shutdown);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", InMemorySubscription.class.getSimpleName() + "[", "]")
                .add("id='" + id + "'")
                .add("queue=" + queue)
                .add("consumer=" + consumer)
                .add("filter=" + filter)
                .add("retryStrategy=" + retryStrategy)
                .add("shutdown=" + shutdown)
                .toString();
    }

    void eventAvailable(CloudEvent cloudEvent) {
        queue.offer(cloudEvent);
    }

    void shutdown() {
        shutdown = true;
    }

    boolean matches(CloudEvent cloudEvent) {
        return matchesFilter(cloudEvent, filter);
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
                executeWithRetry(consumer, __ -> !shutdown, retryStrategy).accept(cloudEvent);
            }
        }
    }
}