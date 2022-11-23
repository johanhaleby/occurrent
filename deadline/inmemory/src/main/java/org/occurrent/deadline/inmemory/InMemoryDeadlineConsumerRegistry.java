/*
 *
 *  Copyright 2022 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.deadline.inmemory;

import org.occurrent.deadline.api.blocking.DeadlineConsumer;
import org.occurrent.deadline.api.blocking.DeadlineConsumerRegistry;
import org.occurrent.deadline.inmemory.internal.DeadlineData;
import org.occurrent.retry.RetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class InMemoryDeadlineConsumerRegistry implements DeadlineConsumerRegistry {
    private static final Logger log = LoggerFactory.getLogger(InMemoryDeadlineConsumerRegistry.class);

    private final ConcurrentMap<String, DeadlineConsumer<Object>> deadlineConsumers = new ConcurrentHashMap<>();
    private final Thread thread;
    private volatile boolean running = true;

    public InMemoryDeadlineConsumerRegistry(BlockingDeque<DeadlineData> deadlineQueue) {
        this(deadlineQueue, new Config());
    }

    public InMemoryDeadlineConsumerRegistry(BlockingDeque<DeadlineData> deadlineQueue, Config config) {
        Objects.requireNonNull(deadlineQueue, "Deadline queue cannot be null");
        thread = new Thread(() -> {
            while (running) {
                try {
                    DeadlineData data = deadlineQueue.pollFirst(config.pollInterval, config.pollIntervalTimeUnit);
                    if (data != null) {
                        DeadlineConsumer<Object> deadlineConsumer = deadlineConsumers.get(data.category);
                        if (deadlineConsumer == null) {
                            log.warn("Failed to find a deadline consumer for category {}, will try again later.", data.category);
                        } else {
                            config.retryStrategy.execute(() -> deadlineConsumer.accept(data.id, data.category, data.deadline, data.data));
                        }
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        thread.start();
    }

    @Override
    public DeadlineConsumerRegistry register(String category, DeadlineConsumer<Object> deadlineConsumer) {
        Objects.requireNonNull(category, "category cannot be null");
        Objects.requireNonNull(deadlineConsumer, DeadlineConsumer.class.getSimpleName() + " cannot be null");
        deadlineConsumers.put(category, deadlineConsumer);
        return this;
    }

    @Override
    public DeadlineConsumerRegistry unregister(String category) {
        Objects.requireNonNull(category, "category cannot be null");
        deadlineConsumers.remove(category);
        return this;
    }

    @Override
    public DeadlineConsumerRegistry unregisterAll() {
        deadlineConsumers.clear();
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Optional<DeadlineConsumer<T>> getConsumer(String category) {
        Objects.requireNonNull(category, "category cannot be null");
        return Optional.ofNullable((DeadlineConsumer<T>) deadlineConsumers.get(category));
    }

    public void shutdown() {
        running = false;
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static class Config {
        final long pollInterval;
        final TimeUnit pollIntervalTimeUnit;
        final RetryStrategy retryStrategy;

        /**
         * Create config with the following settings:
         * <ol>
         *     <li>Poll interval - 500 ms</li>
         *     <li>RetryStrategy - Fixed every 1 second</li>
         * </ol>
         */
        public Config() {
            this(500, TimeUnit.MILLISECONDS, RetryStrategy.fixed(Duration.ofSeconds(1)));
        }

        public Config(long pollInterval, TimeUnit pollIntervalTimeUnit, RetryStrategy retryStrategy) {
            if (pollInterval < 1) {
                throw new IllegalArgumentException("pollInterval must be greater than zero");
            }
            Objects.requireNonNull(pollIntervalTimeUnit, "pollIntervalTimeUnit cannot be null");
            Objects.requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
            this.pollInterval = pollInterval;
            this.pollIntervalTimeUnit = pollIntervalTimeUnit;
            this.retryStrategy = retryStrategy;
        }

        public Config pollIntervalMillis(long pollIntervalMillis) {
            return new Config(pollIntervalMillis, TimeUnit.MILLISECONDS, retryStrategy);
        }

        public Config pollInterval(long pollIntervalMillis, TimeUnit timeUnit) {
            return new Config(pollIntervalMillis, timeUnit, retryStrategy);
        }

        public Config retryStrategy(RetryStrategy retryStrategy) {
            return new Config(pollInterval, pollIntervalTimeUnit, retryStrategy);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Config)) return false;
            Config config = (Config) o;
            return pollInterval == config.pollInterval && pollIntervalTimeUnit == config.pollIntervalTimeUnit && Objects.equals(retryStrategy, config.retryStrategy);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pollInterval, pollIntervalTimeUnit, retryStrategy);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", Config.class.getSimpleName() + "[", "]")
                    .add("pollInterval=" + pollInterval)
                    .add("pollIntervalTimeUnit=" + pollIntervalTimeUnit)
                    .add("retryStrategy=" + retryStrategy)
                    .toString();
        }
    }
}