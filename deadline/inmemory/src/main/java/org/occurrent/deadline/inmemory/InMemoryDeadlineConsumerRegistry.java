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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
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
        thread = new Thread(() -> {
            while (running) {
                try {
                    DeadlineData data = deadlineQueue.pollFirst(500, TimeUnit.MILLISECONDS);
                    if (data != null) {
                        DeadlineConsumer<Object> deadlineConsumer = deadlineConsumers.get(data.category);
                        if (deadlineConsumer == null) {
                            log.warn("Failed to find a deadline consumer for category {}, will try again later.", data.category);
                        } else {
                            deadlineConsumer.accept(data.id, data.category, data.deadline, data.data);
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
}