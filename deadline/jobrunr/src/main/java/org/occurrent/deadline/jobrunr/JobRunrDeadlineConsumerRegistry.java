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

package org.occurrent.deadline.jobrunr;

import org.jobrunr.jobs.lambdas.JobRequestHandler;
import org.occurrent.deadline.api.blocking.Deadline;
import org.occurrent.deadline.api.blocking.DeadlineConsumer;
import org.occurrent.deadline.api.blocking.DeadlineConsumerRegistry;
import org.occurrent.deadline.jobrunr.internal.DeadlineJobRequest;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * <a href="https://www.jobrunr.io">JobRunr</a> {@link DeadlineConsumerRegistry} implementation.
 */
public class JobRunrDeadlineConsumerRegistry implements DeadlineConsumerRegistry, JobRequestHandler<DeadlineJobRequest> {
    private final ConcurrentMap<String, DeadlineConsumer<Object>> deadlineConsumers = new ConcurrentHashMap<>();

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

    @Override
    public void run(DeadlineJobRequest jobRequest) {
        DeadlineConsumer<Object> deadlineConsumer = Optional.ofNullable(deadlineConsumers.get(jobRequest.getCategory()))
                .orElseThrow(() -> new IllegalStateException(String.format("Failed to find a deadline consumer for category %s (deadlineId=%s)", jobRequest.getCategory(), jobRequest.getId())));
        deadlineConsumer.accept(jobRequest.getId(), jobRequest.getCategory(), Deadline.ofEpochMilli(jobRequest.getDeadlineInEpochMilli()), jobRequest.getData());
    }
}
