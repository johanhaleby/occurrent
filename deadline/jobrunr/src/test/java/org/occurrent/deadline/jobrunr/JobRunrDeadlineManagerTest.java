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

import org.jobrunr.configuration.JobRunr;
import org.jobrunr.scheduling.JobRequestScheduler;
import org.jobrunr.server.JobActivator;
import org.jobrunr.storage.InMemoryStorageProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.occurrent.deadline.api.blocking.Deadline;
import org.occurrent.deadline.api.blocking.DeadlineConsumerRegistry;
import org.occurrent.deadline.api.blocking.InMemoryDeadlineConsumerRegistry;
import org.occurrent.deadline.jobrunr.internal.DeadlineJobRequestHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;
import static org.jobrunr.server.BackgroundJobServerConfiguration.usingStandardBackgroundJobServerConfiguration;

class JobRunrDeadlineManagerTest {
    private JobRequestScheduler jobRequestScheduler;
    private JobRunrDeadlineScheduler jobRunrDeadlineScheduler;
    private DeadlineConsumerRegistry deadlineConsumerRegistry;

    @BeforeEach
    void initialize() {
        deadlineConsumerRegistry = new InMemoryDeadlineConsumerRegistry();
        DeadlineJobRequestHandler deadlineJobRequestHandler = new DeadlineJobRequestHandler(deadlineConsumerRegistry);

        Map<Class<?>, Object> beans = new HashMap<>();
        beans.put(DeadlineJobRequestHandler.class, deadlineJobRequestHandler);

        jobRequestScheduler = JobRunr.configure()
                .useStorageProvider(new InMemoryStorageProvider())
                .useJobActivator(new JobActivator() {
                    @Override
                    public <T> T activateJob(Class<T> type) {
                        return (T) beans.get(type);
                    }
                })
                .useBackgroundJobServer(usingStandardBackgroundJobServerConfiguration().andPollIntervalInSeconds(5).andWorkerCount(2))
                .initialize()
                .getJobRequestScheduler();
        jobRunrDeadlineScheduler = new JobRunrDeadlineScheduler(jobRequestScheduler);
    }

    @AfterEach
    void shutdown() {
        jobRequestScheduler.shutdown();
    }

    @Test
    void gd() {
        // Given
        AtomicBoolean completed = new AtomicBoolean(false);
        deadlineConsumerRegistry.register("Something", (id, category, deadline, data) -> {
            System.out.printf("id=%s, category=%s, deadline=%s, data=%s%n", id, category, deadline, data);
            completed.set(true);
        });

        // When
        jobRunrDeadlineScheduler.schedule(UUID.randomUUID(), "Something", Deadline.afterMillis(500), new MyDTO("something"));

        // Then
        await().untilTrue(completed);
    }

    static class MyDTO {
        private String something;

        @SuppressWarnings("unused")
        MyDTO() {
        }

        MyDTO(String something) {
            this.something = something;
        }

        public String getSomething() {
            return something;
        }

        public void setSomething(String something) {
            this.something = something;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", MyDTO.class.getSimpleName() + "[", "]")
                    .add("something='" + something + "'")
                    .toString();
        }
    }
}