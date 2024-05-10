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

import org.jobrunr.scheduling.JobRequestScheduler;
import org.occurrent.deadline.api.blocking.Deadline;
import org.occurrent.deadline.api.blocking.DeadlineScheduler;
import org.occurrent.deadline.jobrunr.internal.DeadlineJobRequest;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/**
 * <a href="https://www.jobrunr.io">JobRunr</a> deadline scheduler implementation.
 */
public class JobRunrDeadlineScheduler implements DeadlineScheduler {
    private final JobRequestScheduler jobRequestScheduler;


    public JobRunrDeadlineScheduler(JobRequestScheduler jobRequestScheduler) {
        Objects.requireNonNull(jobRequestScheduler, JobRequestScheduler.class.getSimpleName() + " cannot be null");
        this.jobRequestScheduler = jobRequestScheduler;
    }

    @Override
    public void schedule(String id, String category, Deadline deadline, Object data) {
        long epochMilli = deadline.toEpochMilli();
        jobRequestScheduler.schedule(generateUUIDFromString(id), deadline.toInstant(), new DeadlineJobRequest(id, category, epochMilli, data));
    }

    @Override
    public void cancel(String id) {
        jobRequestScheduler.delete(generateUUIDFromString(id));
    }

    private static UUID generateUUIDFromString(String id) {
        Objects.requireNonNull(id, "id cannot be null");
        try {
            return UUID.fromString(id);
        } catch (IllegalArgumentException e) {
            return UUID.nameUUIDFromBytes(id.getBytes(StandardCharsets.UTF_8));
        }
    }
}