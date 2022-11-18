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

package org.occurrent.deadline.jobrunr.internal;

import org.jobrunr.jobs.lambdas.JobRequest;
import org.jobrunr.jobs.lambdas.JobRequestHandler;
import org.occurrent.deadline.api.blocking.DeadlineConsumerRegistry;
import org.occurrent.deadline.jobrunr.JobRunrDeadlineConsumerRegistry;

import java.util.Objects;
import java.util.StringJoiner;

public class DeadlineJobRequest implements JobRequest {
    private String id;
    private String category;
    private long deadlineInEpochMilli;
    private Object data;

    @SuppressWarnings("unused")
    DeadlineJobRequest() {
    }

    public DeadlineJobRequest(String id, String category, long deadlineInEpochMilli, Object data) {
        this.id = id;
        this.category = category;
        this.deadlineInEpochMilli = deadlineInEpochMilli;
        this.data = data;
    }

    @Override
    public Class<? extends JobRequestHandler> getJobRequestHandler() {
        return JobRunrDeadlineConsumerRegistry.class;
    }

    public String getId() {
        return id;
    }

    public String getCategory() {
        return category;
    }

    public long getDeadlineInEpochMilli() {
        return deadlineInEpochMilli;
    }

    public Object getData() {
        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DeadlineJobRequest)) return false;
        DeadlineJobRequest that = (DeadlineJobRequest) o;
        return deadlineInEpochMilli == that.deadlineInEpochMilli && Objects.equals(id, that.id) && Objects.equals(category, that.category) && Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, category, deadlineInEpochMilli, data);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DeadlineJobRequest.class.getSimpleName() + "[", "]")
                .add("id='" + id + "'")
                .add("category='" + category + "'")
                .add("deadlineInEpochMilli=" + deadlineInEpochMilli)
                .add("data=" + data)
                .toString();
    }
}
