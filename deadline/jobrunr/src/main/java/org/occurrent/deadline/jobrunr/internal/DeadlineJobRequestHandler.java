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

import org.jobrunr.jobs.lambdas.JobRequestHandler;
import org.occurrent.deadline.api.blocking.Deadline;
import org.occurrent.deadline.api.blocking.DeadlineConsumer;
import org.occurrent.deadline.api.blocking.DeadlineConsumerRegistry;

public class DeadlineJobRequestHandler implements JobRequestHandler<DeadlineJobRequest> {
    private final DeadlineConsumerRegistry deadlineConsumerRegistry;

    public DeadlineJobRequestHandler(DeadlineConsumerRegistry deadlineConsumerRegistry) {
        this.deadlineConsumerRegistry = deadlineConsumerRegistry;
    }

    @Override
    public void run(DeadlineJobRequest jobRequest) {
        DeadlineConsumer<Object> deadlineConsumer = deadlineConsumerRegistry.getConsumer(jobRequest.getCategory())
                .orElseThrow(() -> new IllegalStateException(String.format("Failed to find a deadline consumer for category %s (deadlineId=%s)", jobRequest.getCategory(), jobRequest.getId())));
        deadlineConsumer.accept(jobRequest.getId(), jobRequest.getCategory(), Deadline.ofEpochMilli(jobRequest.getDeadlineInEpochMilli()), jobRequest.getData());
    }
}
