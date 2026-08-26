/*
 * Copyright 2026 Johan Haleby
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


package org.occurrent.eventstore.mongodb.migration.updateeventrepair;

import org.jspecify.annotations.NullMarked;

/**
 * Configures the batching, throttling and reporting of an {@link UpdateEventRepair} run.
 *
 * @param batchSize                 Number of events read and repaired per batch. A smaller batch lowers the memory
 *                                  and write-lock cost per iteration at the cost of more round-trips.
 * @param throttleMillis            Milliseconds to sleep between batches, so a repair does not compete with
 *                                  production traffic. {@code 0} disables throttling.
 * @param maxReportedUnrecoverable  How many {@link UnrecoverableEvent} findings {@link UpdateEventRepairResult}
 *                                  keeps. This bounds findings rather than events, since one event can produce more
 *                                  than one. The result's count of events is always complete and every finding is
 *                                  logged at WARN as it is found, so a badly damaged collection cannot exhaust memory
 *                                  through this list.
 */
@NullMarked
public record UpdateEventRepairOptions(int batchSize, long throttleMillis, int maxReportedUnrecoverable) {

    private static final int DEFAULT_BATCH_SIZE = 500;
    private static final long DEFAULT_THROTTLE_MILLIS = 0;
    private static final int DEFAULT_MAX_REPORTED_UNRECOVERABLE = 1_000;

    public UpdateEventRepairOptions {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than 0");
        }
        if (throttleMillis < 0) {
            throw new IllegalArgumentException("throttleMillis cannot be negative");
        }
        if (maxReportedUnrecoverable < 0) {
            throw new IllegalArgumentException("maxReportedUnrecoverable cannot be negative");
        }
    }

    /**
     * Sensible defaults for a moderately sized deployment, meaning batches of {@value #DEFAULT_BATCH_SIZE} events, no
     * throttling, and up to {@value #DEFAULT_MAX_REPORTED_UNRECOVERABLE} unrecoverable findings kept in the result.
     */
    public static UpdateEventRepairOptions defaults() {
        return new UpdateEventRepairOptions(DEFAULT_BATCH_SIZE, DEFAULT_THROTTLE_MILLIS, DEFAULT_MAX_REPORTED_UNRECOVERABLE);
    }

    public UpdateEventRepairOptions withBatchSize(int batchSize) {
        return new UpdateEventRepairOptions(batchSize, throttleMillis, maxReportedUnrecoverable);
    }

    public UpdateEventRepairOptions withThrottleMillis(long throttleMillis) {
        return new UpdateEventRepairOptions(batchSize, throttleMillis, maxReportedUnrecoverable);
    }

    public UpdateEventRepairOptions withMaxReportedUnrecoverable(int maxReportedUnrecoverable) {
        return new UpdateEventRepairOptions(batchSize, throttleMillis, maxReportedUnrecoverable);
    }
}
