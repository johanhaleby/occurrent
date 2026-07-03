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

package org.occurrent.eventstore.mongodb.migration.positionbackfill;

import org.jspecify.annotations.NullMarked;

/**
 * Configures the throttling, batching and safety margin of a {@link PositionBackfill} run.
 *
 * @param batchSize        Number of events read, positioned and written per batch. A smaller batch size lowers the
 *                          write-lock and memory footprint per iteration at the cost of more round-trips.
 * @param throttleMillis   Milliseconds to sleep between batches, so the backfill does not compete with production
 *                          traffic for write capacity. {@code 0} disables throttling.
 * @param counterSeedSlack Extra positions reserved above the accurate historical event count when seeding the
 *                          position counter (step 1). This absorbs events written concurrently with the count, and
 *                          any stream/DCB events appended between the count and the counter seed, so live writes
 *                          after deploy are guaranteed to land above every position the backfill will assign.
 */
@NullMarked
public record PositionBackfillOptions(int batchSize, long throttleMillis, long counterSeedSlack) {

    private static final int DEFAULT_BATCH_SIZE = 500;
    private static final long DEFAULT_THROTTLE_MILLIS = 0;
    private static final long DEFAULT_COUNTER_SEED_SLACK = 10_000;

    public PositionBackfillOptions {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than 0");
        }
        if (throttleMillis < 0) {
            throw new IllegalArgumentException("throttleMillis cannot be negative");
        }
        if (counterSeedSlack < 0) {
            throw new IllegalArgumentException("counterSeedSlack cannot be negative");
        }
    }

    /**
     * Sensible defaults for a moderately sized deployment: batches of {@value #DEFAULT_BATCH_SIZE} events, no
     * throttling, and {@value #DEFAULT_COUNTER_SEED_SLACK} positions of counter seed slack.
     */
    public static PositionBackfillOptions defaults() {
        return new PositionBackfillOptions(DEFAULT_BATCH_SIZE, DEFAULT_THROTTLE_MILLIS, DEFAULT_COUNTER_SEED_SLACK);
    }

    public PositionBackfillOptions withBatchSize(int batchSize) {
        return new PositionBackfillOptions(batchSize, throttleMillis, counterSeedSlack);
    }

    public PositionBackfillOptions withThrottleMillis(long throttleMillis) {
        return new PositionBackfillOptions(batchSize, throttleMillis, counterSeedSlack);
    }

    public PositionBackfillOptions withCounterSeedSlack(long counterSeedSlack) {
        return new PositionBackfillOptions(batchSize, throttleMillis, counterSeedSlack);
    }
}
