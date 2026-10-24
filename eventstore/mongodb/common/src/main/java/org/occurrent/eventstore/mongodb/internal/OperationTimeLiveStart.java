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

package org.occurrent.eventstore.mongodb.internal;

import org.bson.BsonTimestamp;
import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Internal, not part of the public API. The one place that builds and reads the string a MongoDB event store reports as
 * a catch-up's live start, a JSON document holding an {@code operationTime}. The event stores build it, and the MongoDB
 * subscription models read it, in separate modules. A catch-up stores it as a checkpoint, so the format must not change
 * between versions.
 */
@NullMarked
public final class OperationTimeLiveStart {

    /**
     * The field of the JSON document that holds the operation time.
     */
    public static final String OPERATION_TIME = "operationTime";

    private OperationTimeLiveStart() {
    }

    /**
     * The string form of {@code operationTime}.
     */
    public static String format(BsonTimestamp operationTime) {
        requireNonNull(operationTime, "operationTime cannot be null");
        return new Document(OPERATION_TIME, operationTime).toJson();
    }

    /**
     * The string form of the operation time one increment after {@code operationTime}, which a change stream opened with
     * {@code startAtOperationTime} starts after.
     */
    public static String formatOneIncrementAfter(BsonTimestamp operationTime) {
        requireNonNull(operationTime, "operationTime cannot be null");
        return format(oneIncrementAfter(operationTime));
    }

    /**
     * The live start of a catch-up whose read of the position counter returned {@code operationTime}, one increment
     * after it. MongoDB reports no operation time outside a replica set, where a change stream can't run either, so a
     * missing one fails the catch-up instead of letting it take a live start the replay reads aren't consistent with.
     *
     * @throws IllegalStateException if {@code operationTime} is null
     */
    public static String liveStartAfterRead(@Nullable BsonTimestamp operationTime) {
        if (operationTime == null) {
            throw new IllegalStateException("Cannot run a catch-up subscription, because MongoDB reported no operation time for the catch-up's read of the event store. MongoDB reports one only on a replica set, and a catch-up needs a replica set for its change stream too.");
        }
        return formatOneIncrementAfter(operationTime);
    }

    /**
     * The operation time one increment after {@code operationTime}.
     */
    public static BsonTimestamp oneIncrementAfter(BsonTimestamp operationTime) {
        requireNonNull(operationTime, "operationTime cannot be null");
        return new BsonTimestamp(operationTime.getTime(), operationTime.getInc() + 1);
    }

    /**
     * The operation time in {@code liveStart}, a string {@link #format(BsonTimestamp)} built. Throws what the JSON parser
     * throws when {@code liveStart} isn't JSON, and {@link ClassCastException} when the field isn't a timestamp.
     *
     * @return The operation time, or {@code null} when {@code liveStart} has none
     */
    public static @Nullable BsonTimestamp parse(String liveStart) {
        requireNonNull(liveStart, "liveStart cannot be null");
        return Document.parse(liveStart).get(OPERATION_TIME, BsonTimestamp.class);
    }

    /**
     * The operation time in {@code liveStart}, or empty when it isn't a string {@link #format(BsonTimestamp)} built, for
     * example a resume token or a string another model wrote.
     */
    public static Optional<BsonTimestamp> tryParse(String liveStart) {
        requireNonNull(liveStart, "liveStart cannot be null");
        if (!liveStart.contains(OPERATION_TIME)) {
            return Optional.empty();
        }
        try {
            return Optional.ofNullable(parse(liveStart));
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }
}
