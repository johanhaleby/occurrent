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

import com.mongodb.MongoException;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.MongoOperationTimeoutException;

import java.time.Duration;

/**
 * When a transaction commit may be retried, decided the way the MongoDB driver's own {@code ClientSession.withTransaction}
 * decides it. Don't depend on this class since it's supposed to be internal!
 */
public final class UnknownCommitResult {

    /**
     * How long the driver keeps retrying a commit, measured from when the first commit was sent.
     */
    public static final Duration RETRY_LIMIT = Duration.ofMillis(120_000);

    private UnknownCommitResult() {
    }

    /**
     * @return {@code true} if the failure is a {@link MongoException} that carries the {@code UnknownTransactionCommitResult}
     * label, and is not a {@code MaxTimeMSExpired} ({@link MongoExecutionTimeoutException}) or a
     * {@link MongoOperationTimeoutException}. The driver retries a commit that fails that way and no other.
     */
    public static boolean isRetryable(Throwable failure) {
        // Bounded walk so a cyclic cause chain cannot spin forever.
        Throwable cause = failure;
        for (int hops = 0; cause != null && hops < 64; cause = cause.getCause(), hops++) {
            if (cause instanceof MongoException mongoException) {
                return mongoException.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)
                        && !(mongoException instanceof MongoExecutionTimeoutException)
                        && !(mongoException instanceof MongoOperationTimeoutException);
            }
        }
        return false;
    }
}
