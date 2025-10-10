/*
 *
 *  Copyright 2023 Johan Haleby
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

package org.occurrent.retry;

import org.jspecify.annotations.NullMarked;

import java.time.Duration;
import java.util.Optional;

/**
 * Contains useful information of the state of the retry after a retry attempt
 */
@NullMarked
public interface AfterRetryInfo extends RetryInfo {

    /**
     * @return {@code true} if the retry attempt was successful and no more retry will be made, {@code false} otherwise.
     * @see #getResultOfRetryAttempt()
     */
    default boolean wasSuccessfulRetryAttempt() {
        return getResultOfRetryAttempt() instanceof ResultOfRetryAttempt.Success;
    }

    /**
     * @return {@code true} if the retry attempt failed with an exception, {@code false} otherwise.
     * @see #getResultOfRetryAttempt()
     */
    default boolean wasFailedRetryAttempt() {
        return getResultOfRetryAttempt() instanceof ResultOfRetryAttempt.Failed;
    }

    /**
     * @return The exception (throwable) of the retry attempt if it failed, otherwise the {@code Optional} will be {@code empty}.
     * @see #getResultOfRetryAttempt()
     */
    default Optional<Throwable> getFailedRetryAttemptException() {
        if (wasSuccessfulRetryAttempt()) {
            return Optional.empty();
        } else {
            return Optional.of(((ResultOfRetryAttempt.Failed) getResultOfRetryAttempt()).error());
        }
    }

    /**
     * @return An {@code Optional} containing how long to wait before the next retry attempt kicks in case the retry attempt failed and there additional retries left, or an {@code empty} {@code Optional}.
     */
    Optional<Duration> getBackoffBeforeNextRetryAttempt();

    /**
     * @return The result of the retry attempt
     */
    ResultOfRetryAttempt getResultOfRetryAttempt();

    /**
     * The result of the retry attempt
     */
    sealed interface ResultOfRetryAttempt {
        /**
         * The retry attempt was successful
         */
        record Success() implements ResultOfRetryAttempt {
        }

        /**
         * @param error The error of the retry attempt
         */
        record Failed(Throwable error) implements ResultOfRetryAttempt {
        }
    }
}