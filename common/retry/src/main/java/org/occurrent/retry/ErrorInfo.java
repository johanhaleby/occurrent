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
 * Contains useful information of the state of the error
 */
@NullMarked
public interface ErrorInfo extends RetryInfo {

    /**
     * @return An {@code Optional} containing how long to wait before the next retry attempt kicks in case there additional retries left, or an {@code empty} {@code Optional}.
     */
    Optional<Duration> getBackoffBeforeNextRetryAttempt();

    /**
     * @return {@code true} if the error is retryable, {@code false} otherwise.
     */
    boolean isRetryable();
}