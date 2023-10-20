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

package org.occurrent.retry.internal;

import org.occurrent.retry.MaxAttempts;
import org.occurrent.retry.RetryInfo;

import java.time.Duration;
import java.util.Objects;
import java.util.StringJoiner;

class RetryInfoImpl implements RetryInfo {

    private final int attemptNumber;
    private final int retryCount;
    private final MaxAttempts maxAttempts;
    private final Duration backoff;

    public RetryInfoImpl(int attemptNumber, int retryCount, MaxAttempts maxAttempts, Duration backoff) {
        this.attemptNumber = attemptNumber;
        this.retryCount = retryCount;
        this.maxAttempts = maxAttempts;
        this.backoff = backoff;
    }

    @Override
    public int getRetryCount() {
        return retryCount;
    }

    @Override
    public int getAttemptNumber() {
        return attemptNumber;
    }

    @Override
    public int getMaxAttempts() {
        if (isInfiniteRetriesLeft()) {
            return Integer.MAX_VALUE;
        }
        return ((MaxAttempts.Limit) maxAttempts).limit();
    }

    @Override
    public int getAttemptsLeft() {
        if (isInfiniteRetriesLeft()) {
            return Integer.MAX_VALUE;
        }
        return getMaxAttempts() - getAttemptNumber() + 1;
    }

    @Override
    public boolean isInfiniteRetriesLeft() {
        return maxAttempts instanceof MaxAttempts.Infinite;
    }

    @Override
    public Duration getBackoff() {
        return backoff;
    }

    @Override
    public boolean isLastAttempt() {
        if (isInfiniteRetriesLeft()) {
            return false;
        }
        return getAttemptNumber() == getMaxAttempts();
    }

    @Override
    public boolean isFirstAttempt() {
        return attemptNumber == 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RetryInfoImpl retryInfo)) return false;
        return attemptNumber == retryInfo.attemptNumber && retryCount == retryInfo.retryCount && Objects.equals(maxAttempts, retryInfo.maxAttempts) && Objects.equals(backoff, retryInfo.backoff);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attemptNumber, retryCount, maxAttempts, backoff);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", RetryInfoImpl.class.getSimpleName() + "[", "]")
                .add("attemptNumber=" + attemptNumber)
                .add("retryCount=" + retryCount)
                .add("maxAttempts=" + maxAttempts)
                .add("backoff=" + backoff)
                .toString();
    }

    RetryInfoImpl increaseAttemptsByOne() {
        return new RetryInfoImpl(attemptNumber + 1, retryCount, maxAttempts, backoff);
    }

    RetryInfoImpl withBackoff(Duration backoff) {
        return new RetryInfoImpl(attemptNumber, retryCount, maxAttempts, backoff);
    }
}
