package org.occurrent.retry;

import java.time.Duration;

/**
 * Contains useful information on the state of the retry
 */
public interface RetryInfo {
    /**
     * @return The count of this retry
     */
    int getRetryCount();

    /**
     * @return The number of attempts to call the method that may be retried
     */
    int getNumberOfAttempts();

    /**
     * @return The maximum number of attempts configured for the retry. Returns {@code Integer.MAX_VALUE} if infinite.
     */
    int getMaxAttempts();

    /**
     * @return The number of attempts left before giving up
     */
    int getAttemptsLeft();

    /**
     * @return {@code true} if there are infinite retry attempts left, {@code false} otherwise.
     */
    boolean isInfiniteRetriesLeft();

    /**
     * @return The backoff duration before retrying again.
     */
    Duration getBackoff();

    /**
     * @return {@code true} if the next attempt is the last attempt, {@code false} otherwise.
     */
    boolean isLastAttempt();

    /**
     * @return {@code true} if this attempt was the first attempt, {@code false} otherwise.
     */
    boolean isFirstAttempt();
}
