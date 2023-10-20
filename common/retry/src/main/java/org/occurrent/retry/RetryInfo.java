package org.occurrent.retry;

import java.time.Duration;

/**
 * Contains useful information on the state of the retry
 */
public interface RetryInfo {
    /**
     * @return The count of the <i>current</i> retry, {@code 0} if first <i>attempt</i>, but <i>1</i> for the first <i>retry attempt</i>.
     */
    int getRetryCount();

    /**
     * @return The number of attempts that <i>has been made</i>, {@code 0} if first attempt
     */
    default int getNumberOfPreviousAttempts() {
        return getAttemptNumber() - 1;
    }

    /**
     * @return The number of <i>this</i> attempt, {@code 1} if first attempt.
     */
    int getAttemptNumber();

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
