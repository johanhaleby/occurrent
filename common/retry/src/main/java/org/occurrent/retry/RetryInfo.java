package org.occurrent.retry;

import java.time.Duration;

public interface RetryInfo {
    int getRetryCount();

    int getNumberOfAttempts();

    int getMaxAttempts();

    int getAttemptsLeft();

    boolean isInfiniteRetriesLeft();

    Duration getBackoff();

    boolean isLastAttempt();

    boolean isFirstAttempt();
}
