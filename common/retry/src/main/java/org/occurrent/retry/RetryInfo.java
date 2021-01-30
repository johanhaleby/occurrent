package org.occurrent.retry;

import java.time.Duration;

public interface RetryInfo {
    int getRetryAttempt();

    int getMaxRetryAttempts();

    int getRetryAttemptsLeft();

    boolean isInfiniteRetriesLeft();

    Duration getBackoff();

    boolean isLastRetryAttempt();

    boolean isFirstRetryAttempt();
}
