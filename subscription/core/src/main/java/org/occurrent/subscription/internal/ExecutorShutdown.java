package org.occurrent.subscription.internal;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Utility functions for shutting down executor services.
 */
public class ExecutorShutdown {

    /**
     * Blocks until all tasks have completed execution after a shutdown request, or the timeout occurs, or the current thread is interrupted, whichever happens first.
     *
     * @param executorService The executor service
     * @param timeout         the maximum time to wait
     * @param unit            the time unit of the timeout argument
     */
    public static void shutdownSafely(ExecutorService executorService, long timeout, TimeUnit unit) {
        if (!executorService.isShutdown() && !executorService.isTerminated()) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(timeout, unit)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                if (!executorService.isTerminated()) {
                    executorService.shutdownNow();
                }
                Thread.currentThread().interrupt();
            }
        }
    }
}
