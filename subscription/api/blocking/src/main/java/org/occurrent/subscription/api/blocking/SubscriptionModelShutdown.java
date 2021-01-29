package org.occurrent.subscription.api.blocking;

public interface SubscriptionModelShutdown {
    /**
     * Shutdown the subscription model and close all subscriptions (they can be resumed later if you start from a durable subscription position).
     * A subscription model that is shutdown cannot be started again, since it closes resources such as database connections,
     * thread pools etc.
     */
    void shutdown();
}
