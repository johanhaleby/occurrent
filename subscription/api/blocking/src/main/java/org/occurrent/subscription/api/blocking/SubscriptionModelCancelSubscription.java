package org.occurrent.subscription.api.blocking;

public interface SubscriptionModelCancelSubscription {
    /**
     * Cancel a subscription, this will remove the position from position storage (if used),
     * and you cannot restart it from its current position again.
     */
    void cancelSubscription(String subscriptionId);
}
