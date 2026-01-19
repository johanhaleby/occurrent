package org.occurrent.subscription.jpa.springdata;

public class SubscriptionAlreadyExecuting extends RuntimeException {
    public SubscriptionAlreadyExecuting(String aSubscriptionId) {
    }
}
