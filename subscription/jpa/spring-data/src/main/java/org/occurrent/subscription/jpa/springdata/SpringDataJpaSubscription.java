package org.occurrent.subscription.jpa.springdata;

import org.occurrent.subscription.api.blocking.Subscription;

import java.time.Duration;

public class SpringDataJpaSubscription implements Subscription {
    @Override
    public String id() {
        return "";
    }

    @Override
    public boolean waitUntilStarted(Duration timeout) {
        return true;
    }
}
