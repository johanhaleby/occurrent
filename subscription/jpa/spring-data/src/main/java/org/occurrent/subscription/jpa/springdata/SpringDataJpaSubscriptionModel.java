package org.occurrent.subscription.jpa.springdata;

import io.cloudevents.CloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionPosition;
import org.occurrent.subscription.api.blocking.PositionAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;

import java.util.function.Consumer;

public class SpringDataJpaSubscriptionModel implements PositionAwareSubscriptionModel {
    @Override
    public SubscriptionPosition globalSubscriptionPosition() {
        return null;
    }

    @Override
    public Subscription subscribe(String subscriptionId, SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        return null;
    }

    @Override
    public void stop() {

    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {

    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return false;
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return false;
    }

    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        return null;
    }

    @Override
    public void pauseSubscription(String subscriptionId) {

    }

    @Override
    public void cancelSubscription(String subscriptionId) {

    }
}
