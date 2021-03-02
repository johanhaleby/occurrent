package org.occurrent.subscription.blocking.competingconsumers;

import org.occurrent.subscription.api.blocking.Subscription;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;


public class CompetingConsumerSubscription implements Subscription {
    private final String subscriptionId;
    private final String subscriberId;
    private final Subscription subscription;

    CompetingConsumerSubscription(String subscriptionId, String subscriberId) {
        this(subscriptionId, subscriberId, null);
    }

    CompetingConsumerSubscription(String subscriptionId, String subscriberId, Subscription subscription) {
        this.subscriptionId = subscriptionId;
        this.subscriberId = subscriberId;
        this.subscription = subscription;
    }

    String getSubscriberId() {
        return subscriberId;
    }

    Optional<Subscription> getSubscription() {
        return Optional.ofNullable(subscription);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CompetingConsumerSubscription)) return false;
        CompetingConsumerSubscription that = (CompetingConsumerSubscription) o;
        return Objects.equals(subscriptionId, that.subscriptionId) && Objects.equals(subscriberId, that.subscriberId) && Objects.equals(subscription, that.subscription);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subscriptionId, subscriberId, subscription);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", CompetingConsumerSubscription.class.getSimpleName() + "[", "]")
                .add("subscriptionId='" + subscriptionId + "'")
                .add("subscriberId='" + subscriberId + "'")
                .add("subscription=" + subscription)
                .toString();
    }

    @Override
    public String id() {
        return subscriptionId;
    }

    @Override
    public void waitUntilStarted() {
        getSubscription().ifPresent(Subscription::waitUntilStarted);
    }

    @Override
    public boolean waitUntilStarted(Duration timeout) {
        return getSubscription().map(s -> s.waitUntilStarted(timeout)).orElse(true);
    }
}
