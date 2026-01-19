package org.occurrent.subscription.jpa.springdata;

import io.cloudevents.CloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionPosition;
import org.occurrent.subscription.api.blocking.PositionAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * My Entities publish a @DomainEvent through Spring Data JPA's event system.
 */


@Component
public class SpringDataJpaSubscriptionModel implements PositionAwareSubscriptionModel {
    private final Map<String, SpringDataJpaSubscription> itsSubscriptions;
    private final ApplicationEventPublisher itsPublisher;
    private AtomicBoolean itsRunning;

    public SpringDataJpaSubscriptionModel(ApplicationEventPublisher aPublisher) {
        itsRunning = new AtomicBoolean(false);
        itsPublisher = aPublisher;
        itsSubscriptions = new HashMap<>();
    }

    @Override
    public SubscriptionPosition globalSubscriptionPosition() {
        // I like the idea of ... summing up the current version of all the streams ?
        return null;
    }

    @Override
    @NonNull
    public Subscription subscribe(String subscriptionId, SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        var sub = itsSubscriptions.put(subscriptionId,
                new SpringDataJpaSubscription(subscriptionId, filter, action));
        // not current
        if (!startAt.isNow()) {
            // publish all events till now.
        }
        return sub;
    }

    @Override
    public void stop() {
        itsSubscriptions.keySet()
                .forEach(this::pauseSubscription);
        itsRunning.set(false);
    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        itsRunning.set(true);
        if (resumeSubscriptionsAutomatically) {
            itsSubscriptions.keySet()
                    .forEach(this::resumeSubscription);
        }
    }

    @Override
    public boolean isRunning() {
        return itsRunning.get();
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return !itsSubscriptions.get(subscriptionId).isPaused();
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return itsSubscriptions.get(subscriptionId).isPaused();
    }

    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        if (isRunning(subscriptionId)) {
            throw new IllegalArgumentException("Currently running");
        }
        return itsSubscriptions.get(subscriptionId).resume();
        // TODO: re-publish the missing events
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
        if (isPaused(subscriptionId)){
            throw new IllegalArgumentException("is paused already");
        }
        itsSubscriptions.get(subscriptionId).pauseNow();
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        // clear all pending events ?!
        itsSubscriptions.remove(subscriptionId).pauseNow();
    }

    /**
     * An Event was recorded, spread it to all matching subscriptions.
     */
    void spreadOut(CloudEvent aCloudEvent) {
        // Even if currently stopping, we should enqueue the subscriptions,
        // else we'd need a mechanism to re-process those events as well,
        // which is a bit too much for the moment.
        itsSubscriptions.values().stream()
                .filter(s -> s.eventMatches(aCloudEvent))
                .map(s -> new SubscriptionEvent(s.id(), aCloudEvent))
                .forEach(itsPublisher::publishEvent);
    }

    void process(String subscriptionId, CloudEvent event) {
        itsSubscriptions.get(subscriptionId).accept(event);
    }
}
