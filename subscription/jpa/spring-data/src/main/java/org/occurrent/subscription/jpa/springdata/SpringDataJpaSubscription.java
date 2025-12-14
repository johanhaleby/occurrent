package org.occurrent.subscription.jpa.springdata;

import io.cloudevents.CloudEvent;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.Subscription;
import org.springframework.lang.NonNull;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class SpringDataJpaSubscription implements Subscription {

    private final String itsSubscriptionId;
    private final SubscriptionFilter itsFilter;
    private final Consumer<CloudEvent> itsAction;
    private final AtomicBoolean itsExecuting;

    public SpringDataJpaSubscription(String subscriptionId, SubscriptionFilter filter, Consumer<CloudEvent> action) {
        itsSubscriptionId = subscriptionId;
        itsFilter = filter;
        itsAction = action;
        itsExecuting = new AtomicBoolean(false);
    }

    @Override
    @NonNull
    public String id() {
        return itsSubscriptionId;
    }

    @Override
    public boolean waitUntilStarted(Duration timeout) {
        return true;
    }

    public void accept(CloudEvent event) {
        if (!itsExecuting.compareAndSet(false, true)){
            throw new SubscriptionAlreadyExecuting(itsSubscriptionId);
        }
        try {
            itsAction.accept(event);
        } finally {
            itsExecuting.set(false);
        }
    }
}
