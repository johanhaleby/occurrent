package org.occurrent.subscription.jpa.springdata;

import io.cloudevents.CloudEvent;

record SubscriptionEvent(
        String subscriptionId,
        CloudEvent event
) {
}
