package org.occurrent.subscription.jpa.springdata;

import io.cloudevents.CloudEvent;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.stereotype.Component;

@Component
public class DomainEventManagement {

    private final SpringDataJpaSubscriptionModel itsModel;

    public DomainEventManagement(
            SpringDataJpaSubscriptionModel itsModel) {
        this.itsModel = itsModel;
    }

    @ApplicationModuleListener
    public void on(CloudEvent aCloudEvent) {
        // Spread out, one per subscriber.
        itsModel.spreadOut(aCloudEvent);
    }

    @ApplicationModuleListener
    public void on(SubscriptionEvent aEvent) {
        if (itsModel.isPaused(aEvent.subscriptionId())) {
            // don't acknowledge the event!
            throw new SubscriptionPausedEvent(aEvent.subscriptionId());
        }
        itsModel.process(aEvent.subscriptionId(), aEvent.event());
    }

}
