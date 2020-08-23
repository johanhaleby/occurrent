package org.occurrent.example.eventstore.mongodb.spring.subscriptionprojections;

import io.cloudevents.CloudEvent;
import org.springframework.stereotype.Component;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.subscription.api.blocking.BlockingSubscription;

import javax.annotation.PostConstruct;
import java.time.Duration;

import static io.vavr.API.*;
import static io.vavr.Predicates.instanceOf;
import static java.time.temporal.ChronoUnit.SECONDS;

@Component
public class CurrentNameProjectionUpdater {

    private final BlockingSubscription<CloudEvent> subscription;
    private final CurrentNameProjection currentNameProjection;
    private final DeserializeCloudEventToDomainEvent deserializeCloudEventToDomainEvent;

    public CurrentNameProjectionUpdater(BlockingSubscription<CloudEvent> subscription,
                                        CurrentNameProjection currentNameProjection,
                                        DeserializeCloudEventToDomainEvent deserializeCloudEventToDomainEvent) {
        this.subscription = subscription;
        this.currentNameProjection = currentNameProjection;
        this.deserializeCloudEventToDomainEvent = deserializeCloudEventToDomainEvent;
    }

    @PostConstruct
    void startProjectionUpdater() {
        subscription
                .subscribe("current-name", cloudEvent -> {
                    DomainEvent domainEvent = deserializeCloudEventToDomainEvent.deserialize(cloudEvent);
                    String eventId = cloudEvent.getId();
                    CurrentName currentName = Match(domainEvent).of(
                            Case($(instanceOf(NameDefined.class)), e -> new CurrentName(eventId, e.getName())),
                            Case($(instanceOf(NameWasChanged.class)), e -> new CurrentName(eventId, e.getName())));
                    currentNameProjection.save(currentName);
                })
                .waitUntilStarted(Duration.of(2, SECONDS));
    }
}