package se.haleby.occurrent.example.eventstore.mongodb.spring.subscriptionprojections;

import org.springframework.stereotype.Component;
import se.haleby.occurrent.subscription.mongodb.spring.blocking.SpringBlockingSubscriptionWithPositionPersistenceForMongoDB;
import se.haleby.occurrent.domain.DomainEvent;
import se.haleby.occurrent.domain.NameDefined;
import se.haleby.occurrent.domain.NameWasChanged;

import javax.annotation.PostConstruct;
import java.time.Duration;

import static io.vavr.API.*;
import static io.vavr.Predicates.instanceOf;
import static java.time.temporal.ChronoUnit.SECONDS;

@Component
public class CurrentNameProjectionUpdater {

    private final SpringBlockingSubscriptionWithPositionPersistenceForMongoDB subscription;
    private final CurrentNameProjection currentNameProjection;
    private final DeserializeCloudEventToDomainEvent deserializeCloudEventToDomainEvent;

    public CurrentNameProjectionUpdater(SpringBlockingSubscriptionWithPositionPersistenceForMongoDB subscription,
                                        CurrentNameProjection currentNameProjection,
                                        DeserializeCloudEventToDomainEvent deserializeCloudEventToDomainEvent) {
        this.subscription = subscription;
        this.currentNameProjection = currentNameProjection;
        this.deserializeCloudEventToDomainEvent = deserializeCloudEventToDomainEvent;
    }

    @PostConstruct
    void startProjectionUpdater() throws InterruptedException {
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