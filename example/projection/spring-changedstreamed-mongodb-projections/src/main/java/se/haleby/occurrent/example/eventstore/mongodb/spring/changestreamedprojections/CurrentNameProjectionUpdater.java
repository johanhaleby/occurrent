package se.haleby.occurrent.example.eventstore.mongodb.spring.changestreamedprojections;

import org.springframework.stereotype.Component;
import se.haleby.occurrent.changestreamer.mongodb.spring.blocking.SpringBlockingChangeStreamerForMongoDB;
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

    private final SpringBlockingChangeStreamerForMongoDB changeStreamer;
    private final CurrentNameProjection currentNameProjection;
    private final DeserializeCloudEventToDomainEvent deserializeCloudEventToDomainEvent;

    public CurrentNameProjectionUpdater(SpringBlockingChangeStreamerForMongoDB changeStreamer,
                                        CurrentNameProjection currentNameProjection,
                                        DeserializeCloudEventToDomainEvent deserializeCloudEventToDomainEvent) {
        this.changeStreamer = changeStreamer;
        this.currentNameProjection = currentNameProjection;
        this.deserializeCloudEventToDomainEvent = deserializeCloudEventToDomainEvent;
    }

    @PostConstruct
    void startProjectionUpdater() throws InterruptedException {
        changeStreamer
                .stream("current-name", cloudEvent -> {
                    DomainEvent domainEvent = deserializeCloudEventToDomainEvent.deserialize(cloudEvent);
                    String eventId = cloudEvent.getId();
                    CurrentName currentName = Match(domainEvent).of(
                            Case($(instanceOf(NameDefined.class)), e -> new CurrentName(eventId, e.getName())),
                            Case($(instanceOf(NameWasChanged.class)), e -> new CurrentName(eventId, e.getName())));
                    currentNameProjection.save(currentName);
                })
                .await(Duration.of(2, SECONDS));
    }
}