package se.haleby.occurrent.example.eventstore.mongodb.spring.transactional;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import se.haleby.occurrent.domain.DomainEvent;
import se.haleby.occurrent.domain.Name;
import se.haleby.occurrent.domain.NameDefined;
import se.haleby.occurrent.domain.NameWasChanged;
import se.haleby.occurrent.eventstore.api.blocking.EventStream;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static io.vavr.API.*;
import static io.vavr.Predicates.instanceOf;

@Service
@Transactional
public class NameApplicationService {

    private final DomainEventStore eventStore;
    private CurrentNameProjection currentNameProjection;

    public NameApplicationService(DomainEventStore eventStore, CurrentNameProjection currentNameProjection) {
        this.eventStore = eventStore;
        this.currentNameProjection = currentNameProjection;
    }


    public void defineName(UUID id, LocalDateTime time, String name) {
        List<DomainEvent> events = Name.defineName(id.toString(), time, name);
        eventStore.append(id, 0, events);
        currentNameProjection.save(buildProjectionFromEvents(id, events));
    }

    public void changeName(UUID id, LocalDateTime time, String name) {
        EventStream<DomainEvent> eventStream = eventStore.loadEventStream(id);
        List<DomainEvent> events = eventStream.eventList();

        List<DomainEvent> newEvents = Name.changeName(events, UUID.randomUUID().toString(), time, name);

        eventStore.append(id, eventStream.version(), newEvents);
        currentNameProjection.save(buildProjectionFromEvents(id, append(events, newEvents)));
    }

    private CurrentName buildProjectionFromEvents(UUID id, List<DomainEvent> domainEvents) {
        return io.vavr.collection.List.ofAll(domainEvents).foldLeft(new CurrentName(id.toString()), (currentName, domainEvent) ->
                Match(domainEvent).of(
                        Case($(instanceOf(NameDefined.class)), e -> currentName.changeName(e.getName())),
                        Case($(instanceOf(NameWasChanged.class)), e -> currentName.changeName(e.getName()))
                )
        );
    }

    private static <T> List<T> append(List<T> list, List<T> list2) {
        ArrayList<T> ts1 = new ArrayList<>(list);
        ts1.addAll(list2);
        return Collections.unmodifiableList(ts1);
    }

    // For testing purposes
    void setCurrentNameProjection(CurrentNameProjection currentNameProjection) {
        this.currentNameProjection = currentNameProjection;
    }
}