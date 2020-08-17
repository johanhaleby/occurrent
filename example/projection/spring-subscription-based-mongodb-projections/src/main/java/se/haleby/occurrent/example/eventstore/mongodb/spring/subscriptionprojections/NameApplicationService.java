package se.haleby.occurrent.example.eventstore.mongodb.spring.subscriptionprojections;

import org.springframework.stereotype.Service;
import se.haleby.occurrent.domain.DomainEvent;
import se.haleby.occurrent.domain.Name;
import se.haleby.occurrent.eventstore.api.blocking.EventStream;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
public class NameApplicationService {

    private final DomainEventStore eventStore;

    public NameApplicationService(DomainEventStore eventStore) {
        this.eventStore = eventStore;
    }

    public void defineName(UUID id, LocalDateTime time, String name) {
        List<DomainEvent> events = Name.defineName(UUID.randomUUID().toString(), time, name);
        eventStore.append(id, events);
    }

    public void changeName(UUID id, LocalDateTime time, String name) {
        EventStream<DomainEvent> eventStream = eventStore.loadEventStream(id);
        List<DomainEvent> events = eventStream.eventList();

        List<DomainEvent> newEvents = Name.changeName(events, UUID.randomUUID().toString(), time, name);

        eventStore.append(id, newEvents);
    }
}