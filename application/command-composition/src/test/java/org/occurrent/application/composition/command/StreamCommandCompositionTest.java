package org.occurrent.application.composition.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.occurrent.application.composition.command.partial.PartialFunctionApplication;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class StreamCommandCompositionTest {

    private InMemoryEventStore eventStore;
    private ApplicationService applicationService;

    @BeforeEach
    void configure_event_store_and_application_service() {
        eventStore = new InMemoryEventStore();
        applicationService = new ApplicationService(eventStore, new ObjectMapper());
    }

    @Test
    void compose_stream_commands_with_partial_application() {
        // Given
        String eventId1 = UUID.randomUUID().toString();
        String eventId2 = UUID.randomUUID().toString();
        LocalDateTime now = LocalDateTime.now();

        // When
        applicationService.executeStreamCommand("name1", StreamCommandComposition.composeCommands(PartialFunctionApplication.partial(NameWithStreamCommand::defineName, eventId1, now, "My name"), PartialFunctionApplication.partial(NameWithStreamCommand::changeName, eventId2, now, "My name 2")));

        // Then
        List<DomainEvent> domainEvents = eventStore.read("name1").events().map(applicationService::convertCloudEventToDomainEvent).collect(Collectors.toList());
        assertThat(domainEvents.stream().map(event -> event.getClass().getSimpleName())).containsExactly(NameDefined.class.getSimpleName(), NameWasChanged.class.getSimpleName());
        assertThat(domainEvents.stream().map(DomainEvent::name)).containsExactly("My name", "My name 2");
    }
}
