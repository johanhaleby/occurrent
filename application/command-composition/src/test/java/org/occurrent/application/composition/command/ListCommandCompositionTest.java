/*
 * Copyright 2021 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.application.composition.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.occurrent.application.composition.command.partial.PartialFunctionApplication;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.Name;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class ListCommandCompositionTest {

    private InMemoryEventStore eventStore;
    private ApplicationService applicationService;

    @BeforeEach
    void configure_event_store_and_application_service() {
        eventStore = new InMemoryEventStore();
        applicationService = new ApplicationService(eventStore, new ObjectMapper());
    }

    @Test
    void compose_list_commands_with_partial_application() {
        // Given
        String eventId1 = UUID.randomUUID().toString();
        String eventId2 = UUID.randomUUID().toString();
        LocalDateTime now = LocalDateTime.now();

        // When
        applicationService.executeListCommand("name1", ListCommandComposition.composeCommands(PartialFunctionApplication.partial(Name::defineName, eventId1, now, "name", "My name"), PartialFunctionApplication.partial(Name::changeName, eventId2, now, "name", "My name 2")));

        // Then
        List<DomainEvent> domainEvents = eventStore.read("name1").events().map(applicationService::convertCloudEventToDomainEvent).collect(Collectors.toList());
        assertThat(domainEvents.stream().map(event -> event.getClass().getSimpleName())).containsExactly(NameDefined.class.getSimpleName(), NameWasChanged.class.getSimpleName());
        assertThat(domainEvents.stream().map(DomainEvent::name)).containsExactly("My name", "My name 2");
    }

    @Test
    void compose_list_commands_with_partial_application_does_not_return_previous_events() {
        // Given
        List<Function<List<DomainEvent>, List<DomainEvent>>> fns = Arrays.asList(e -> Collections.singletonList(new NameDefined("eventId1", new Date(), "name", "name1")), e -> Collections.singletonList(new NameWasChanged("eventId2", new Date(), "name", "name2")));

        // When
        Function<List<DomainEvent>, List<DomainEvent>> composedCommand = ListCommandComposition.composeCommands(fns);

        // Then
        List<DomainEvent> domainEvents = composedCommand.apply(Arrays.asList(new NameDefined("eventId3", new Date(), "name", "name3"), new NameDefined("eventId4", new Date(), "name", "name4")));
        assertThat(domainEvents).hasSize(2);
        assertThat(domainEvents).extracting(DomainEvent::eventId).containsExactly("eventId1", "eventId2");
    }

    @Test
    void previous_events_are_passed_to_each_function_that_is_involved_in_the_composition() {
        // Given
        List<Function<List<DomainEvent>, List<DomainEvent>>> fns = Arrays.asList(e -> {
            if (e.size() == 2) {
                return Collections.singletonList(new NameDefined("eventId1", new Date(), "name", "name1"));
            } else {
                return Collections.emptyList();
            }
        }, e -> {
            if (e.size() == 3) {
                return Collections.singletonList(new NameWasChanged("eventId2", new Date(), "name", "name2"));
            } else {
                return Collections.emptyList();
            }
        });

        // When
        Function<List<DomainEvent>, List<DomainEvent>> composedCommand = ListCommandComposition.composeCommands(fns);

        // Then
        List<DomainEvent> domainEvents = composedCommand.apply(Arrays.asList(new NameDefined("eventId3", new Date(), "name", "name3"), new NameDefined("eventId4", new Date(), "name", "name4")));
        assertThat(domainEvents).hasSize(2);
        assertThat(domainEvents).extracting(DomainEvent::eventId).containsExactly("eventId1", "eventId2");
    }
}
