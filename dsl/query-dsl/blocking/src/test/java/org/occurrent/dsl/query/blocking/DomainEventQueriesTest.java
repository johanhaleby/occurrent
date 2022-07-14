/*
 *
 *  Copyright 2021 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.dsl.query.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.blocking.generic.GenericApplicationService;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.Name;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.application.composition.command.CommandConversion.toStreamCommand;
import static org.occurrent.application.composition.command.ListCommandComposition.composeCommands;
import static org.occurrent.application.composition.command.partial.PartialFunctionApplication.partial;
import static org.occurrent.eventstore.api.SortBy.SortDirection.DESCENDING;
import static org.occurrent.filter.Filter.type;

public class DomainEventQueriesTest {

    private ApplicationService<DomainEvent> applicationService;
    private DomainEventQueries<DomainEvent> domainEventQueries;

    @BeforeEach
    void createInstances() {
        CloudEventConverter<DomainEvent> cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::getEventId).build();
        InMemoryEventStore eventStore = new InMemoryEventStore();
        applicationService = new GenericApplicationService<>(eventStore, cloudEventConverter);
        domainEventQueries = new DomainEventQueries<>(eventStore, cloudEventConverter);
    }

    @Test
    void all() {
        // Given
        LocalDateTime time = LocalDateTime.now();

        applicationService.execute("stream", toStreamCommand(
                composeCommands(
                        partial(Name::defineName, "eventId1", time, "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "Jane Doe")
                )
        ));

        // When
        List<DomainEvent> events = domainEventQueries.all().collect(Collectors.toList());

        // Then
        assertAll(
                () -> assertThat(events).hasSize(2),
                () -> assertThat(events.stream().findFirst()).hasValue(new NameDefined("eventId1", time, "Some Doe")),
                () -> assertThat(events.stream().skip(1).findFirst()).hasValue(new NameWasChanged("eventId2", time, "Jane Doe"))
        );
    }

    @Test
    void queryWithAllFilter() {
        // Given
        LocalDateTime time = LocalDateTime.now();

        applicationService.execute("stream", toStreamCommand(
                composeCommands(
                        partial(Name::defineName, "eventId1", time, "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "Jane Doe")
                )
        ));

        // When
        List<DomainEvent> events = domainEventQueries.query(Filter.all()).collect(Collectors.toList());

        // Then
        assertAll(
                () -> assertThat(events).hasSize(2),
                () -> assertThat(events.stream().findFirst()).hasValue(new NameDefined("eventId1", time, "Some Doe")),
                () -> assertThat(events.stream().skip(1).findFirst()).hasValue(new NameWasChanged("eventId2", time, "Jane Doe"))
        );
    }

    @Test
    void queryBasedOnType() {
        // Given
        LocalDateTime time = LocalDateTime.now();

        applicationService.execute("stream", toStreamCommand(
                composeCommands(
                        partial(Name::defineName, "eventId1", time, "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "Jane Doe")
                )
        ));

        // When
        List<NameDefined> events = domainEventQueries.<NameDefined>query(type(NameDefined.class.getName())).collect(Collectors.toList());

        // Then
        assertAll(
                () -> assertThat(events).hasSize(1),
                () -> assertThat(events.stream().findFirst()).hasValue(new NameDefined("eventId1", time, "Some Doe"))
        );
    }

    @Test
    void queryOne() {
        // Given
        LocalDateTime time = LocalDateTime.now();

        applicationService.execute("stream", toStreamCommand(
                composeCommands(
                        partial(Name::defineName, "eventId1", time, "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "Jane Doe")
                )
        ));

        // When
        NameDefined event = domainEventQueries.<NameDefined>queryOne(type(NameDefined.class.getName()));

        // Then
        assertThat(event).isEqualTo(new NameDefined("eventId1", time, "Some Doe"));
    }

    @Test
    void queryBasedOnClassType() {
        // Given
        LocalDateTime time = LocalDateTime.now();

        applicationService.execute("stream", toStreamCommand(
                composeCommands(
                        partial(Name::defineName, "eventId1", time, "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "Jane Doe")
                )
        ));

        // When
        List<NameDefined> events = domainEventQueries.query(NameDefined.class).collect(Collectors.toList());

        // Then
        assertAll(
                () -> assertThat(events).hasSize(1),
                () -> assertThat(events.stream().findFirst()).hasValue(new NameDefined("eventId1", time, "Some Doe"))
        );
    }

    @Test
    void queryOneBasedOnClassType() {
        // Given
        LocalDateTime time = LocalDateTime.now();

        applicationService.execute("stream", toStreamCommand(
                composeCommands(
                        partial(Name::defineName, "eventId1", time, "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "Jane Doe"),
                        partial(Name::changeName, "eventId3", time, "Jane Doe2")
                )
        ));

        // When
        NameWasChanged event = domainEventQueries.queryOne(NameWasChanged.class);

        // Then
        assertThat(event).isEqualTo(new NameWasChanged("eventId2", time, "Jane Doe"));
    }

    @Test
    void queryBasedOnVarArgClassType() {
        // Given
        LocalDateTime time = LocalDateTime.now();

        applicationService.execute("stream", toStreamCommand(
                composeCommands(
                        partial(Name::defineName, "eventId1", time, "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "Jane Doe"),
                        partial(Name::changeName, "eventId3", time, "Jane Doe2")
                )
        ));

        // When
        List<DomainEvent> events = domainEventQueries.query(NameWasChanged.class, NameDefined.class).collect(Collectors.toList());

        // Then
        assertAll(
                () -> assertThat(events).hasSize(3),
                () -> assertThat(events).extracting(DomainEvent::getEventId).containsOnly("eventId1", "eventId2", "eventId3")
        );
    }

    @Test
    void queryBasedOCollectionClassType() {
        // Given
        LocalDateTime time = LocalDateTime.now();

        applicationService.execute("stream", toStreamCommand(
                composeCommands(
                        partial(Name::defineName, "eventId1", time, "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "Jane Doe"),
                        partial(Name::changeName, "eventId3", time, "Jane Doe2")
                )
        ));

        // When
        List<DomainEvent> events = domainEventQueries.query(Arrays.asList(NameWasChanged.class, NameDefined.class)).collect(Collectors.toList());

        // Then
        assertAll(
                () -> assertThat(events).hasSize(3),
                () -> assertThat(events).extracting(DomainEvent::getEventId).containsOnly("eventId1", "eventId2", "eventId3")
        );
    }

    @Test
    void queryOneBasedOnClassTypeAndSortBy() {
        // Given
        LocalDateTime time = LocalDateTime.now();

        applicationService.execute("stream", toStreamCommand(
                composeCommands(
                        partial(Name::defineName, "eventId1", time, "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "Jane Doe"),
                        partial(Name::changeName, "eventId3", time, "Jane Doe2")
                )
        ));

        // When
        NameWasChanged event = domainEventQueries.queryOne(NameWasChanged.class, SortBy.natural(DESCENDING));

        // Then
        assertThat(event).isEqualTo(new NameWasChanged("eventId3", time, "Jane Doe2"));
    }
}