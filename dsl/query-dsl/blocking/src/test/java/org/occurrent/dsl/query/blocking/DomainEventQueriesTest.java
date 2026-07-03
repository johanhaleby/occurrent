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
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
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

@DisplayNameGeneration(ReplaceUnderscores.class)
public class DomainEventQueriesTest {

    private ApplicationService<DomainEvent> applicationService;
    private DomainEventQueries<DomainEvent> domainEventQueries;

    @BeforeEach
    void createInstances() {
        CloudEventConverter<DomainEvent> cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
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
                        partial(Name::defineName, "eventId1", time, "name", "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "name", "Jane Doe")
                )
        ));

        // When
        List<DomainEvent> events = domainEventQueries.all().collect(Collectors.toList());

        // Then
        assertAll(
                () -> assertThat(events).hasSize(2),
                () -> assertThat(events.stream().findFirst()).hasValue(new NameDefined("eventId1", time, "name", "Some Doe")),
                () -> assertThat(events.stream().skip(1).findFirst()).hasValue(new NameWasChanged("eventId2", time, "name", "Jane Doe"))
        );
    }

    @Test
    void query_with_all_filter() {
        // Given
        LocalDateTime time = LocalDateTime.now();

        applicationService.execute("stream", toStreamCommand(
                composeCommands(
                        partial(Name::defineName, "eventId1", time, "name", "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "name", "Jane Doe")
                )
        ));

        // When
        List<DomainEvent> events = domainEventQueries.query(Filter.all()).collect(Collectors.toList());

        // Then
        assertAll(
                () -> assertThat(events).hasSize(2),
                () -> assertThat(events.stream().findFirst()).hasValue(new NameDefined("eventId1", time, "name", "Some Doe")),
                () -> assertThat(events.stream().skip(1).findFirst()).hasValue(new NameWasChanged("eventId2", time, "name", "Jane Doe"))
        );
    }

    @Test
    void query_based_on_type() {
        // Given
        LocalDateTime time = LocalDateTime.now();

        applicationService.execute("stream", toStreamCommand(
                composeCommands(
                        partial(Name::defineName, "eventId1", time, "name", "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "name", "Jane Doe")
                )
        ));

        // When
        List<NameDefined> events = domainEventQueries.<NameDefined>query(type(NameDefined.class.getName())).collect(Collectors.toList());

        // Then
        assertAll(
                () -> assertThat(events).hasSize(1),
                () -> assertThat(events.stream().findFirst()).hasValue(new NameDefined("eventId1", time, "name", "Some Doe"))
        );
    }


    @Test
    void query_one() {
        // Given
        LocalDateTime time = LocalDateTime.now();

        applicationService.execute("stream", toStreamCommand(
                composeCommands(
                        partial(Name::defineName, "eventId1", time, "name", "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "name", "Jane Doe")
                )
        ));

        // When
        NameDefined event = domainEventQueries.queryOne(type(NameDefined.class.getName()));

        // Then
        assertThat(event).isEqualTo(new NameDefined("eventId1", time, "name", "Some Doe"));
    }

    @Test
    void query_one_when_multiple_events_match_then_the_first_is_returned() {
        // Given
        LocalDateTime time = LocalDateTime.now();

        applicationService.execute("stream1", toStreamCommand(
                composeCommands(
                        partial(Name::defineName, "eventId1", time, "name", "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "name", "Jane Doe")
                )
        ));

        applicationService.execute("stream2", toStreamCommand(
                composeCommands(
                        partial(Name::defineName, "eventId3", time, "name", "Another Doe"),
                        partial(Name::changeName, "eventId4", time, "name", "Jane2 Doe")
                )
        ));

        // When
        NameDefined event = domainEventQueries.queryOne(type(NameDefined.class.getName()));

        // Then
        assertThat(event).isEqualTo(new NameDefined("eventId1", time, "name", "Some Doe"));
    }

    @Test
    void query_based_on_class_type() {
        // Given
        LocalDateTime time = LocalDateTime.now();

        applicationService.execute("stream", toStreamCommand(
                composeCommands(
                        partial(Name::defineName, "eventId1", time, "name", "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "name", "Jane Doe")
                )
        ));

        // When
        List<NameDefined> events = domainEventQueries.query(NameDefined.class).collect(Collectors.toList());

        // Then
        assertAll(
                () -> assertThat(events).hasSize(1),
                () -> assertThat(events.stream().findFirst()).hasValue(new NameDefined("eventId1", time, "name", "Some Doe"))
        );
    }

    @Test
    void query_one_based_on_class_type() {
        // Given
        LocalDateTime time = LocalDateTime.now();

        applicationService.execute("stream", toStreamCommand(
                composeCommands(
                        partial(Name::defineName, "eventId1", time, "name", "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "name", "Jane Doe"),
                        partial(Name::changeName, "eventId3", time, "name", "Jane Doe2")
                )
        ));

        // When
        NameWasChanged event = domainEventQueries.queryOne(NameWasChanged.class);

        // Then
        assertThat(event).isEqualTo(new NameWasChanged("eventId2", time, "name", "Jane Doe"));
    }

    @Test
    void query_based_on_var_arg_class_type() {
        // Given
        LocalDateTime time = LocalDateTime.now();

        applicationService.execute("stream", toStreamCommand(
                composeCommands(
                        partial(Name::defineName, "eventId1", time, "name", "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "name", "Jane Doe"),
                        partial(Name::changeName, "eventId3", time, "name", "Jane Doe2")
                )
        ));

        // When
        List<DomainEvent> events = domainEventQueries.query(NameWasChanged.class, NameDefined.class).collect(Collectors.toList());
        

        // Then
        assertAll(
                () -> assertThat(events).hasSize(3),
                () -> assertThat(events).extracting(DomainEvent::eventId).containsOnly("eventId1", "eventId2", "eventId3")
        );
    }

    @Test
    void query_based_o_collection_class_type() {
        // Given
        LocalDateTime time = LocalDateTime.now();

        applicationService.execute("stream", toStreamCommand(
                composeCommands(
                        partial(Name::defineName, "eventId1", time, "name", "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "name", "Jane Doe"),
                        partial(Name::changeName, "eventId3", time, "name", "Jane Doe2")
                )
        ));

        // When
        List<DomainEvent> events = domainEventQueries.query(Arrays.asList(NameWasChanged.class, NameDefined.class)).collect(Collectors.toList());

        // Then
        assertAll(
                () -> assertThat(events).hasSize(3),
                () -> assertThat(events).extracting(DomainEvent::eventId).containsOnly("eventId1", "eventId2", "eventId3")
        );
    }

    @Test
    void query_one_based_on_class_type_and_sort_by() {
        // Given
        LocalDateTime time = LocalDateTime.now();

        applicationService.execute("stream", toStreamCommand(
                composeCommands(
                        partial(Name::defineName, "eventId1", time, "name", "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "name", "Jane Doe"),
                        partial(Name::changeName, "eventId3", time, "name", "Jane Doe2")
                )
        ));

        // When
        NameWasChanged event = domainEventQueries.queryOne(NameWasChanged.class, SortBy.natural(DESCENDING));

        // Then
        assertThat(event).isEqualTo(new NameWasChanged("eventId3", time, "name", "Jane Doe2"));
    }

    @Test
    void afterPosition_returns_domain_events_strictly_after_the_given_position_in_ascending_position_order() {
        // Given
        LocalDateTime time = LocalDateTime.now();
        InMemoryEventStore eventStore = new InMemoryEventStore().withStreamPosition();
        CloudEventConverter<DomainEvent> cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        ApplicationService<DomainEvent> applicationServiceWithPosition = new GenericApplicationService<>(eventStore, cloudEventConverter);
        DomainEventQueries<DomainEvent> queriesWithPosition = new DomainEventQueries<>(eventStore, cloudEventConverter);

        applicationServiceWithPosition.execute("stream1", toStreamCommand(partial(Name::defineName, "eventId1", time, "name", "Some Doe")));
        applicationServiceWithPosition.execute("stream1", toStreamCommand(partial(Name::changeName, "eventId2", time, "name", "Jane Doe")));
        applicationServiceWithPosition.execute("stream1", toStreamCommand(partial(Name::changeName, "eventId3", time, "name", "Jane Doe2")));

        // When
        List<DomainEvent> events = queriesWithPosition.afterPosition(1).collect(Collectors.toList());

        // Then
        assertAll(
                () -> assertThat(events).hasSize(2),
                () -> assertThat(events).extracting(DomainEvent::eventId).containsExactly("eventId2", "eventId3")
        );
    }

    @Test
    void readInPositionOrder_returns_domain_events_matching_the_filter_within_the_given_range_in_ascending_position_order() {
        // Given
        LocalDateTime time = LocalDateTime.now();
        InMemoryEventStore eventStore = new InMemoryEventStore().withStreamPosition();
        CloudEventConverter<DomainEvent> cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        ApplicationService<DomainEvent> applicationServiceWithPosition = new GenericApplicationService<>(eventStore, cloudEventConverter);
        DomainEventQueries<DomainEvent> queriesWithPosition = new DomainEventQueries<>(eventStore, cloudEventConverter);

        applicationServiceWithPosition.execute("stream1", toStreamCommand(partial(Name::defineName, "eventId1", time, "name", "Some Doe")));
        applicationServiceWithPosition.execute("stream1", toStreamCommand(partial(Name::changeName, "eventId2", time, "name", "Jane Doe")));
        applicationServiceWithPosition.execute("stream1", toStreamCommand(partial(Name::changeName, "eventId3", time, "name", "Jane Doe2")));

        // When
        List<DomainEvent> events = queriesWithPosition.readInPositionOrder(Filter.all(), org.occurrent.eventstore.api.PositionRange.between(1, 2))
                .collect(Collectors.toList());

        // Then
        assertAll(
                () -> assertThat(events).hasSize(1),
                () -> assertThat(events).extracting(DomainEvent::eventId).containsExactly("eventId2")
        );
    }

    @Test
    void afterPosition_throws_when_the_underlying_event_store_does_not_write_a_position() {
        // When / Then
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> queriesWithoutPosition().afterPosition(0))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void readInPositionOrder_throws_when_the_underlying_event_store_does_not_write_a_position() {
        // When / Then
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> queriesWithoutPosition().readInPositionOrder(Filter.all(), org.occurrent.eventstore.api.PositionRange.fromBeginning()))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void currentPosition_throws_when_the_underlying_event_store_does_not_write_a_position() {
        // When / Then
        org.assertj.core.api.Assertions.assertThatThrownBy(queriesWithoutPosition()::currentPosition)
                .isInstanceOf(UnsupportedOperationException.class);
    }

    // Stream position is on by default, so a plain InMemoryEventStore writes position; opt out to get a store that
    // rejects the position-requiring query APIs.
    private DomainEventQueries<DomainEvent> queriesWithoutPosition() {
        InMemoryEventStore eventStore = new InMemoryEventStore().withoutStreamPosition();
        CloudEventConverter<DomainEvent> cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        return new DomainEventQueries<>(eventStore, cloudEventConverter);
    }
}