/*
 * Copyright 2026 Johan Haleby
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

package org.occurrent.dsl.query.reactor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.application.service.reactor.generic.GenericApplicationService;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.Name;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.application.composition.command.ListCommandComposition.composeCommands;
import static org.occurrent.application.composition.command.partial.PartialFunctionApplication.partial;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.SortBy.SortDirection.DESCENDING;
import static org.occurrent.filter.Filter.type;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class DomainEventQueriesTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    FlushMongoDBExtension flush = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".querydsl"));

    private ApplicationService<DomainEvent> applicationService;
    private DomainEventQueries<DomainEvent> domainEventQueries;
    // A store with stream position opted out, for the guard tests. The default store above writes position (on by
    // default) and would not trip the guard.
    private DomainEventQueries<DomainEvent> domainEventQueriesWithoutPosition;

    @BeforeEach
    void createInstances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".querydsl");
        MongoClient mongoClient = MongoClients.create(connectionString);
        ReactiveMongoTemplate mongoTemplate = new ReactiveMongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        ReactiveMongoTransactionManager tx = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(tx)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .build();
        ReactorMongoEventStore eventStore = new ReactorMongoEventStore(mongoTemplate, config);
        CloudEventConverter<DomainEvent> cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        applicationService = new GenericApplicationService<>(eventStore, cloudEventConverter);
        domainEventQueries = new DomainEventQueries<>(eventStore, cloudEventConverter);

        EventStoreConfig configWithoutPosition = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(tx)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .withoutStreamPosition()
                .build();
        ReactorMongoEventStore eventStoreWithoutPosition = new ReactorMongoEventStore(mongoTemplate, configWithoutPosition);
        domainEventQueriesWithoutPosition = new DomainEventQueries<>(eventStoreWithoutPosition, cloudEventConverter);
    }

    @Test
    void all() {
        // Given
        LocalDateTime time = LocalDateTime.now();
        applicationService.execute("stream",
                composeCommands(
                        partial(Name::defineName, "eventId1", time, "name", "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "name", "Jane Doe")
                )
        ).block();

        // When
        List<DomainEvent> events = domainEventQueries.all().collectList().block();

        // Then
        assertAll(
                () -> assertThat(events).hasSize(2),
                () -> assertThat(events).element(0).isEqualTo(new NameDefined("eventId1", time, "name", "Some Doe")),
                () -> assertThat(events).element(1).isEqualTo(new NameWasChanged("eventId2", time, "name", "Jane Doe"))
        );
    }

    @Test
    void query_based_on_type() {
        // Given
        LocalDateTime time = LocalDateTime.now();
        applicationService.execute("stream",
                composeCommands(
                        partial(Name::defineName, "eventId1", time, "name", "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "name", "Jane Doe")
                )
        ).block();

        // When
        List<NameDefined> events = domainEventQueries.<NameDefined>query(type(NameDefined.class.getName())).collectList().block();

        // Then
        assertAll(
                () -> assertThat(events).hasSize(1),
                () -> assertThat(events).element(0).isEqualTo(new NameDefined("eventId1", time, "name", "Some Doe"))
        );
    }

    @Test
    void query_one() {
        // Given
        LocalDateTime time = LocalDateTime.now();
        applicationService.execute("stream",
                composeCommands(
                        partial(Name::defineName, "eventId1", time, "name", "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "name", "Jane Doe")
                )
        ).block();

        // When
        NameDefined event = domainEventQueries.<NameDefined>queryOne(type(NameDefined.class.getName())).block();

        // Then
        assertThat(event).isEqualTo(new NameDefined("eventId1", time, "name", "Some Doe"));
    }

    @Test
    void query_one_when_no_events_match_then_mono_is_empty() {
        // When
        NameDefined event = domainEventQueries.<NameDefined>queryOne(type(NameDefined.class.getName())).block();

        // Then
        assertThat(event).isNull();
    }

    @Test
    void query_based_on_class_type() {
        // Given
        LocalDateTime time = LocalDateTime.now();
        applicationService.execute("stream",
                composeCommands(
                        partial(Name::defineName, "eventId1", time, "name", "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "name", "Jane Doe")
                )
        ).block();

        // When
        List<NameDefined> events = domainEventQueries.query(NameDefined.class).collectList().block();

        // Then
        assertAll(
                () -> assertThat(events).hasSize(1),
                () -> assertThat(events).element(0).isEqualTo(new NameDefined("eventId1", time, "name", "Some Doe"))
        );
    }

    @Test
    void query_one_based_on_class_type() {
        // Given
        LocalDateTime time = LocalDateTime.now();
        applicationService.execute("stream",
                composeCommands(
                        partial(Name::defineName, "eventId1", time, "name", "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "name", "Jane Doe"),
                        partial(Name::changeName, "eventId3", time, "name", "Jane Doe2")
                )
        ).block();

        // When
        NameWasChanged event = domainEventQueries.queryOne(NameWasChanged.class).block();

        // Then
        assertThat(event).isEqualTo(new NameWasChanged("eventId2", time, "name", "Jane Doe"));
    }

    @Test
    void query_based_on_var_arg_class_type() {
        // Given
        LocalDateTime time = LocalDateTime.now();
        applicationService.execute("stream",
                composeCommands(
                        partial(Name::defineName, "eventId1", time, "name", "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "name", "Jane Doe"),
                        partial(Name::changeName, "eventId3", time, "name", "Jane Doe2")
                )
        ).block();

        // When
        List<DomainEvent> events = domainEventQueries.query(NameWasChanged.class, NameDefined.class).collectList().block();

        // Then
        assertAll(
                () -> assertThat(events).hasSize(3),
                () -> assertThat(events).extracting(DomainEvent::eventId).containsOnly("eventId1", "eventId2", "eventId3")
        );
    }

    @Test
    void query_based_on_collection_class_type() {
        // Given
        LocalDateTime time = LocalDateTime.now();
        applicationService.execute("stream",
                composeCommands(
                        partial(Name::defineName, "eventId1", time, "name", "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "name", "Jane Doe"),
                        partial(Name::changeName, "eventId3", time, "name", "Jane Doe2")
                )
        ).block();

        // When
        List<DomainEvent> events = domainEventQueries.query(Arrays.asList(NameWasChanged.class, NameDefined.class)).collectList().block();

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
        applicationService.execute("stream",
                composeCommands(
                        partial(Name::defineName, "eventId1", time, "name", "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "name", "Jane Doe"),
                        partial(Name::changeName, "eventId3", time, "name", "Jane Doe2")
                )
        ).block();

        // When
        NameWasChanged event = domainEventQueries.queryOne(NameWasChanged.class, SortBy.natural(DESCENDING)).block();

        // Then
        assertThat(event).isEqualTo(new NameWasChanged("eventId3", time, "name", "Jane Doe2"));
    }

    @Test
    void count_and_exists() {
        // Given
        LocalDateTime time = LocalDateTime.now();
        applicationService.execute("stream",
                composeCommands(
                        partial(Name::defineName, "eventId1", time, "name", "Some Doe"),
                        partial(Name::changeName, "eventId2", time, "name", "Jane Doe")
                )
        ).block();

        // When
        Long count = domainEventQueries.count().block();
        Boolean exists = domainEventQueries.exists(type(NameDefined.class.getName())).block();

        // Then
        assertAll(
                () -> assertThat(count).isEqualTo(2L),
                () -> assertThat(exists).isTrue()
        );
    }

    @Test
    void toDomainEvents_converts_an_existing_flux_of_cloud_events() {
        // Given
        LocalDateTime time = LocalDateTime.now();
        applicationService.execute("stream",
                partial(Name::defineName, "eventId1", time, "name", "Some Doe")
        ).block();
        Flux<CloudEvent> cloudEvents = domainEventQueries.eventStoreQueries().all();

        // When
        List<DomainEvent> events = domainEventQueries.<DomainEvent>toDomainEvents(cloudEvents).collectList().block();

        // Then
        assertThat(events).containsExactly(new NameDefined("eventId1", time, "name", "Some Doe"));
    }

    private DomainEventQueries<DomainEvent> domainEventQueriesFor(ReactorMongoEventStore eventStore) {
        CloudEventConverter<DomainEvent> cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        return new DomainEventQueries<>(eventStore, cloudEventConverter);
    }

    private ApplicationService<DomainEvent> applicationServiceFor(ReactorMongoEventStore eventStore) {
        CloudEventConverter<DomainEvent> cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        return new GenericApplicationService<>(eventStore, cloudEventConverter);
    }

    private ReactorMongoEventStore storeWith(EventStoreConfig.Builder builder) {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".querydsl");
        MongoClient mongoClient = MongoClients.create(connectionString);
        ReactiveMongoTemplate mongoTemplate = new ReactiveMongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        ReactiveMongoTransactionManager transactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig config = builder
                .eventStoreCollectionName("events")
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .build();
        return new ReactorMongoEventStore(mongoTemplate, config);
    }

    @Test
    void afterPosition_returns_domain_events_strictly_after_the_given_position_in_ascending_position_order() {
        // Given
        ReactorMongoEventStore eventStore = storeWith(new EventStoreConfig.Builder().eventStoreCapabilities(STREAM, DCB));
        DomainEventQueries<DomainEvent> queriesWithPosition = domainEventQueriesFor(eventStore);
        ApplicationService<DomainEvent> applicationServiceWithPosition = applicationServiceFor(eventStore);

        LocalDateTime time = LocalDateTime.now();
        applicationServiceWithPosition.execute("stream1", partial(Name::defineName, "eventId1", time, "name", "Some Doe")).block();
        applicationServiceWithPosition.execute("stream1", partial(Name::changeName, "eventId2", time, "name", "Jane Doe")).block();
        applicationServiceWithPosition.execute("stream1", partial(Name::changeName, "eventId3", time, "name", "Jane Doe2")).block();

        // When
        List<DomainEvent> events = queriesWithPosition.afterPosition(1).collectList().block();

        // Then
        assertAll(
                () -> assertThat(events).hasSize(2),
                () -> assertThat(events).extracting(DomainEvent::eventId).containsExactly("eventId2", "eventId3")
        );
    }

    @Test
    void readInPositionOrder_returns_domain_events_matching_the_filter_within_the_given_range_in_ascending_position_order() {
        // Given
        ReactorMongoEventStore eventStore = storeWith(new EventStoreConfig.Builder().eventStoreCapabilities(STREAM, DCB));
        DomainEventQueries<DomainEvent> queriesWithPosition = domainEventQueriesFor(eventStore);
        ApplicationService<DomainEvent> applicationServiceWithPosition = applicationServiceFor(eventStore);

        LocalDateTime time = LocalDateTime.now();
        applicationServiceWithPosition.execute("stream1", partial(Name::defineName, "eventId1", time, "name", "Some Doe")).block();
        applicationServiceWithPosition.execute("stream1", partial(Name::changeName, "eventId2", time, "name", "Jane Doe")).block();
        applicationServiceWithPosition.execute("stream1", partial(Name::changeName, "eventId3", time, "name", "Jane Doe2")).block();

        // When
        List<DomainEvent> events = queriesWithPosition.readInPositionOrder(org.occurrent.filter.Filter.all(), PositionRange.between(1, 2)).collectList().block();

        // Then
        assertAll(
                () -> assertThat(events).hasSize(1),
                () -> assertThat(events).extracting(DomainEvent::eventId).containsExactly("eventId2")
        );
    }

    @Test
    void afterPosition_emits_an_error_when_the_underlying_event_store_does_not_write_a_position() {
        // domainEventQueriesWithoutPosition is backed by a STREAM-only store with stream position opted out.

        // When / Then
        StepVerifier.create(domainEventQueriesWithoutPosition.afterPosition(0))
                .expectError(UnsupportedOperationException.class)
                .verify();
    }

    @Test
    void readInPositionOrder_emits_an_error_when_the_underlying_event_store_does_not_write_a_position() {
        // When / Then
        StepVerifier.create(domainEventQueriesWithoutPosition.readInPositionOrder(org.occurrent.filter.Filter.all(), PositionRange.fromBeginning()))
                .expectError(UnsupportedOperationException.class)
                .verify();
    }

    @Test
    void currentPosition_emits_an_error_when_the_underlying_event_store_does_not_write_a_position() {
        // When / Then
        StepVerifier.create(domainEventQueriesWithoutPosition.currentPosition())
                .expectError(UnsupportedOperationException.class)
                .verify();
    }
}
