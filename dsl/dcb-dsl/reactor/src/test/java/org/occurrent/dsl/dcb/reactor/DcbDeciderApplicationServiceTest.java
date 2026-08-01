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

package org.occurrent.dsl.dcb.reactor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.dcb.TagGenerator;
import org.occurrent.application.service.reactor.dcb.DcbApplicationService;
import org.occurrent.application.service.reactor.dcb.GenericDcbApplicationService;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.dsl.dcb.DcbDecider;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

@Testcontainers
@DisplayName("DcbApplicationServiceDeciders")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DcbDeciderApplicationServiceTest {

    @Container
    private static final MongoDBContainer mongoDBContainer;

    static {
        mongoDBContainer = new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet();
        List<String> ports = new ArrayList<>();
        ports.add("27017:27017");
        mongoDBContainer.withReuse(true).setPortBindings(ports);
    }

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcbappservicedeciders"));

    private ReactorMongoEventStore eventStore;
    private CloudEventConverter<DomainEvent> converter;
    private DcbApplicationService<DomainEvent> applicationService;
    private DcbDeciderApplicationService<DomainEvent> deciderApplicationService;
    private LocalDateTime time;

    @BeforeEach
    void create_instances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcbappservicedeciders");
        MongoClient mongoClient = MongoClients.create(connectionString);
        ReactiveMongoTemplate mongoTemplate = new ReactiveMongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        ReactiveMongoTransactionManager transactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        eventStore = new ReactorMongoEventStore(mongoTemplate, config);
        converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        applicationService = new GenericDcbApplicationService<>(eventStore, converter, (DomainEvent event) -> Set.of(tagFor(event)), GenericDcbApplicationService.defaultRetry());
        deciderApplicationService = new DcbDeciderApplicationService<>(applicationService);
        time = LocalDateTime.now();
    }

    @Nested
    @DisplayName("execute")
    class Execute {

        @Test
        void appends_the_events_decided_by_the_decider() {
            // When
            DcbAppendResult result = deciderApplicationService.execute(new DefineName("Jane Doe"), nameDcbDecider()).block();

            // Then
            assertThat(requireNonNull(result).eventCount()).isEqualTo(1);
            assertThat(readNameEvents("name")).containsExactly(new NameDefined("event-1", time, "name", "Jane Doe"));
        }

        @Test
        void completes_empty_when_the_decision_produces_no_new_events() {
            // Given
            append(new NameDefined("event-0", time, "name", "Jane Doe"));

            // When
            DcbAppendResult result = deciderApplicationService.execute(new DefineName("Jane Doe"), nameDcbDecider()).block();

            // Then
            assertThat(result).isNull();
            assertThat(readNameEvents("name")).containsExactly(new NameDefined("event-0", time, "name", "Jane Doe"));
        }

        @Test
        void appends_events_for_multiple_commands_that_resolve_to_the_same_boundary() {
            // When
            DcbAppendResult result = deciderApplicationService.execute(List.of(new DefineName("Jane Doe"), new ChangeName("John Doe")), nameDcbDecider()).block();

            // Then
            assertThat(result).isNotNull();
            assertThat(readNameEvents("name")).containsExactly(
                    new NameDefined("event-1", time, "name", "Jane Doe"),
                    new NameWasChanged("event-2", time, "name", "John Doe")
            );
        }

        @Test
        void fails_with_IllegalArgumentException_when_the_decider_does_not_recognize_the_command() {
            // Given
            var unrecognizingDecider = DcbDecider.from(
                    nameDecider(),
                    (NameCommand command) -> command instanceof DefineName ? nameQuery("name") : null,
                    (TagGenerator<DomainEvent>) event -> Set.of(tagFor(event))
            );

            // When
            Mono<DcbAppendResult> result = deciderApplicationService.execute(new ChangeName("John Doe"), unrecognizingDecider);

            // Then
            StepVerifier.create(result).expectError(IllegalArgumentException.class).verify();
        }

        @Test
        void fails_with_IllegalArgumentException_when_commands_in_a_batch_resolve_to_different_boundaries() {
            // Given
            var perCommandDecider = DcbDecider.from(
                    nameDecider(),
                    (NameCommand command) -> switch (command) {
                        case DefineName defineName -> nameQuery(defineName.name());
                        case ChangeName changeName -> nameQuery(changeName.name());
                        case NoOp __ -> nameQuery("name");
                    },
                    (TagGenerator<DomainEvent>) event -> Set.of(tagFor(event))
            );

            // When
            Mono<DcbAppendResult> result = deciderApplicationService.execute(List.of(new DefineName("Jane Doe"), new ChangeName("John Doe")), perCommandDecider);

            // Then
            StepVerifier.create(result).expectError(IllegalArgumentException.class).verify();
        }

        @Test
        void executes_under_the_boundary_it_is_given_without_asking_the_decider_for_one() {
            // Given a criteria function that counts how often it is asked
            AtomicInteger derivations = new AtomicInteger();
            var countingDecider = DcbDecider.from(
                    nameDecider(),
                    (NameCommand command) -> {
                        derivations.incrementAndGet();
                        return nameQuery("name");
                    },
                    (TagGenerator<DomainEvent>) event -> Set.of(tagFor(event))
            );

            // When the boundary is passed in rather than derived
            Mono<DcbAppendResult> result = deciderApplicationService.execute(nameQuery("name"), List.of(new DefineName("Jane Doe")), countingDecider);

            // Then the events landed and the decider was never asked to resolve a boundary
            StepVerifier.create(result).expectNextCount(1).verifyComplete();
            assertThat(derivations).hasValue(0);
        }
    }

    @Nested
    @DisplayName("executeAndReturnDecision")
    class ExecuteAndReturnDecision {

        @Test
        void emits_the_folded_state_plus_the_new_events() {
            // Given
            append(new NameDefined("event-0", time, "name", "Jane Doe"));

            // When
            Decider.Decision<String, DomainEvent> decision = deciderApplicationService.executeAndReturnDecision(new ChangeName("John Doe"), nameDcbDecider()).block();

            // Then
            assertThat(requireNonNull(decision).state()).isEqualTo("John Doe");
            assertThat(decision.events()).containsExactly(new NameWasChanged("event-2", time, "name", "John Doe"));
        }

        @Test
        void fails_with_IllegalArgumentException_when_the_decider_does_not_recognize_the_command() {
            // Given
            var unrecognizingDecider = DcbDecider.from(
                    nameDecider(),
                    (NameCommand command) -> command instanceof DefineName ? nameQuery("name") : null,
                    (TagGenerator<DomainEvent>) event -> Set.of(tagFor(event))
            );

            // When
            Mono<Decider.Decision<String, DomainEvent>> result = deciderApplicationService.executeAndReturnDecision(new ChangeName("John Doe"), unrecognizingDecider);

            // Then
            StepVerifier.create(result).expectError(IllegalArgumentException.class).verify();
        }
    }

    @Nested
    @DisplayName("executeAndReturnState")
    class ExecuteAndReturnState {

        @Test
        void emits_the_folded_state_after_multiple_commands() {
            // Given
            deciderApplicationService.execute(List.of(new DefineName("Jane Doe"), new ChangeName("John Doe")), nameDcbDecider()).block();

            // When
            String state = deciderApplicationService.executeAndReturnState(NoOp.INSTANCE, nameDcbDecider()).block();

            // Then
            assertThat(state).isEqualTo("John Doe");
        }
    }

    @Nested
    @DisplayName("executeAndReturnEvents")
    class ExecuteAndReturnEvents {

        @Test
        void emits_the_new_events_for_multiple_commands_in_order() {
            // When
            List<DomainEvent> newEvents = deciderApplicationService.executeAndReturnEvents(List.of(new DefineName("Jane Doe"), new ChangeName("John Doe")), nameDcbDecider()).block();

            // Then
            assertThat(newEvents).containsExactly(
                    new NameDefined("event-1", time, "name", "Jane Doe"),
                    new NameWasChanged("event-2", time, "name", "John Doe")
            );
            assertThat(readNameEvents("name")).containsExactlyElementsOf(newEvents);
        }
    }

    // executeAndReturnState cannot carry a null state, so this decider uses a non-null String, with an empty string
    // for "no name yet".
    private Decider<NameCommand, String, DomainEvent> nameDecider() {
        return Decider.create(
                "",
                (NameCommand command, String state) -> switch (command) {
                    case DefineName defineName -> state.isEmpty() ? List.of(new NameDefined("event-1", time, "name", defineName.name())) : List.of();
                    case ChangeName changeName -> List.of(new NameWasChanged("event-2", time, "name", changeName.name()));
                    case NoOp __ -> List.of();
                },
                (state, event) -> event.name()
        );
    }

    private DcbDecider<NameCommand, String, DomainEvent> nameDcbDecider() {
        return DcbDecider.from(nameDecider(), command -> nameQuery("name"), event -> Set.of(tagFor(event)));
    }

    private void append(DomainEvent... events) {
        List<Tag> tags = List.of(Tag.of("name", "name"));
        var cloudEvents = converter.toCloudEvents(List.of(events)).stream()
                .map(event -> DcbCloudEvents.withTags(event, tags))
                .toList();
        eventStore.append(cloudEvents).block();
    }

    private List<DomainEvent> readNameEvents(String nameId) {
        return converter.toDomainEvents(requireNonNull(eventStore.read(nameQuery(nameId)).block()).events().stream()).toList();
    }

    private static DcbCriteria nameQuery(String nameId) {
        return DcbCriteria.tags(Tag.of("name", nameId));
    }

    private static Tag tagFor(DomainEvent event) {
        return Tag.of("name", event.userId());
    }

    private sealed interface NameCommand {
    }

    private record DefineName(String name) implements NameCommand {
    }

    private record ChangeName(String name) implements NameCommand {
    }

    private enum NoOp implements NameCommand {
        INSTANCE
    }
}
