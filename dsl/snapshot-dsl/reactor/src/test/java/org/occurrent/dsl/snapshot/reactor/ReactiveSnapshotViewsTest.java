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

package org.occurrent.dsl.snapshot.reactor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.application.service.reactor.generic.GenericApplicationService;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.dsl.snapshot.SnapshotPolicy;
import org.occurrent.dsl.snapshot.SnapshotView;
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

import java.net.URI;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

@Testcontainers
@DisplayName("ReactiveSnapshotViews")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ReactiveSnapshotViewsTest {

    @Container
    private static final MongoDBContainer mongoDBContainer;

    static {
        mongoDBContainer = new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet();
        List<String> ports = new ArrayList<>();
        ports.add("27017:27017");
        mongoDBContainer.withReuse(true).setPortBindings(ports);
    }

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reactivesnapshotviews"));

    private ReactorMongoEventStore eventStore;
    private CloudEventConverter<DomainEvent> converter;
    private ApplicationService<DomainEvent> applicationService;
    private ReactiveSnapshotStore<String> store;
    private SnapshotView<String, DomainEvent> snapshotView;
    private LocalDateTime time;

    @BeforeEach
    void create_instances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reactivesnapshotviews");
        MongoClient mongoClient = MongoClients.create(connectionString);
        ReactiveMongoTemplate mongoTemplate = new ReactiveMongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        ReactiveMongoTransactionManager transactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM)
                .build();
        eventStore = new ReactorMongoEventStore(mongoTemplate, config);
        converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        applicationService = new GenericApplicationService<>(eventStore, converter);
        store = ReactiveSnapshotStore.inMemory();
        time = LocalDateTime.now();
        snapshotView = SnapshotView.<String, DomainEvent>builder("")
                .on(NameDefined.class, (state, event) -> event.name())
                .on(NameWasChanged.class, (state, event) -> event.name())
                .schemaVersion(1)
                .build();
    }

    @Test
    void reads_the_current_state_and_writes_a_snapshot() {
        String streamId = UUID.randomUUID().toString();
        applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane"))).block();
        applicationService.execute(streamId, events -> List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "Janet"))).block();

        String state = ReactiveSnapshotViews.readState(eventStore, converter, streamId, snapshotView, store, SnapshotPolicy.always()).block();

        assertAll(
                () -> assertThat(state).isEqualTo("Janet"),
                () -> assertThat(store.findLatest(streamId).blockOptional()).isPresent(),
                () -> assertThat(store.findLatest(streamId).blockOptional().orElseThrow().state()).isEqualTo("Janet"),
                () -> assertThat(store.findLatest(streamId).blockOptional().orElseThrow().version()).isEqualTo(2L)
        );
    }

    @Test
    void a_second_read_resumes_from_the_snapshot() {
        String streamId = UUID.randomUUID().toString();
        applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", "Jane"))).block();
        ReactiveSnapshotViews.readState(eventStore, converter, streamId, snapshotView, store, SnapshotPolicy.always()).block();

        applicationService.execute(streamId, events -> List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "Janet"))).block();
        String state = ReactiveSnapshotViews.readState(eventStore, converter, streamId, snapshotView, store, SnapshotPolicy.always()).block();

        assertThat(state).isEqualTo("Janet");
    }
}
