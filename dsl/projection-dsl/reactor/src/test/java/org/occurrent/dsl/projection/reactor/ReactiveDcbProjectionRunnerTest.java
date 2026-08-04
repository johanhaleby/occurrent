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

package org.occurrent.dsl.projection.reactor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.dsl.projection.DcbProjection;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static java.time.Duration.ofSeconds;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves the reactor {@link ReactiveDcbProjectionRunner} threads real DCB delivery metadata into the fold, rather than
 * {@link org.occurrent.cloudevents.EventMetadata#empty()}, mirroring how {@code DcbReactorSubscriptionsTest} proves
 * the same for the lower-level {@code DcbSubscriptions}. Uses a real DCB-capable {@link ReactorMongoEventStore}
 * (Testcontainers), since the metadata only exists once an event has actually round-tripped through a store.
 */
@Timeout(30)
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactiveDcbProjectionRunnerTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flush = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer, "projectiondcbrunner"));

    private ReactorMongoEventStore eventStore;
    private ReactorMongoSubscriptionModel subscriptionModel;
    private CloudEventConverter<DomainEvent> converter;

    @BeforeEach
    void createInstances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("projectiondcbrunner"));
        MongoClient mongoClient = MongoClients.create(connectionString);
        ReactiveMongoTemplate mongoTemplate = new ReactiveMongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        ReactiveMongoTransactionManager tx = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(tx)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(EventStoreCapability.STREAM, EventStoreCapability.DCB)
                .build();
        eventStore = new ReactorMongoEventStore(mongoTemplate, config);
        subscriptionModel = new ReactorMongoSubscriptionModel(mongoTemplate, "events", TimeRepresentation.RFC_3339_STRING);
        converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
    }

    @Test
    void the_viewstaterepository_overload_folds_the_events_real_dcb_metadata_instead_of_empty_metadata() {
        ConcurrentHashMap<String, Long> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Long, String> repository = ViewStateRepository.create(repo::get, repo::put);
        // Keyed by the delivered stream id and folded to the delivered stream version: both accessors throw on
        // EventMetadata.empty(), so this only succeeds if the runner actually carries real DCB metadata into the fold
        // instead of routing through the plain, metadata-less subscribe path.
        Projection<Long, DomainEvent, String> projection = Projection.<Long, DomainEvent, String>builder(0L)
                .id((metadata, event) -> metadata.getStreamId())
                .on(NameDefined.class, (state, metadata, event) -> metadata.getStreamVersion())
                .build();
        DcbProjection<Long, DomainEvent, String> dcbProjection = new DcbProjection<>(projection, DcbCriteria.tags(Tag.parse("entity:alice")));
        ReactiveDcbProjectionRunner<DomainEvent> runner = ReactiveDcbProjectionRunner.create(subscriptionModel, converter);
        runner.project("alice-projection", dcbProjection, repository).waitUntilStarted().block();

        appendTagged(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "alice", "Alice"), "entity:alice");

        await().atMost(ofSeconds(5)).untilAsserted(() -> {
            assertThat(repo).hasSize(1);
            assertThat(repo.values()).containsExactly(1L);
        });
    }

    @Test
    void the_bifunction_overload_exposes_real_dcb_metadata_to_the_update_function() {
        ConcurrentHashMap<String, Long> repo = new ConcurrentHashMap<>();
        // This overload never calls dcbProjection.projection(), only the criteria, which scopes the subscription.
        DcbProjection<Long, DomainEvent, String> dcbProjection = new DcbProjection<>(
                Projection.<Long, DomainEvent, String>builder(0L).id(event -> "singleton").build(),
                DcbCriteria.tags(Tag.parse("entity:bob")));
        ReactiveDcbProjectionRunner<DomainEvent> runner = ReactiveDcbProjectionRunner.create(subscriptionModel, converter);
        // getStreamVersion() throws on EventMetadata.empty(), so this only passes if real DCB metadata reaches the
        // BiFunction instead of the plain, metadata-less subscribe path.
        runner.project("bob-projection", dcbProjection, (metadata, event) -> {
            repo.put(metadata.getStreamId(), metadata.getStreamVersion());
            return Mono.empty();
        }).waitUntilStarted().block();

        appendTagged(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "bob", "Bob"), "entity:bob");

        await().atMost(ofSeconds(5)).untilAsserted(() -> {
            assertThat(repo).hasSize(1);
            assertThat(repo.values()).containsExactly(1L);
        });
    }

    @Test
    void the_materializedview_overload_folds_the_events_real_dcb_metadata_instead_of_empty_metadata() {
        ConcurrentHashMap<String, Long> repo = new ConcurrentHashMap<>();
        // Overridden by hand rather than built with MaterializedView.create, so update(EventMetadata, E) is the only
        // path that can populate repo: getStreamId()/getStreamVersion() throw on EventMetadata.empty(), so this only
        // succeeds if the runner carries real DCB metadata into the view instead of the plain, metadata-less path.
        MaterializedView<DomainEvent> view = new MaterializedView<>() {
            @Override
            public void update(DomainEvent event) {
                throw new AssertionError("expected the metadata-carrying update(EventMetadata, E) to be called, not update(E)");
            }

            @Override
            public void update(EventMetadata metadata, DomainEvent event) {
                repo.put(metadata.getStreamId(), metadata.getStreamVersion());
            }
        };
        DcbProjection<Long, DomainEvent, String> dcbProjection = new DcbProjection<>(
                Projection.<Long, DomainEvent, String>builder(0L).id(event -> "singleton").build(),
                DcbCriteria.tags(Tag.parse("entity:carol")));
        ReactiveDcbProjectionRunner<DomainEvent> runner = ReactiveDcbProjectionRunner.create(subscriptionModel, converter);
        runner.project("carol-projection", dcbProjection, view).waitUntilStarted().block();

        appendTagged(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "carol", "Carol"), "entity:carol");

        await().atMost(ofSeconds(5)).untilAsserted(() -> {
            assertThat(repo).hasSize(1);
            assertThat(repo.values()).containsExactly(1L);
        });
    }

    private void appendTagged(DomainEvent event, String tag) {
        CloudEvent cloudEvent = converter.toCloudEvent(event);
        eventStore.append(List.of(DcbCloudEvents.withTags(cloudEvent, List.of(Tag.parse(tag))))).block();
    }
}
