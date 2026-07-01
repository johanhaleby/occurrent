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
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.dsl.dcb.DcbDomainEventStream;
import org.occurrent.dsl.query.reactor.DomainEventQueries;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.reactor.EventStoreQueries;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbDomainEventQueriesTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet().withReuse(true);

    @RegisterExtension
    FlushMongoDBExtension flush = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcbquerydsl"));

    private ReactorMongoEventStore eventStore;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private DcbDomainEventQueries<DomainEvent> dcbQueries;
    private LocalDateTime time;

    @BeforeEach
    void createInstances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcbquerydsl");
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
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        dcbQueries = new DcbDomainEventQueries<>(new DomainEventQueries<>(eventStore, cloudEventConverter));
        time = LocalDateTime.now();
    }

    @Test
    void query_converts_matching_dcb_events_to_domain_events() {
        NameDefined nameDefined = new NameDefined("eventId1", time, "name", "Some Doe");
        NameWasChanged nameWasChanged = new NameWasChanged("eventId2", time, "name", "Jane Doe");
        append("name:1", nameDefined, nameWasChanged);

        List<DomainEvent> events = dcbQueries.query(DcbQuery.tags("name:1")).collectList().block();

        assertThat(events).containsExactly(nameDefined, nameWasChanged);
    }

    @Test
    void query_honors_read_options_after_sequence_position() {
        NameDefined nameDefined = new NameDefined("eventId1", time, "name", "Some Doe");
        NameWasChanged nameWasChanged = new NameWasChanged("eventId2", time, "name", "Jane Doe");
        append("name:1", nameDefined);
        append("name:1", nameWasChanged);

        List<DomainEvent> events = dcbQueries.query(DcbQuery.tags("name:1"), DcbReadOptions.afterSequencePosition(1)).collectList().block();

        assertThat(events).containsExactly(nameWasChanged);
    }

    @Test
    void query_with_position_preserves_last_sequence_position() {
        NameDefined nameDefined = new NameDefined("eventId1", time, "name", "Some Doe");
        append("name:1", nameDefined);
        append("other:1", new NameWasChanged("eventId2", time, "name", "Jane Doe"));

        DcbDomainEventStream<DomainEvent> eventStream = dcbQueries.queryWithPosition(DcbQuery.tags("name:1")).block();

        assertThat(eventStream.events()).containsExactly(nameDefined);
        assertThat(eventStream.stream()).containsExactly(nameDefined);
        assertThat(eventStream.lastSequencePosition()).isEqualTo(2);
    }

    @Test
    void query_with_position_exposes_a_usable_consistency_token() {
        append("name:1", new NameDefined("eventId1", time, "name", "Some Doe"));

        DcbDomainEventStream<DomainEvent> eventStream = dcbQueries.queryWithPosition(DcbQuery.tags("name:1")).block();
        DcbConsistencyToken token = eventStream.consistencyToken();
        assertThat(token).isNotNull();

        // A matching event committed after the DSL read invalidates the token, so a conditional append carrying it back
        // to the store is correctly rejected. This proves the token flows through the DSL projection, not just the position.
        append("name:1", new NameWasChanged("eventId2", time, "name", "Jane Doe"));
        List<CloudEvent> newEvents = cloudEventConverter.toCloudEvents(Stream.of(new NameWasChanged("eventId3", time, "name", "Joe Doe")))
                .map(event -> DcbCloudEvents.withTags(event, List.of("name:1")))
                .toList();

        assertThatThrownBy(() -> eventStore.append(newEvents, DcbAppendCondition.failIfEventsMatch(DcbQuery.tags("name:1"), token)).block())
                .isInstanceOf(DcbAppendConditionNotFulfilledException.class);
    }

    @Test
    void delegates_plain_filter_and_type_queries_to_the_wrapped_domain_event_queries() {
        NameDefined nameDefined = new NameDefined("eventId1", time, "name", "Some Doe");
        NameWasChanged nameWasChanged = new NameWasChanged("eventId2", time, "name", "Jane Doe");
        append("name:1", nameDefined, nameWasChanged);

        List<NameWasChanged> byType = dcbQueries.query(NameWasChanged.class).collectList().block();
        List<DomainEvent> byFilter = dcbQueries.<DomainEvent>query(Filter.all()).collectList().block();
        NameDefined one = dcbQueries.queryOne(NameDefined.class).block();

        assertThat(byType).containsExactly(nameWasChanged);
        assertThat(byFilter).containsExactly(nameDefined, nameWasChanged);
        assertThat(one).isEqualTo(nameDefined);
    }

    @Test
    void delegates_count_and_exists_to_the_wrapped_domain_event_queries() {
        append("name:1", new NameDefined("eventId1", time, "name", "Some Doe"));

        Long count = dcbQueries.count().block();
        Boolean exists = dcbQueries.exists(Filter.type(NameDefined.class.getName())).block();

        assertThat(count).isEqualTo(1L);
        assertThat(exists).isTrue();
    }

    @Test
    void constructor_rejects_a_domain_event_queries_not_backed_by_a_reactive_dcb_event_store() {
        DomainEventQueries<DomainEvent> wrapped = new DomainEventQueries<>(new StreamOnlyEventStoreQueries(), cloudEventConverter);

        assertThatThrownBy(() -> new DcbDomainEventQueries<>(wrapped))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DCB queries require");
    }

    private static final class StreamOnlyEventStoreQueries implements EventStoreQueries {
        @Override
        public Flux<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
            return Flux.empty();
        }

        @Override
        public Mono<Long> count(Filter filter) {
            return Mono.just(0L);
        }

        @Override
        public Mono<Boolean> exists(Filter filter) {
            return Mono.just(false);
        }
    }

    private void append(String tag, DomainEvent... events) {
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(Stream.of(events))
                .map(event -> DcbCloudEvents.withTags(event, List.of(tag)))
                .toList();
        eventStore.append(cloudEvents).block();
    }
}
