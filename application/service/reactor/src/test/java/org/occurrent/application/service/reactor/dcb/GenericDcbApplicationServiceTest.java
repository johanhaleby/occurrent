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

package org.occurrent.application.service.reactor.dcb;

import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.dcb.DcbQuery.tags;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class GenericDcbApplicationServiceTest {

    @Container
    private static final MongoDBContainer mongoDBContainer;

    static {
        mongoDBContainer = new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet();
        List<String> ports = new ArrayList<>();
        ports.add("27017:27017");
        mongoDBContainer.withReuse(true).setPortBindings(ports);
    }

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcbappservice"));

    private ReactorMongoEventStore eventStore;

    @BeforeEach
    void create_reactive_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcbappservice");
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
    }

    @Test
    void reads_by_dcb_query_and_appends_with_tags_from_domain_events() {
        eventStore.append(List.of(DcbCloudEvents.withTags(converter().toCloudEvent(new DomainEvent("NameDefined", "name:1")), Set.of("name:1")))).block();
        GenericDcbApplicationService<DomainEvent> applicationService = new GenericDcbApplicationService<>(eventStore, converter(), event -> Set.of(event.name()));

        Optional<DcbAppendResult> result = applicationService.execute(tags("name:1"), events -> {
            List<DomainEvent> currentEvents = events.toList();
            assertThat(currentEvents).extracting(DomainEvent::type).containsExactly("NameDefined");
            return Stream.of(new DomainEvent("NameChanged", "name:1"));
        }).block();

        assertThat(result).isPresent();
        assertThat(requireNonNull(result).get().eventCount()).isEqualTo(1);
        DcbEventStream eventStream = eventStore.read(tags("name:1")).block();
        assertThat(requireNonNull(eventStream).events()).extracting(CloudEvent::getType).containsExactly("NameDefined", "NameChanged");
        assertThat(DcbCloudEvents.getTags(eventStream.events().get(1))).containsExactly("name:1");
    }

    @Test
    void does_not_append_when_domain_function_returns_no_events() {
        GenericDcbApplicationService<DomainEvent> applicationService = new GenericDcbApplicationService<>(eventStore, converter(), event -> Set.of(event.name()));

        StepVerifier.create(applicationService.execute(tags("name:1"), events -> Stream.empty()))
                .expectNext(Optional.empty())
                .verifyComplete();

        assertThat(requireNonNull(eventStore.read(tags("name:1")).block()).events()).isEmpty();
    }

    @Test
    void runs_the_side_effect_once_after_a_successful_append() {
        GenericDcbApplicationService<DomainEvent> applicationService = new GenericDcbApplicationService<>(eventStore, converter(), event -> Set.of(event.name()));
        AtomicInteger sideEffectInvocations = new AtomicInteger();

        applicationService.execute(tags("name:1"),
                DcbExecuteOptions.<DomainEvent>options().sideEffect(events -> Mono.fromRunnable(() -> {
                    assertThat(events.map(DomainEvent::type).toList()).containsExactly("NameDefined");
                    sideEffectInvocations.incrementAndGet();
                })),
                events -> Stream.of(new DomainEvent("NameDefined", "name:1"))).block();

        assertThat(sideEffectInvocations).hasValue(1);
    }

    @Test
    void does_not_run_the_side_effect_when_no_events_are_produced() {
        GenericDcbApplicationService<DomainEvent> applicationService = new GenericDcbApplicationService<>(eventStore, converter(), event -> Set.of(event.name()));
        AtomicInteger sideEffectInvocations = new AtomicInteger();

        applicationService.execute(tags("name:1"),
                DcbExecuteOptions.<DomainEvent>options().sideEffect(events -> Mono.fromRunnable(sideEffectInvocations::incrementAndGet)),
                events -> Stream.empty()).block();

        assertThat(sideEffectInvocations).hasValue(0);
    }

    @Test
    void retries_from_a_fresh_dcb_read_when_append_condition_detects_a_conflict() {
        eventStore.append(List.of(DcbCloudEvents.withTags(converter().toCloudEvent(new DomainEvent("NameDefined", "name:1")), Set.of("name:1")))).block();
        CloudEvent conflictingEvent = DcbCloudEvents.withTags(converter().toCloudEvent(new DomainEvent("NameChangedByOther", "name:1")), Set.of("name:1"));
        ConflictingOnceDcbEventStore conflictingStore = new ConflictingOnceDcbEventStore(eventStore, conflictingEvent);
        AtomicInteger attempts = new AtomicInteger();
        GenericDcbApplicationService<DomainEvent> applicationService = new GenericDcbApplicationService<>(conflictingStore, converter(), event -> Set.of(event.name()));

        Optional<DcbAppendResult> result = applicationService.execute(tags("name:1"), events -> {
            int attempt = attempts.incrementAndGet();
            List<DomainEvent> currentEvents = events.toList();
            if (attempt == 1) {
                assertThat(currentEvents).extracting(DomainEvent::type).containsExactly("NameDefined");
            } else {
                assertThat(currentEvents).extracting(DomainEvent::type).containsExactly("NameDefined", "NameChangedByOther");
            }
            return Stream.of(new DomainEvent("NameChangedByService", "name:1"));
        }).block();

        assertThat(result).isPresent();
        assertThat(attempts).hasValue(2);
        assertThat(requireNonNull(eventStore.read(tags("name:1")).block()).events())
                .extracting(CloudEvent::getType)
                .containsExactly("NameDefined", "NameChangedByOther", "NameChangedByService");
    }

    private static CloudEventConverter<DomainEvent> converter() {
        return new CloudEventConverter<>() {
            @Override
            public CloudEvent toCloudEvent(DomainEvent domainEvent) {
                return CloudEventBuilder.v1()
                        .withId(UUID.randomUUID().toString())
                        .withSource(URI.create("urn:test"))
                        .withType(domainEvent.type())
                        .withData(domainEvent.name().getBytes(UTF_8))
                        .build();
            }

            @Override
            public DomainEvent toDomainEvent(CloudEvent cloudEvent) {
                return new DomainEvent(cloudEvent.getType(), new String(requireNonNull(cloudEvent.getData()).toBytes(), UTF_8));
            }

            @Override
            public String getCloudEventType(Class<? extends DomainEvent> type) {
                return type.getName();
            }
        };
    }

    private record DomainEvent(String type, String name) {
    }

    /**
     * A reactive DCB event store that, on the first conditional append, first commits a conflicting matching event so
     * the carried consistency token is stale and the append fails once, then delegates normally on later attempts.
     */
    private static class ConflictingOnceDcbEventStore implements DcbEventStore {
        private final DcbEventStore delegate;
        private final CloudEvent conflictingEvent;
        private final AtomicBoolean conflictInserted = new AtomicBoolean();

        private ConflictingOnceDcbEventStore(DcbEventStore delegate, CloudEvent conflictingEvent) {
            this.delegate = delegate;
            this.conflictingEvent = conflictingEvent;
        }

        @Override
        public Mono<DcbEventStream> read(DcbQuery query, DcbReadOptions options) {
            return delegate.read(query, options);
        }

        @Override
        public Mono<DcbAppendResult> append(List<CloudEvent> events) {
            return delegate.append(events);
        }

        @Override
        public Mono<DcbAppendResult> append(List<CloudEvent> events, DcbAppendCondition condition) {
            if (conflictInserted.compareAndSet(false, true)) {
                return delegate.append(List.of(conflictingEvent)).then(delegate.append(events, condition));
            }
            return delegate.append(events, condition);
        }
    }
}
