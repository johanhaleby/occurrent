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

package org.occurrent.application.service.reactor.generic;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.generic.GenericCloudEventConverter;
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.application.service.reactor.ReactiveSynchronousEventDispatcher;
import org.occurrent.application.service.reactor.ReactiveTransactionExecutor;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
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
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
class ReactiveGenericApplicationServiceSynchronousSubscriptionsTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet().withReuse(true);

    @RegisterExtension
    FlushMongoDBExtension flush = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".appservice"));

    private ReactorMongoEventStore eventStore;
    private CloudEventConverter<DomainEvent> converter;

    @BeforeEach
    void create_instances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".appservice");
        MongoClient mongoClient = MongoClients.create(connectionString);
        ReactiveMongoTemplate mongoTemplate = new ReactiveMongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        ReactiveMongoTransactionManager tx = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(tx)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM)
                .build();
        eventStore = new ReactorMongoEventStore(mongoTemplate, config);
        converter = customCloudEventConverter();
    }

    @Test
    void dispatches_written_events_synchronously_before_execute_returns_and_enriched_with_stream_version() {
        RecordingDispatcher dispatcher = new RecordingDispatcher();
        ApplicationService<DomainEvent> applicationService = GenericApplicationService.builder(eventStore, converter)
                .synchronousSubscriptions(dispatcher)
                .build();
        String streamId = UUID.randomUUID().toString();

        StepVerifier.create(applicationService.execute(streamId, events -> List.of(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Johan"))))
                .expectNextCount(1)
                .verifyComplete();

        assertThat(dispatcher.dispatched).hasSize(1);
        CloudEvent dispatched = dispatcher.dispatched.getFirst();
        // The store enriches on write; the dispatched events are the re-read, enriched ones, not the pre-write converter output.
        assertThat(dispatched.getExtension("streamversion")).isEqualTo(1L);
    }

    @Test
    void does_not_dispatch_when_no_synchronous_subscriptions_are_registered() {
        RecordingDispatcher dispatcher = new RecordingDispatcher();
        dispatcher.hasSubscriptions = false;
        ApplicationService<DomainEvent> applicationService = GenericApplicationService.builder(eventStore, converter)
                .synchronousSubscriptions(dispatcher)
                .build();

        StepVerifier.create(applicationService.execute(UUID.randomUUID().toString(), events -> List.of(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Johan"))))
                .expectNextCount(1)
                .verifyComplete();

        assertThat(dispatcher.dispatched).isEmpty();
    }

    @Test
    void does_not_dispatch_when_the_command_produces_no_events() {
        RecordingDispatcher dispatcher = new RecordingDispatcher();
        ApplicationService<DomainEvent> applicationService = GenericApplicationService.builder(eventStore, converter)
                .synchronousSubscriptions(dispatcher)
                .build();

        StepVerifier.create(applicationService.execute(UUID.randomUUID().toString(), events -> List.<DomainEvent>of()))
                .expectNextCount(1)
                .verifyComplete();

        assertThat(dispatcher.dispatched).isEmpty();
    }

    @Test
    void tells_the_dispatcher_there_is_no_transaction_when_the_default_executor_is_used() {
        RegimeRecordingDispatcher dispatcher = new RegimeRecordingDispatcher();
        ApplicationService<DomainEvent> applicationService = GenericApplicationService.builder(eventStore, converter)
                .synchronousSubscriptions(dispatcher)
                .build();

        StepVerifier.create(applicationService.execute(UUID.randomUUID().toString(), events -> List.of(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Johan"))))
                .expectNextCount(1)
                .verifyComplete();

        // The default is ReactiveTransactionExecutor.noTransaction(), so handlers must be isolated from each other.
        assertThat(dispatcher.toldTransactional).isFalse();
    }

    @Test
    void tells_the_dispatcher_there_is_a_transaction_when_the_executor_says_so() {
        RegimeRecordingDispatcher dispatcher = new RegimeRecordingDispatcher();
        ApplicationService<DomainEvent> applicationService = GenericApplicationService.builder(eventStore, converter)
                .synchronousSubscriptions(dispatcher)
                .transactionExecutor(new ReactiveTransactionExecutor() {
                    @Override
                    public <T> Mono<T> inTransaction(Supplier<Mono<T>> action) {
                        return Mono.defer(action);
                    }

                    @Override
                    public Mono<Boolean> isTransactional() {
                        return Mono.just(true);
                    }
                })
                .build();

        StepVerifier.create(applicationService.execute(UUID.randomUUID().toString(), events -> List.of(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Johan"))))
                .expectNextCount(1)
                .verifyComplete();

        assertThat(dispatcher.toldTransactional).isTrue();
    }

    private static CloudEventConverter<DomainEvent> customCloudEventConverter() {
        ObjectMapper objectMapper = new ObjectMapper();
        return new GenericCloudEventConverter<DomainEvent>(
                cloudEvent -> {
                    try {
                        return switch (cloudEvent.getType()) {
                            case "name-defined-v1" -> objectMapper.readValue(requireNonNull(cloudEvent.getData()).toBytes(), NameDefined.class);
                            case "name-was-changed-v1" -> objectMapper.readValue(requireNonNull(cloudEvent.getData()).toBytes(), NameWasChanged.class);
                            default -> throw new IllegalArgumentException("Unsupported event type " + cloudEvent.getType());
                        };
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                },
                event -> {
                    try {
                        return CloudEventBuilder.v1()
                                .withId(event.eventId())
                                .withSource(URI.create("http://name"))
                                .withType(customCloudEventType(event.getClass()))
                                .withTime(event.timestamp().toInstant().atOffset(ZoneOffset.UTC))
                                .withSubject(event.name())
                                .withDataContentType("application/json")
                                .withData(objectMapper.writeValueAsBytes(event))
                                .build();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                },
                ReactiveGenericApplicationServiceSynchronousSubscriptionsTest::customCloudEventType
        );
    }

    private static String customCloudEventType(Class<? extends DomainEvent> type) {
        if (type.equals(NameDefined.class)) {
            return "name-defined-v1";
        } else if (type.equals(NameWasChanged.class)) {
            return "name-was-changed-v1";
        }
        throw new IllegalArgumentException("Unsupported event type " + type.getName());
    }

    private static final class RecordingDispatcher implements ReactiveSynchronousEventDispatcher {
        private final List<CloudEvent> dispatched = new ArrayList<>();
        private boolean hasSubscriptions = true;

        @Override
        public Mono<Void> dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
            return Mono.fromRunnable(() -> dispatched.addAll(writtenCloudEvents));
        }

        @Override
        public boolean hasSubscriptions() {
            return hasSubscriptions;
        }
    }

    private static final class RegimeRecordingDispatcher implements ReactiveSynchronousEventDispatcher {
        private @Nullable Boolean toldTransactional;

        @Override
        public Mono<Void> dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
            return Mono.fromRunnable(() -> toldTransactional = transactional);
        }

        @Override
        public boolean hasSubscriptions() {
            return true;
        }
    }
}
