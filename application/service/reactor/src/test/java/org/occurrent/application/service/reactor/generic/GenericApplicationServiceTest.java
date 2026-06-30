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
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.generic.GenericCloudEventConverter;
import org.occurrent.application.service.ExecuteFilter;
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.application.service.reactor.ExecuteOptions;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.EventStream;
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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.application.service.reactor.ExecuteOptions.options;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class GenericApplicationServiceTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet().withReuse(true);

    @RegisterExtension
    FlushMongoDBExtension flush = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".appservice"));

    private ReactorMongoEventStore eventStore;
    private CloudEventConverter<DomainEvent> converter;
    private ApplicationService<DomainEvent> applicationService;

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
        applicationService = new GenericApplicationService<>(eventStore, converter);
    }

    @Test
    void writes_decider_produced_events_and_returns_the_write_result() {
        String streamId = UUID.randomUUID().toString();

        WriteResult result = applicationService.execute(streamId, options(),
                events -> Stream.of(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Johan"))).block();

        assertThat(requireNonNull(result).streamId()).isEqualTo(streamId);
        assertThat(result.newStreamVersion()).isEqualTo(1L);
        assertThat(readNames(streamId)).containsExactly("Johan");
    }

    @Test
    void supports_a_read_filter_and_a_reactive_side_effect() {
        String streamId = UUID.randomUUID().toString();
        write(streamId,
                new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Johan"),
                new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Mattias"));
        AtomicReference<String> sideEffectPayload = new AtomicReference<>("not-called");
        AtomicReference<Long> typesSeenByDomain = new AtomicReference<>(0L);

        WriteResult result = applicationService.execute(streamId,
                ExecuteOptions.<DomainEvent>options().filter(ExecuteFilter.type(NameDefined.class))
                        .sideEffect(events -> Mono.fromRunnable(() -> sideEffectPayload.set(events.findFirst().map(DomainEvent::name).orElse("empty")))),
                events -> {
                    typesSeenByDomain.set(events.count());
                    return Stream.of(new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "New Name"));
                }).block();

        // The read was filtered to NameDefined, so the domain function only saw that one event. The write still appends
        // to the full stream (version 2 -> 3), and the reactive side-effect ran once with the new event.
        assertThat(typesSeenByDomain.get()).isEqualTo(1L);
        assertThat(requireNonNull(result).newStreamVersion()).isEqualTo(3L);
        assertThat(sideEffectPayload.get()).isEqualTo("New Name");
    }

    @Test
    void supports_execute_filter_as_a_direct_argument() {
        String streamId = UUID.randomUUID().toString();
        write(streamId,
                new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Johan"),
                new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Mattias"));

        WriteResult result = applicationService.execute(streamId, ExecuteFilter.type(NameDefined.class),
                events -> Stream.of(new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "New Name"))).block();

        assertThat(requireNonNull(result).newStreamVersion()).isEqualTo(3L);
    }

    @Test
    void runs_the_side_effect_after_execute_even_when_no_events_are_produced() {
        // Parity with the blocking GenericApplicationService: the side-effect runs once after a successful execute,
        // with the new events (an empty stream here), regardless of whether the domain function produced any.
        String streamId = UUID.randomUUID().toString();
        AtomicInteger sideEffectInvocations = new AtomicInteger();

        applicationService.execute(streamId,
                options().sideEffect(events -> Mono.fromRunnable(sideEffectInvocations::incrementAndGet)),
                events -> Stream.empty()).block();

        assertThat(sideEffectInvocations).hasValue(1);
        assertThat(readNames(streamId)).isEmpty();
    }

    @Test
    void retries_from_a_fresh_read_when_the_write_condition_is_not_fulfilled() {
        String streamId = UUID.randomUUID().toString();
        write(streamId, new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Johan"));
        CloudEvent interloper = converter.toCloudEvent(new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Interloper"));
        ApplicationService<DomainEvent> service = new GenericApplicationService<>(new ConflictingOnceEventStore(eventStore, interloper), converter);
        AtomicInteger attempts = new AtomicInteger();

        WriteResult result = service.execute(streamId, options(),
                events -> {
                    attempts.incrementAndGet();
                    return Stream.of(new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "New Name"));
                }).block();

        // The first write conflicts because the interloper bumped the stream version, the retry reads the fresh stream
        // and writes successfully.
        assertThat(attempts).hasValue(2);
        assertThat(requireNonNull(result)).isNotNull();
        assertThat(readNames(streamId)).containsExactly("Johan", "Interloper", "New Name");
    }

    private void write(String streamId, DomainEvent... events) {
        eventStore.write(streamId, Flux.fromIterable(converter.toCloudEvents(Stream.of(events)).toList())).block();
    }

    private List<String> readNames(String streamId) {
        return requireNonNull(eventStore.read(streamId).flatMapMany(EventStream::events).map(converter::toDomainEvent).map(DomainEvent::name).collectList().block());
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
                GenericApplicationServiceTest::customCloudEventType
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

    /**
     * A reactive event store that, on the first conditional write, first commits a conflicting event so the carried
     * stream version is stale and the write fails once, then delegates normally on later attempts.
     */
    private static final class ConflictingOnceEventStore implements EventStore {
        private final EventStore delegate;
        private final CloudEvent conflictingEvent;
        private final AtomicBoolean conflictInserted = new AtomicBoolean();

        private ConflictingOnceEventStore(EventStore delegate, CloudEvent conflictingEvent) {
            this.delegate = delegate;
            this.conflictingEvent = conflictingEvent;
        }

        @Override
        public Mono<EventStream<CloudEvent>> read(String streamId, int skip, int limit) {
            return delegate.read(streamId, skip, limit);
        }

        @Override
        public Mono<WriteResult> write(String streamId, WriteCondition writeCondition, Flux<CloudEvent> events) {
            if (conflictInserted.compareAndSet(false, true)) {
                return delegate.write(streamId, Flux.just(conflictingEvent)).then(delegate.write(streamId, writeCondition, events));
            }
            return delegate.write(streamId, writeCondition, events);
        }

        @Override
        public Mono<WriteResult> write(String streamId, Flux<CloudEvent> events) {
            return delegate.write(streamId, events);
        }

        @Override
        public Mono<Boolean> exists(String streamId) {
            return delegate.exists(streamId);
        }
    }
}
