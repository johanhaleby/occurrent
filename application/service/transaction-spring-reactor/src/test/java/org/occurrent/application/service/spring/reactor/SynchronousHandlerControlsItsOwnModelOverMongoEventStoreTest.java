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

package org.occurrent.application.service.spring.reactor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.generic.GenericCloudEventConverter;
import org.occurrent.application.service.reactor.generic.GenericApplicationService;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.DomainEventConverter;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.reactor.EventStream;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.synchronous.reactor.SynchronousSubscriptionModel;
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

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * A synchronous handler that pauses its own subscription or stops its own model while the write that dispatched to it is
 * still open. On the reactive Mongo store the handler runs on a driver callback thread, which is neither the thread that
 * opened the write window nor a Reactor non-blocking thread, so waiting for that window to close would wait for the
 * handler itself. The write must complete and the call must return.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class SynchronousHandlerControlsItsOwnModelOverMongoEventStoreTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(15);
    private static final String SUBSCRIPTION_ID = "self-controlling";

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flush = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private ReactorMongoEventStore eventStore;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private SynchronousSubscriptionModel model;
    private String streamId;
    private final AtomicBoolean handlerRan = new AtomicBoolean();
    private final AtomicBoolean handlerReturned = new AtomicBoolean();

    @BeforeEach
    void initialize() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".synchandlercontrolsitsownmodel");
        mongoClient = MongoClients.create(connectionString);
        SimpleReactiveMongoDatabaseFactory databaseFactory = new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase()));
        ReactiveMongoTransactionManager transactionManager = new ReactiveMongoTransactionManager(databaseFactory);
        ReactiveMongoTemplate mongoTemplate = new ReactiveMongoTemplate(databaseFactory);
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM)
                .build();
        eventStore = new ReactorMongoEventStore(mongoTemplate, config);
        DomainEventConverter domainEventConverter = new DomainEventConverter(new ObjectMapper());
        cloudEventConverter = new GenericCloudEventConverter<>(domainEventConverter::convertToDomainEvent, domainEventConverter::convertToCloudEvent);
        model = new SynchronousSubscriptionModel();
        streamId = UUID.randomUUID().toString();
    }

    @AfterEach
    void closeClient() {
        mongoClient.close();
    }

    @Test
    void a_handler_that_pauses_its_own_subscription_does_not_hold_up_the_write_that_dispatched_to_it() {
        model.subscribe(SUBSCRIPTION_ID, event -> Mono.fromRunnable(() -> {
            model.pauseSubscription(SUBSCRIPTION_ID);
            handlerReturned.set(true);
        }));
        var applicationService = GenericApplicationService.builder(eventStore, cloudEventConverter)
                .synchronousSubscriptions(model)
                .build();

        applicationService.execute(streamId, this::decide).block(TIMEOUT);

        assertThat(handlerReturned).as("[handler returned from pauseSubscription]").isTrue();
        assertThat(eventsInStream()).as("[events in the stream]").hasSize(1);
        assertThat(model.isPaused(SUBSCRIPTION_ID)).as("[subscription paused once the write is done]").isTrue();
    }

    @Test
    void a_handler_that_stops_its_own_model_does_not_hold_up_the_write_that_dispatched_to_it() {
        model.subscribe(SUBSCRIPTION_ID, event -> Mono.fromRunnable(() -> {
            model.stop();
            handlerReturned.set(true);
        }));
        var applicationService = GenericApplicationService.builder(eventStore, cloudEventConverter)
                .synchronousSubscriptions(model)
                .build();

        applicationService.execute(streamId, this::decide).block(TIMEOUT);

        assertThat(handlerReturned).as("[handler returned from stop]").isTrue();
        assertThat(eventsInStream()).as("[events in the stream]").hasSize(1);
        assertThat(model.isRunning()).as("[model stopped once the write is done]").isFalse();
    }

    @Test
    void control_a_handler_that_does_not_touch_its_model_runs_and_the_write_completes() {
        model.subscribe(SUBSCRIPTION_ID, event -> Mono.fromRunnable(() -> handlerRan.set(true)));
        var applicationService = GenericApplicationService.builder(eventStore, cloudEventConverter)
                .synchronousSubscriptions(model)
                .build();

        applicationService.execute(streamId, this::decide).block(TIMEOUT);

        assertThat(handlerRan).as("[handler ran]").isTrue();
        assertThat(eventsInStream()).as("[events in the stream]").hasSize(1);
    }

    private List<DomainEvent> decide(List<DomainEvent> events) {
        return List.of(new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Johan"));
    }

    private List<String> eventsInStream() {
        return requireNonNull(eventStore.read(streamId).flatMapMany(EventStream::events).map(CloudEvent::getId).collectList().block(TIMEOUT));
    }
}
