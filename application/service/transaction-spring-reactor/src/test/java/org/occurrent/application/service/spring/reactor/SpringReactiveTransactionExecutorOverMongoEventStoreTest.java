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
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.generic.GenericCloudEventConverter;
import org.occurrent.application.service.reactor.ReactiveSynchronousEventDispatcher;
import org.occurrent.application.service.reactor.dcb.GenericDcbApplicationService;
import org.occurrent.application.service.reactor.generic.GenericApplicationService;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.DomainEventConverter;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.reactor.EventStream;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;

/**
 * The executor against a real {@link ReactorMongoEventStore}. A transaction it owns is undone when a synchronous handler
 * fails, so the command runs again and leaves one event and one side effect. Under {@code PROPAGATION_NOT_SUPPORTED} it
 * opens no transaction and reports none, yet the store's session synchronization is active and still rolls the write back
 * when the handler fails.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringReactiveTransactionExecutorOverMongoEventStoreTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);
    private static final String SIDE_EFFECT_COLLECTION = "handler_side_effects";
    private static final Tag TAG = Tag.parse("name:1");
    private static final DcbCriteria CRITERIA = tags(TAG);

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flush = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private ReactiveMongoTemplate mongoTemplate;
    private ReactiveMongoTransactionManager transactionManager;
    private ReactorMongoEventStore eventStore;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private String streamId;
    private final AtomicInteger decisions = new AtomicInteger();
    private final List<String> decidedEventIds = new CopyOnWriteArrayList<>();

    @BeforeEach
    void initialize() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".springreactiveexecutorovermongo");
        mongoClient = MongoClients.create(connectionString);
        SimpleReactiveMongoDatabaseFactory databaseFactory = new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase()));
        transactionManager = new ReactiveMongoTransactionManager(databaseFactory);
        mongoTemplate = new ReactiveMongoTemplate(databaseFactory);
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        eventStore = new ReactorMongoEventStore(mongoTemplate, config);
        // Created up front, since a transaction can't create the collection a handler inserts into
        mongoTemplate.collectionExists(SIDE_EFFECT_COLLECTION)
                .flatMap(exists -> exists ? Mono.empty() : mongoTemplate.createCollection(SIDE_EFFECT_COLLECTION).then())
                .block(TIMEOUT);
        DomainEventConverter domainEventConverter = new DomainEventConverter(new ObjectMapper());
        cloudEventConverter = new GenericCloudEventConverter<>(domainEventConverter::convertToDomainEvent, domainEventConverter::convertToCloudEvent);
        streamId = UUID.randomUUID().toString();
    }

    @AfterEach
    void closeClient() {
        mongoClient.close();
    }

    @Test
    void a_retried_conflict_thrown_by_a_handler_rolls_back_the_write_and_the_command_runs_again_when_the_executor_owns_the_transaction_on_a_stream() {
        WriteConditionNotFulfilledException conflict = new WriteConditionNotFulfilledException(streamId, 1, WriteCondition.anyStreamVersion());
        var applicationService = GenericApplicationService.builder(eventStore, cloudEventConverter)
                .synchronousSubscriptions(new RecordsSideEffectAndFailsOnce(conflict))
                .transactionExecutor(new SpringReactiveTransactionExecutor(transactionManager))
                .build();

        Throwable thrown = failureOf(applicationService.execute(streamId, this::decide));

        assertThat(thrown).as("[exception reaching the caller]").isNull();
        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(2);
        assertThat(sideEffects()).as("[side effects]").isEqualTo(1);
        assertThat(eventsInStream()).as("[events in the stream]").containsExactly(decidedEventIds.getLast());
    }

    @Test
    void a_retried_conflict_thrown_by_a_handler_rolls_back_the_append_and_the_command_runs_again_when_the_executor_owns_the_transaction_on_a_dcb_boundary() {
        DcbAppendConditionNotFulfilledException conflict = new DcbAppendConditionNotFulfilledException(DcbAppendCondition.failIfEventsMatch(CRITERIA), 1);
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new RecordsSideEffectAndFailsOnce(conflict))
                .transactionExecutor(new SpringReactiveTransactionExecutor(transactionManager))
                .build();

        Throwable thrown = failureOf(applicationService.execute(CRITERIA, this::decide));

        assertThat(thrown).as("[exception reaching the caller]").isNull();
        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(2);
        assertThat(sideEffects()).as("[side effects]").isEqualTo(1);
        assertThat(eventsInBoundary()).as("[events in the boundary]").containsExactly(decidedEventIds.getLast());
    }

    @Test
    void under_propagation_not_supported_the_store_rolls_back_the_write_of_a_failing_handler_although_the_executor_reports_no_transaction_so_the_command_is_not_run_again_on_a_stream() {
        WriteConditionNotFulfilledException conflict = new WriteConditionNotFulfilledException(streamId, 1, WriteCondition.anyStreamVersion());
        var applicationService = GenericApplicationService.builder(eventStore, cloudEventConverter)
                .synchronousSubscriptions(new RecordsSideEffectAndFailsOnce(conflict))
                .transactionExecutor(new SpringReactiveTransactionExecutor(operatorWith(TransactionDefinition.PROPAGATION_NOT_SUPPORTED)))
                .build();

        Throwable thrown = failureOf(applicationService.execute(streamId, this::decide));

        assertThat(thrown).as("[exception reaching the caller]").isSameAs(conflict);
        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(1);
        assertThat(eventsInStream()).as("[events in the stream]").isEmpty();
    }

    @Test
    void under_propagation_not_supported_the_store_rolls_back_the_append_of_a_failing_handler_although_the_executor_reports_no_transaction_so_the_command_is_not_run_again_on_a_dcb_boundary() {
        DcbAppendConditionNotFulfilledException conflict = new DcbAppendConditionNotFulfilledException(DcbAppendCondition.failIfEventsMatch(CRITERIA), 1);
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new RecordsSideEffectAndFailsOnce(conflict))
                .transactionExecutor(new SpringReactiveTransactionExecutor(operatorWith(TransactionDefinition.PROPAGATION_NOT_SUPPORTED)))
                .build();

        Throwable thrown = failureOf(applicationService.execute(CRITERIA, this::decide));

        assertThat(thrown).as("[exception reaching the caller]").isSameAs(conflict);
        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(1);
        assertThat(eventsInBoundary()).as("[events in the boundary]").isEmpty();
    }

    private TransactionalOperator operatorWith(int propagationBehavior) {
        DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
        definition.setPropagationBehavior(propagationBehavior);
        return TransactionalOperator.create(transactionManager, definition);
    }

    private List<DomainEvent> decide(List<DomainEvent> events) {
        decisions.incrementAndGet();
        DomainEvent decided = new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Johan");
        decidedEventIds.add(decided.eventId());
        return List.of(decided);
    }

    private List<String> eventsInStream() {
        return requireNonNull(eventStore.read(streamId).flatMapMany(EventStream::events).map(CloudEvent::getId).collectList().block(TIMEOUT));
    }

    private List<String> eventsInBoundary() {
        return requireNonNull(eventStore.read(CRITERIA).block(TIMEOUT)).events().stream().map(CloudEvent::getId).toList();
    }

    private long sideEffects() {
        return requireNonNull(mongoTemplate.count(new Query(), SIDE_EFFECT_COLLECTION).block(TIMEOUT));
    }

    private static @Nullable Throwable failureOf(Mono<?> execution) {
        return execution.then(Mono.<Throwable>empty()).onErrorResume(Mono::just).block(TIMEOUT);
    }

    // Records a side effect through the same template as the store, so it joins the transaction the store's write is in, then fails the first dispatch
    private final class RecordsSideEffectAndFailsOnce implements ReactiveSynchronousEventDispatcher {
        private final AtomicBoolean failed = new AtomicBoolean();
        private final RuntimeException failure;

        RecordsSideEffectAndFailsOnce(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public Mono<Void> dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
            return mongoTemplate.insert(new Document("eventId", writtenCloudEvents.getFirst().getId()), SIDE_EFFECT_COLLECTION)
                    .then(Mono.defer(() -> failed.compareAndSet(false, true) ? Mono.error(failure) : Mono.<Void>empty()));
        }

        @Override
        public boolean hasSubscriptions() {
            return true;
        }
    }
}
