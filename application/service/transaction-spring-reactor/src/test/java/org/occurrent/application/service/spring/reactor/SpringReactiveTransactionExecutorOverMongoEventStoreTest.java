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
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.synchronous.reactor.SynchronousSubscriptionModel;
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
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;

/**
 * The executor against a real {@link ReactorMongoEventStore}. A transaction it owns is undone when a synchronous handler
 * fails, so the command runs again and leaves one event and one side effect. Under {@code PROPAGATION_NOT_SUPPORTED} it
 * opens no transaction and reports none, yet the store's session synchronization is active and still rolls the write back
 * when the handler fails. A transaction the caller opened changes nothing for an executor that opens its own, on
 * {@code PROPAGATION_REQUIRES_NEW} or on another transaction manager.
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

    @Test
    void a_retried_conflict_thrown_by_a_handler_rolls_back_the_write_and_the_command_runs_again_under_propagation_requires_new_in_the_callers_transaction_on_a_stream() {
        WriteConditionNotFulfilledException conflict = new WriteConditionNotFulfilledException(streamId, 1, WriteCondition.anyStreamVersion());
        var applicationService = GenericApplicationService.builder(eventStore, cloudEventConverter)
                .synchronousSubscriptions(new RecordsSideEffectAndFailsOnce(conflict))
                .transactionExecutor(new SpringReactiveTransactionExecutor(operatorWith(TransactionDefinition.PROPAGATION_REQUIRES_NEW)))
                .build();

        TransactionalOperator.create(transactionManager).transactional(applicationService.execute(streamId, this::decide)).block(TIMEOUT);

        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(2);
        assertThat(sideEffects()).as("[side effects]").isEqualTo(1);
        assertThat(eventsInStream()).as("[events in the stream]").containsExactly(decidedEventIds.getLast());
    }

    @Test
    void a_retried_conflict_thrown_by_a_handler_rolls_back_the_append_and_the_command_runs_again_under_propagation_requires_new_in_the_callers_transaction_on_a_dcb_boundary() {
        DcbAppendConditionNotFulfilledException conflict = new DcbAppendConditionNotFulfilledException(DcbAppendCondition.failIfEventsMatch(CRITERIA), 1);
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new RecordsSideEffectAndFailsOnce(conflict))
                .transactionExecutor(new SpringReactiveTransactionExecutor(operatorWith(TransactionDefinition.PROPAGATION_REQUIRES_NEW)))
                .build();

        TransactionalOperator.create(transactionManager).transactional(applicationService.execute(CRITERIA, this::decide)).block(TIMEOUT);

        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(2);
        assertThat(sideEffects()).as("[side effects]").isEqualTo(1);
        assertThat(eventsInBoundary()).as("[events in the boundary]").containsExactly(decidedEventIds.getLast());
    }

    @Test
    void a_retried_conflict_thrown_by_a_handler_rolls_back_the_write_and_the_command_runs_again_in_a_callers_transaction_on_another_transaction_manager_on_a_stream() {
        WriteConditionNotFulfilledException conflict = new WriteConditionNotFulfilledException(streamId, 1, WriteCondition.anyStreamVersion());
        var applicationService = GenericApplicationService.builder(eventStore, cloudEventConverter)
                .synchronousSubscriptions(new RecordsSideEffectAndFailsOnce(conflict))
                .transactionExecutor(new SpringReactiveTransactionExecutor(transactionManager))
                .build();

        anotherTransactionalOperator().transactional(applicationService.execute(streamId, this::decide)).block(TIMEOUT);

        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(2);
        assertThat(sideEffects()).as("[side effects]").isEqualTo(1);
        assertThat(eventsInStream()).as("[events in the stream]").containsExactly(decidedEventIds.getLast());
    }

    @Test
    void a_retried_conflict_thrown_by_a_handler_rolls_back_the_append_and_the_command_runs_again_in_a_callers_transaction_on_another_transaction_manager_on_a_dcb_boundary() {
        DcbAppendConditionNotFulfilledException conflict = new DcbAppendConditionNotFulfilledException(DcbAppendCondition.failIfEventsMatch(CRITERIA), 1);
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new RecordsSideEffectAndFailsOnce(conflict))
                .transactionExecutor(new SpringReactiveTransactionExecutor(transactionManager))
                .build();

        anotherTransactionalOperator().transactional(applicationService.execute(CRITERIA, this::decide)).block(TIMEOUT);

        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(2);
        assertThat(sideEffects()).as("[side effects]").isEqualTo(1);
        assertThat(eventsInBoundary()).as("[events in the boundary]").containsExactly(decidedEventIds.getLast());
    }

    @Test
    void a_handler_on_a_service_without_a_transaction_loses_its_reaction_when_the_store_rolls_back_the_write_it_asked_for_under_propagation_not_supported() {
        WriteConditionNotFulfilledException conflict = new WriteConditionNotFulfilledException(streamId, 1, WriteCondition.anyStreamVersion());
        String reactionStreamId = UUID.randomUUID().toString();
        var reacting = GenericApplicationService.builder(eventStore, cloudEventConverter)
                .synchronousSubscriptions(new RecordsSideEffectAndFailsOnce(conflict))
                .transactionExecutor(new SpringReactiveTransactionExecutor(operatorWith(TransactionDefinition.PROPAGATION_NOT_SUPPORTED)))
                .build();
        var applicationService = GenericApplicationService.builder(eventStore, cloudEventConverter)
                .synchronousSubscriptions(new Reacts(() -> reacting.execute(reactionStreamId, this::decide).then()))
                .build();

        Throwable thrown = failureOf(applicationService.execute(streamId, this::decide));

        assertThat(thrown).as("[exception reaching the caller]").isSameAs(conflict);
        assertThat(eventsInStream()).as("[events in the stream]").containsExactly(decidedEventIds.getFirst());
        assertThat(eventStore.read(reactionStreamId).flatMapMany(EventStream::events).collectList().block(TIMEOUT)).as("[events in the reaction's stream]").isEmpty();
    }

    @Test
    void the_action_is_told_that_the_executor_opened_a_transaction_when_none_is_open() {
        var executor = new SpringReactiveTransactionExecutor(transactionManager);

        assertThat(executor.<Boolean>inTransactionTellingIfOpened(Mono::just).block(TIMEOUT)).isTrue();
    }

    @Test
    void the_action_is_told_that_the_executor_opened_a_transaction_under_propagation_requires_new_in_the_callers_transaction() {
        var executor = new SpringReactiveTransactionExecutor(operatorWith(TransactionDefinition.PROPAGATION_REQUIRES_NEW));

        Boolean opened = TransactionalOperator.create(transactionManager).transactional(executor.<Boolean>inTransactionTellingIfOpened(Mono::just)).block(TIMEOUT);

        assertThat(opened).isTrue();
    }

    @Test
    void the_action_is_told_that_the_executor_opened_no_transaction_when_it_joins_the_callers() {
        var executor = new SpringReactiveTransactionExecutor(transactionManager);

        Boolean opened = TransactionalOperator.create(transactionManager).transactional(executor.<Boolean>inTransactionTellingIfOpened(Mono::just)).block(TIMEOUT);

        assertThat(opened).isFalse();
    }

    @Test
    void the_action_is_told_that_the_executor_opened_no_transaction_under_propagation_not_supported() {
        var executor = new SpringReactiveTransactionExecutor(operatorWith(TransactionDefinition.PROPAGATION_NOT_SUPPORTED));

        assertThat(executor.<Boolean>inTransactionTellingIfOpened(Mono::just).block(TIMEOUT)).isFalse();
    }

    @Test
    void a_failing_handler_does_not_stop_the_handlers_behind_it_and_the_caller_commits_the_write_after_catching_the_failure_in_a_transaction_the_caller_opened_on_a_stream() {
        RuntimeException failure = new IllegalStateException("handler a failed");
        List<String> receivedByB = new CopyOnWriteArrayList<>();
        var applicationService = GenericApplicationService.builder(eventStore, cloudEventConverter)
                .synchronousSubscriptions(subscriptionsWhereAFailsAndBRecords(failure, receivedByB))
                .transactionExecutor(new SpringReactiveTransactionExecutor(transactionManager))
                // Retries every exception
                .retry(Retry.max(3))
                .build();
        AtomicReference<Throwable> caught = new AtomicReference<>();

        Throwable commitFailure = failureOf(TransactionalOperator.create(transactionManager).transactional(
                applicationService.execute(streamId, this::decide).onErrorResume(error -> {
                    caught.set(error);
                    return Mono.empty();
                })));

        assertThat(caught.get()).as("[exception caught by the caller]").isSameAs(failure);
        assertThat(receivedByB).as("[events received by b]").containsExactlyElementsOf(decidedEventIds);
        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(1);
        assertThat(commitFailure).as("[exception from the commit]").isNull();
        assertThat(eventsInStream()).as("[events in the stream]").containsExactlyElementsOf(decidedEventIds);
    }

    @Test
    void a_failing_handler_does_not_stop_the_handlers_behind_it_and_the_caller_commits_the_append_after_catching_the_failure_in_a_transaction_the_caller_opened_on_a_dcb_boundary() {
        RuntimeException failure = new IllegalStateException("handler a failed");
        List<String> receivedByB = new CopyOnWriteArrayList<>();
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(subscriptionsWhereAFailsAndBRecords(failure, receivedByB))
                .transactionExecutor(new SpringReactiveTransactionExecutor(transactionManager))
                // Retries every exception
                .retry(Retry.max(3))
                .build();
        AtomicReference<Throwable> caught = new AtomicReference<>();

        Throwable commitFailure = failureOf(TransactionalOperator.create(transactionManager).transactional(
                applicationService.execute(CRITERIA, this::decide).onErrorResume(error -> {
                    caught.set(error);
                    return Mono.empty();
                })));

        assertThat(caught.get()).as("[exception caught by the caller]").isSameAs(failure);
        assertThat(receivedByB).as("[events received by b]").containsExactlyElementsOf(decidedEventIds);
        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(1);
        assertThat(commitFailure).as("[exception from the commit]").isNull();
        assertThat(eventsInBoundary()).as("[events in the boundary]").containsExactlyElementsOf(decidedEventIds);
    }

    private TransactionalOperator anotherTransactionalOperator() {
        return TransactionalOperator.create(new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, "another")));
    }

    // Subscription a errors and b records the ids it receives
    private static SynchronousSubscriptionModel subscriptionsWhereAFailsAndBRecords(RuntimeException failureOfA, List<String> receivedByB) {
        SynchronousSubscriptionModel subscriptions = new SynchronousSubscriptionModel();
        subscriptions.subscribe("a", null, StartAt.now(), event -> Mono.error(failureOfA));
        subscriptions.subscribe("b", null, StartAt.now(), event -> Mono.fromRunnable(() -> receivedByB.add(event.getId())));
        return subscriptions;
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

    // A handler that reacts to the event by running a command of its own
    private record Reacts(Supplier<Mono<Void>> command) implements ReactiveSynchronousEventDispatcher {

        @Override
        public Mono<Void> dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
            return Mono.defer(command);
        }

        @Override
        public boolean hasSubscriptions() {
            return true;
        }
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
