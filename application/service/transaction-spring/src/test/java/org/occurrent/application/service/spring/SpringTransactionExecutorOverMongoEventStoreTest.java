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

package org.occurrent.application.service.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.service.ExecutorScope;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.generic.GenericCloudEventConverter;
import org.occurrent.application.service.blocking.SynchronousEventDispatcher;
import org.occurrent.application.service.blocking.dcb.DcbExecuteOptions;
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService;
import org.occurrent.application.service.blocking.generic.GenericApplicationService;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.DomainEventConverter;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.synchronous.blocking.SynchronousSubscriptionModel;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.UnexpectedRollbackException;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;

/**
 * The executor against a real {@link SpringMongoEventStore}. A transaction it owns is undone when a synchronous handler
 * fails, so the command runs again and leaves one event and one side effect. Under {@code PROPAGATION_NOT_SUPPORTED} it
 * opens no transaction and reports none, yet the store's session synchronization is active and still rolls a write to a
 * stream, or a DCB boundary's write of a command that asks for no marker versions, back when the handler fails, and the command doesn't run again. A transaction the caller opened changes nothing for an executor that opens its own, on
 * {@code PROPAGATION_REQUIRES_NEW} or on another transaction manager. In a transaction the caller opened on the same
 * manager, a failing handler stops the handlers behind it and keeps the caller from committing, unless the manager is set
 * not to mark a transaction rollback-only after a failure in a part of it.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringTransactionExecutorOverMongoEventStoreTest {

    private static final String SIDE_EFFECT_COLLECTION = "handler_side_effects";
    private static final Tag TAG = Tag.parse("name:1");
    private static final DcbCriteria CRITERIA = tags(TAG);

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flush = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private MongoTemplate mongoTemplate;
    private MongoTransactionManager transactionManager;
    private SpringMongoEventStore eventStore;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private String streamId;
    private final AtomicInteger decisions = new AtomicInteger();
    private final List<String> decidedEventIds = new ArrayList<>();

    @BeforeEach
    void initialize() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".springexecutorovermongo");
        mongoClient = MongoClients.create(connectionString);
        SimpleMongoClientDatabaseFactory databaseFactory = new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase()));
        transactionManager = new MongoTransactionManager(databaseFactory);
        mongoTemplate = new MongoTemplate(databaseFactory);
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        eventStore = new SpringMongoEventStore(mongoTemplate, config);
        // Created up front, since a transaction can't create the collection a handler inserts into
        if (!mongoTemplate.collectionExists(SIDE_EFFECT_COLLECTION)) {
            mongoTemplate.createCollection(SIDE_EFFECT_COLLECTION);
        }
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
                .transactionExecutor(new SpringTransactionExecutor(transactionManager))
                .build();

        applicationService.execute(streamId, this::decide);

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
                .transactionExecutor(new SpringTransactionExecutor(transactionManager))
                .build();

        applicationService.execute(CRITERIA, this::decide);

        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(2);
        assertThat(sideEffects()).as("[side effects]").isEqualTo(1);
        assertThat(eventsInBoundary()).as("[events in the boundary]").containsExactly(decidedEventIds.getLast());
    }

    @Test
    void under_propagation_not_supported_the_store_rolls_back_the_write_of_a_failing_handler_although_the_executor_reports_no_transaction_so_the_command_is_not_run_again_on_a_stream() {
        WriteConditionNotFulfilledException conflict = new WriteConditionNotFulfilledException(streamId, 1, WriteCondition.anyStreamVersion());
        var applicationService = GenericApplicationService.builder(eventStore, cloudEventConverter)
                .synchronousSubscriptions(new RecordsSideEffectAndFailsOnce(conflict))
                .transactionExecutor(new SpringTransactionExecutor(templateWith(TransactionDefinition.PROPAGATION_NOT_SUPPORTED)))
                .build();

        Throwable thrown = catchThrowable(() -> applicationService.execute(streamId, this::decide));

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
                .transactionExecutor(new SpringTransactionExecutor(templateWith(TransactionDefinition.PROPAGATION_NOT_SUPPORTED)))
                .build();

        Throwable thrown = catchThrowable(() -> applicationService.execute(CRITERIA, this::decide));

        assertThat(thrown).as("[exception reaching the caller]").isSameAs(conflict);
        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(1);
        assertThat(eventsInBoundary()).as("[events in the boundary]").isEmpty();
    }

    @Test
    void a_retried_conflict_thrown_by_a_handler_rolls_back_the_write_and_the_command_runs_again_under_propagation_requires_new_in_the_callers_transaction_on_a_stream() {
        WriteConditionNotFulfilledException conflict = new WriteConditionNotFulfilledException(streamId, 1, WriteCondition.anyStreamVersion());
        var applicationService = GenericApplicationService.builder(eventStore, cloudEventConverter)
                .synchronousSubscriptions(new RecordsSideEffectAndFailsOnce(conflict))
                .transactionExecutor(new SpringTransactionExecutor(templateWith(TransactionDefinition.PROPAGATION_REQUIRES_NEW)))
                .build();

        new TransactionTemplate(transactionManager).executeWithoutResult(status -> applicationService.execute(streamId, this::decide));

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
                .transactionExecutor(new SpringTransactionExecutor(templateWith(TransactionDefinition.PROPAGATION_REQUIRES_NEW)))
                .build();

        new TransactionTemplate(transactionManager).executeWithoutResult(status -> applicationService.execute(CRITERIA, this::decide));

        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(2);
        assertThat(sideEffects()).as("[side effects]").isEqualTo(1);
        assertThat(eventsInBoundary()).as("[events in the boundary]").containsExactly(decidedEventIds.getLast());
    }

    @Test
    void a_retried_conflict_thrown_by_a_handler_rolls_back_the_write_and_the_command_runs_again_in_a_callers_transaction_on_another_transaction_manager_on_a_stream() {
        WriteConditionNotFulfilledException conflict = new WriteConditionNotFulfilledException(streamId, 1, WriteCondition.anyStreamVersion());
        var applicationService = GenericApplicationService.builder(eventStore, cloudEventConverter)
                .synchronousSubscriptions(new RecordsSideEffectAndFailsOnce(conflict))
                .transactionExecutor(new SpringTransactionExecutor(transactionManager))
                .build();

        anotherTransactionTemplate().executeWithoutResult(status -> applicationService.execute(streamId, this::decide));

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
                .transactionExecutor(new SpringTransactionExecutor(transactionManager))
                .build();

        anotherTransactionTemplate().executeWithoutResult(status -> applicationService.execute(CRITERIA, this::decide));

        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(2);
        assertThat(sideEffects()).as("[side effects]").isEqualTo(1);
        assertThat(eventsInBoundary()).as("[events in the boundary]").containsExactly(decidedEventIds.getLast());
    }

    @Test
    void a_decider_with_versions_in_a_callers_scope_without_a_transaction_runs_again_after_an_append_that_conflicts_with_one_committed_since_its_read() {
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new Reacts(() -> {
                }))
                .transactionExecutor(new SpringTransactionExecutor(templateWith(TransactionDefinition.PROPAGATION_SUPPORTS)))
                .build();
        String concurrentEventId = UUID.randomUUID().toString();
        AtomicBoolean appendedConcurrently = new AtomicBoolean();

        templateWith(TransactionDefinition.PROPAGATION_SUPPORTS).executeWithoutResult(status ->
                applicationService.execute(CRITERIA, DcbExecuteOptions.<DomainEvent>options().withVersions(), events -> {
                    if (appendedConcurrently.compareAndSet(false, true)) {
                        appendOnAnotherThread(concurrentEventId);
                    }
                    return decide(events);
                }));

        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(2);
        assertThat(eventsInBoundary()).as("[events in the boundary]").containsExactly(concurrentEventId, decidedEventIds.getLast());
    }

    @Test
    void a_command_without_versions_in_a_callers_scope_without_a_transaction_fails_at_once_on_an_append_that_conflicts_with_one_committed_since_its_read() {
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new Reacts(() -> {
                }))
                .transactionExecutor(new SpringTransactionExecutor(templateWith(TransactionDefinition.PROPAGATION_SUPPORTS)))
                .build();
        String concurrentEventId = UUID.randomUUID().toString();
        AtomicBoolean appendedConcurrently = new AtomicBoolean();
        long startedAt = System.nanoTime();

        Throwable thrown = catchThrowable(() -> templateWith(TransactionDefinition.PROPAGATION_SUPPORTS).executeWithoutResult(status ->
                applicationService.execute(CRITERIA, events -> {
                    if (appendedConcurrently.compareAndSet(false, true)) {
                        appendOnAnotherThread(concurrentEventId);
                    }
                    return decide(events);
                })));

        assertThat(thrown).as("the append in the session bound to the caller's scope conflicts with the concurrent one").isNotNull();
        assertThat(Stream.iterate(thrown, Objects::nonNull, Throwable::getCause)).as("a transient conflict in the cause chain")
                .anySatisfy(cause -> assertThat(cause).isInstanceOfSatisfying(MongoException.class, e -> assertThat(e.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)).isTrue()));
        assertThat(Duration.ofNanos(System.nanoTime() - startedAt)).as("a retry in the bound session runs until the store's deadline").isLessThan(Duration.ofSeconds(30));
        assertThat(eventsInBoundary()).as("[events in the boundary]").containsExactly(concurrentEventId);
    }

    @Test
    void a_handler_on_a_service_without_a_transaction_loses_its_reaction_when_the_store_rolls_back_the_write_it_asked_for_under_propagation_not_supported() {
        WriteConditionNotFulfilledException conflict = new WriteConditionNotFulfilledException(streamId, 1, WriteCondition.anyStreamVersion());
        String reactionStreamId = UUID.randomUUID().toString();
        var reacting = GenericApplicationService.builder(eventStore, cloudEventConverter)
                .synchronousSubscriptions(new RecordsSideEffectAndFailsOnce(conflict))
                .transactionExecutor(new SpringTransactionExecutor(templateWith(TransactionDefinition.PROPAGATION_NOT_SUPPORTED)))
                .build();
        var applicationService = GenericApplicationService.builder(eventStore, cloudEventConverter)
                .synchronousSubscriptions(new Reacts(() -> reacting.execute(reactionStreamId, this::decide)))
                .build();

        Throwable thrown = catchThrowable(() -> applicationService.execute(streamId, this::decide));

        assertThat(thrown).as("[exception reaching the caller]").isSameAs(conflict);
        assertThat(eventsInStream()).as("[events in the stream]").containsExactly(decidedEventIds.getFirst());
        assertThat(eventStore.read(reactionStreamId).events().toList()).as("[events in the reaction's stream]").isEmpty();
    }

    @Test
    void a_failing_handler_stops_the_handlers_behind_it_and_the_caller_cannot_commit_the_write_after_catching_the_failure_in_a_transaction_the_caller_opened() {
        RuntimeException failure = new IllegalStateException("handler a failed");
        List<String> receivedByB = new ArrayList<>();
        var applicationService = serviceWithHandlers(failure, receivedByB);
        AtomicReference<Throwable> caught = new AtomicReference<>();

        Throwable commitFailure = catchThrowable(() -> new TransactionTemplate(transactionManager).executeWithoutResult(status ->
                caught.set(catchThrowable(() -> applicationService.execute(streamId, this::decide)))));

        assertThat(caught.get()).as("[exception caught by the caller]").isSameAs(failure);
        assertThat(receivedByB).as("[events received by b]").isEmpty();
        assertThat(commitFailure).as("[exception from the commit]").isInstanceOf(UnexpectedRollbackException.class);
        assertThat(eventsInStream()).as("[events in the stream]").isEmpty();
    }

    @Test
    void a_failing_handler_does_not_stop_the_handlers_behind_it_and_the_caller_can_commit_the_write_after_catching_the_failure_when_the_manager_does_not_mark_the_transaction_rollback_only() {
        transactionManager.setGlobalRollbackOnParticipationFailure(false);
        RuntimeException failure = new IllegalStateException("handler a failed");
        List<String> receivedByB = new ArrayList<>();
        var applicationService = serviceWithHandlers(failure, receivedByB);
        AtomicReference<Throwable> caught = new AtomicReference<>();

        Throwable commitFailure = catchThrowable(() -> new TransactionTemplate(transactionManager).executeWithoutResult(status ->
                caught.set(catchThrowable(() -> applicationService.execute(streamId, this::decide)))));

        assertThat(caught.get()).as("[exception caught by the caller]").isSameAs(failure);
        assertThat(receivedByB).as("[events received by b]").containsExactlyElementsOf(decidedEventIds);
        assertThat(commitFailure).as("[exception from the commit]").isNull();
        assertThat(eventsInStream()).as("[events in the stream]").containsExactlyElementsOf(decidedEventIds);
    }

    @Test
    void the_action_is_told_that_the_executor_opened_a_transaction_when_none_is_open() {
        var executor = new SpringTransactionExecutor(transactionManager);

        assertThat(executor.<Boolean>inTransactionTellingIfOpened(ExecutorScope::openedTransaction)).isTrue();
    }

    @Test
    void the_action_is_told_that_the_executor_opened_a_transaction_under_propagation_requires_new_in_the_callers_transaction() {
        var executor = new SpringTransactionExecutor(templateWith(TransactionDefinition.PROPAGATION_REQUIRES_NEW));

        boolean opened = requireNonNull(new TransactionTemplate(transactionManager).execute(status -> executor.<Boolean>inTransactionTellingIfOpened(ExecutorScope::openedTransaction)));

        assertThat(opened).isTrue();
    }

    @Test
    void the_action_is_told_that_the_executor_opened_no_transaction_when_it_joins_the_callers() {
        var executor = new SpringTransactionExecutor(transactionManager);

        boolean opened = requireNonNull(new TransactionTemplate(transactionManager).execute(status -> executor.<Boolean>inTransactionTellingIfOpened(ExecutorScope::openedTransaction)));

        assertThat(opened).isFalse();
    }

    @Test
    void the_action_is_told_that_the_executor_opened_no_transaction_under_propagation_not_supported() {
        var executor = new SpringTransactionExecutor(templateWith(TransactionDefinition.PROPAGATION_NOT_SUPPORTED));

        assertThat(executor.<Boolean>inTransactionTellingIfOpened(ExecutorScope::openedTransaction)).isFalse();
    }

    // Subscription a throws and b records the ids it receives
    private GenericApplicationService<DomainEvent> serviceWithHandlers(RuntimeException failureOfA, List<String> receivedByB) {
        SynchronousSubscriptionModel subscriptions = new SynchronousSubscriptionModel();
        subscriptions.subscribe("a", null, StartAt.now(), event -> {
            throw failureOfA;
        });
        subscriptions.subscribe("b", null, StartAt.now(), event -> receivedByB.add(event.getId()));
        return GenericApplicationService.builder(eventStore, cloudEventConverter)
                .synchronousSubscriptions(subscriptions)
                .transactionExecutor(new SpringTransactionExecutor(transactionManager))
                .build();
    }

    // Commits an event to the boundary on another thread, outside the caller's synchronization
    private void appendOnAnotherThread(String eventId) {
        var plainService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter).tagGenerator(event -> Set.of(TAG)).build();
        CompletableFuture.runAsync(() -> plainService.execute(CRITERIA, events -> List.of(new NameWasChanged(eventId, LocalDateTime.now(), "name", "Jane")))).join();
    }

    private TransactionTemplate anotherTransactionTemplate() {
        return new TransactionTemplate(new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, "another")));
    }

    private TransactionTemplate templateWith(int propagationBehavior) {
        TransactionTemplate template = new TransactionTemplate(transactionManager);
        template.setPropagationBehavior(propagationBehavior);
        return template;
    }

    private List<DomainEvent> decide(List<DomainEvent> events) {
        decisions.incrementAndGet();
        DomainEvent decided = new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Johan");
        decidedEventIds.add(decided.eventId());
        return List.of(decided);
    }

    private List<String> eventsInStream() {
        return eventStore.read(streamId).events().map(CloudEvent::getId).toList();
    }

    private List<String> eventsInBoundary() {
        return eventStore.read(CRITERIA).events().stream().map(CloudEvent::getId).toList();
    }

    private long sideEffects() {
        return mongoTemplate.getCollection(SIDE_EFFECT_COLLECTION).countDocuments();
    }

    // A handler that reacts to the event by running a command of its own
    private record Reacts(Runnable command) implements SynchronousEventDispatcher {

        @Override
        public void dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
            command.run();
        }

        @Override
        public boolean hasSubscriptions() {
            return true;
        }
    }

    // Records a side effect through the same template as the store, so it joins the transaction the store's write is in, then fails the first dispatch
    private final class RecordsSideEffectAndFailsOnce implements SynchronousEventDispatcher {
        private final AtomicBoolean failed = new AtomicBoolean();
        private final RuntimeException failure;

        RecordsSideEffectAndFailsOnce(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public void dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
            mongoTemplate.insert(new Document("eventId", writtenCloudEvents.getFirst().getId()), SIDE_EFFECT_COLLECTION);
            if (failed.compareAndSet(false, true)) {
                throw failure;
            }
        }

        @Override
        public boolean hasSubscriptions() {
            return true;
        }
    }
}
