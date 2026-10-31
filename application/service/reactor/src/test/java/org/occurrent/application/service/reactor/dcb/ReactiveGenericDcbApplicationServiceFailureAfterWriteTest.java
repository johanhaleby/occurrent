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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.generic.GenericCloudEventConverter;
import org.occurrent.application.service.reactor.ReactiveSynchronousEventDispatcher;
import org.occurrent.application.service.reactor.ReactiveTransactionExecutor;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.DomainEventConverter;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
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
import reactor.util.retry.Retry;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;

/**
 * Without a transaction the append has committed by the time the re-read and the synchronous dispatch run, so a failure
 * there must reach the caller as it is. Running the command again would decide and append it a second time.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactiveGenericDcbApplicationServiceFailureAfterWriteTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);
    private static final Tag TAG = Tag.parse("name:1");
    private static final DcbCriteria CRITERIA = tags(TAG);
    // The events of the own-retry test carry this tag, so the append condition built from a read of CRITERIA can't fail on them
    private static final Tag UNREAD_TAG = Tag.parse("name:unread");
    private static final Retry RETRY_EVERYTHING = Retry.fixedDelay(4, Duration.ofMillis(1)).filter(__ -> true).onRetryExhaustedThrow((spec, signal) -> signal.failure());

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flush = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private ReactorMongoEventStore mongoEventStore;
    private InterferingEventStore eventStore;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private final AtomicInteger decisions = new AtomicInteger();
    private final List<String> decidedEventIds = new CopyOnWriteArrayList<>();

    @BeforeEach
    void initialize() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcbfailureafterwrite");
        MongoClient mongoClient = MongoClients.create(connectionString);
        ReactiveMongoTemplate mongoTemplate = new ReactiveMongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        ReactiveMongoTransactionManager transactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        mongoEventStore = new ReactorMongoEventStore(mongoTemplate, config);
        eventStore = new InterferingEventStore(mongoEventStore);
        DomainEventConverter domainEventConverter = new DomainEventConverter(new ObjectMapper());
        cloudEventConverter = new GenericCloudEventConverter<>(domainEventConverter::convertToDomainEvent, domainEventConverter::convertToCloudEvent);
    }

    @Test
    void a_retried_conflict_thrown_by_a_handler_after_the_append_reaches_the_caller_without_running_the_command_again() {
        DcbAppendConditionNotFulfilledException conflict = new DcbAppendConditionNotFulfilledException(DcbAppendCondition.failIfEventsMatch(CRITERIA), 1);
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new FailingOnceDispatcher(conflict))
                .transactionExecutor(ReactiveTransactionExecutor.noTransaction())
                .build();

        Throwable thrown = failureOf(applicationService.execute(CRITERIA, this::decide));

        assertThat(decisions.get()).as("domain function runs").isEqualTo(1);
        assertThat(thrown).as("exception reaching the caller").isSameAs(conflict);
        assertThat(eventsInBoundary()).as("events in the boundary").isEqualTo(decidedEventIds);
    }

    @Test
    void a_plain_exception_thrown_by_a_handler_after_the_append_reaches_the_caller_when_the_retry_matches_every_exception() {
        RuntimeException failure = new RuntimeException("handler failed");
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new FailingOnceDispatcher(failure))
                .transactionExecutor(ReactiveTransactionExecutor.noTransaction())
                .retry(RETRY_EVERYTHING)
                .build();

        Throwable thrown = failureOf(applicationService.execute(CRITERIA, this::decide));

        assertThat(decisions.get()).as("domain function runs").isEqualTo(1);
        assertThat(thrown).as("exception reaching the caller").isSameAs(failure);
        assertThat(eventsInBoundary()).as("events in the boundary").isEqualTo(decidedEventIds);
    }

    @Test
    void a_failing_re_read_after_the_append_reaches_the_caller_when_the_retry_matches_every_exception() {
        RuntimeException failure = new RuntimeException("store unavailable");
        eventStore.failReReadAfterNextAppend(failure);
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new RecordingDispatcher())
                .transactionExecutor(ReactiveTransactionExecutor.noTransaction())
                .retry(RETRY_EVERYTHING)
                .build();

        Throwable thrown = failureOf(applicationService.execute(CRITERIA, this::decide));

        assertThat(decisions.get()).as("domain function runs").isEqualTo(1);
        assertThat(thrown).as("exception reaching the caller").isSameAs(failure);
        assertThat(eventsInBoundary()).as("events in the boundary").isEqualTo(decidedEventIds);
    }

    @Test
    void the_side_effect_does_not_run_when_a_handler_throws_a_retried_conflict_after_the_append() {
        DcbAppendConditionNotFulfilledException conflict = new DcbAppendConditionNotFulfilledException(DcbAppendCondition.failIfEventsMatch(CRITERIA), 1);
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new FailingOnceDispatcher(conflict))
                .transactionExecutor(ReactiveTransactionExecutor.noTransaction())
                .build();
        AtomicInteger sideEffectRuns = new AtomicInteger();

        failureOf(applicationService.execute(CRITERIA, DcbExecuteOptions.<DomainEvent>options().sideEffect(events -> Mono.fromRunnable(sideEffectRuns::incrementAndGet)), this::decide));

        assertThat(sideEffectRuns.get()).as("side effect runs").isZero();
    }

    @Test
    void a_checked_exception_thrown_sneakily_by_a_handler_reaches_the_caller_when_the_retry_matches_every_throwable() {
        IOException failure = new IOException("handler failed");
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new SneakilyThrowingDispatcher(failure))
                .transactionExecutor(ReactiveTransactionExecutor.noTransaction())
                .retry(Retry.fixedDelay(4, Duration.ofMillis(1)).filter(Throwable.class::isInstance).onRetryExhaustedThrow((spec, signal) -> signal.failure()))
                .build();

        Throwable thrown = failureOf(applicationService.execute(CRITERIA, this::decide));

        assertThat(decisions.get()).as("domain function runs").isEqualTo(1);
        assertThat(thrown).as("exception reaching the caller").isSameAs(failure);
        assertThat(eventsInBoundary()).as("events in the boundary").isEqualTo(decidedEventIds);
    }

    @Test
    void a_handler_failure_rolls_back_an_executor_that_opens_a_transaction_but_does_not_report_one() {
        RuntimeException failure = new RuntimeException("handler failed");
        OpensTransaction executor = new OpensTransaction();
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new FailingOnceDispatcher(failure))
                .transactionExecutor(executor)
                .build();

        Throwable thrown = failureOf(applicationService.execute(CRITERIA, this::decide));

        assertThat(thrown).as("exception reaching the caller").isSameAs(failure);
        assertThat(executor.commits.get()).as("commits").isZero();
        assertThat(executor.rollbacks.get()).as("rollbacks").isEqualTo(1);
    }

    @Test
    void a_retried_conflict_thrown_by_a_handler_still_runs_the_command_again_when_the_executor_reports_a_transaction() {
        DcbAppendConditionNotFulfilledException conflict = new DcbAppendConditionNotFulfilledException(DcbAppendCondition.failIfEventsMatch(CRITERIA), 1);
        OwnsTransaction executor = new OwnsTransaction();
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new FailingOnceDispatcher(conflict))
                .transactionExecutor(executor)
                .build();

        failureOf(applicationService.execute(CRITERIA, this::decide));

        assertThat(decisions.get()).as("domain function runs").isEqualTo(2);
        assertThat(executor.rollbacks.get()).as("rollbacks").isEqualTo(1);
        assertThat(executor.commits.get()).as("commits").isEqualTo(1);
    }

    @Test
    void a_conflict_on_the_append_itself_still_runs_the_command_again_from_a_fresh_read_without_a_transaction() {
        RecordingDispatcher dispatcher = new RecordingDispatcher();
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(dispatcher)
                .transactionExecutor(ReactiveTransactionExecutor.noTransaction())
                .build();
        DomainEvent concurrent = nameWasChanged();
        eventStore.appendBeforeNextConditionalAppend(cloudEventConverter.toCloudEvents(List.of(concurrent)).stream().map(event -> DcbCloudEvents.withTags(event, Set.of(TAG))).toList());

        failureOf(applicationService.execute(CRITERIA, this::decide));

        assertThat(decisions.get()).as("domain function runs").isEqualTo(2);
        assertThat(eventsInBoundary()).as("events in the boundary").isEqualTo(List.of(concurrent.eventId(), decidedEventIds.getLast()));
        assertThat(dispatcher.dispatchedEventIds).as("dispatched events").containsExactly(decidedEventIds.getLast());
    }

    @Test
    void a_failure_after_the_append_reaches_the_caller_as_it_is_when_the_executor_translates_exceptions_and_the_retry_matches_every_exception() {
        RuntimeException failure = new RuntimeException("handler failed");
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new FailingOnceDispatcher(failure))
                .transactionExecutor(new TranslatesExceptions())
                .retry(RETRY_EVERYTHING)
                .build();

        Throwable thrown = failureOf(applicationService.execute(CRITERIA, this::decide));

        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(1);
        assertThat(thrown).as("[exception reaching the caller]").isSameAs(failure);
        assertThat(eventsInBoundary()).as("[events in the boundary]").isEqualTo(decidedEventIds);
    }

    @Test
    void a_failure_after_the_append_reaches_the_caller_without_a_second_append_when_the_executor_retries_on_its_own_and_reports_no_transaction() {
        RuntimeException failure = new RuntimeException("handler failed");
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(UNREAD_TAG))
                .synchronousSubscriptions(new FailingOnceDispatcher(failure))
                .transactionExecutor(new RetriesOnItsOwn())
                .retry(RETRY_EVERYTHING)
                .build();

        Throwable thrown = failureOf(applicationService.execute(CRITERIA, this::decide));

        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(1);
        assertThat(thrown).as("[exception reaching the caller]").isSameAs(failure);
        assertThat(eventStore.appendCalls.get()).as("[appends attempted]").isEqualTo(1);
        assertThat(eventsMatching(tags(UNREAD_TAG))).as("[events stored under the tag the command did not read]").isEqualTo(decidedEventIds);
    }

    @Test
    void a_retried_conflict_thrown_by_a_handler_after_the_append_reaches_the_caller_without_running_the_command_again_when_the_transaction_is_joined() {
        DcbAppendConditionNotFulfilledException conflict = new DcbAppendConditionNotFulfilledException(DcbAppendCondition.failIfEventsMatch(CRITERIA), 1);
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new FailingOnceDispatcher(conflict))
                .transactionExecutor(new JoinsTransaction())
                .build();
        AtomicInteger sideEffectRuns = new AtomicInteger();

        Throwable thrown = failureOf(applicationService.execute(CRITERIA, DcbExecuteOptions.<DomainEvent>options().sideEffect(events -> Mono.fromRunnable(sideEffectRuns::incrementAndGet)), this::decide));

        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(1);
        assertThat(thrown).as("[exception reaching the caller]").isSameAs(conflict);
        assertThat(sideEffectRuns.get()).as("[side effect runs]").isZero();
    }

    @Test
    void a_conflict_on_the_append_itself_still_runs_the_command_again_from_a_fresh_read_when_the_transaction_is_joined() {
        RecordingDispatcher dispatcher = new RecordingDispatcher();
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(dispatcher)
                .transactionExecutor(new JoinsTransaction())
                .build();
        DomainEvent concurrent = nameWasChanged();
        eventStore.appendBeforeNextConditionalAppend(cloudEventConverter.toCloudEvents(List.of(concurrent)).stream().map(event -> DcbCloudEvents.withTags(event, Set.of(TAG))).toList());

        Throwable thrown = failureOf(applicationService.execute(CRITERIA, this::decide));

        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(2);
        assertThat(thrown).as("[exception reaching the caller]").isNull();
        assertThat(eventsInBoundary()).as("[events in the boundary]").isEqualTo(List.of(concurrent.eventId(), decidedEventIds.getLast()));
        assertThat(dispatcher.dispatchedEventIds).as("[dispatched events]").containsExactly(decidedEventIds.getLast());
    }

    @Test
    void a_failure_after_the_append_carries_what_the_executor_suppressed_when_it_reaches_the_caller() {
        RuntimeException failure = new RuntimeException("handler failed");
        SuppressesCleanupFailure executor = new SuppressesCleanupFailure();
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new FailingOnceDispatcher(failure))
                .transactionExecutor(executor)
                .build();

        Throwable thrown = failureOf(applicationService.execute(CRITERIA, this::decide));

        assertThat(thrown).as("[exception reaching the caller]").isSameAs(failure);
        assertThat(requireNonNull(thrown).getSuppressed()).as("[exceptions suppressed by the failure]").containsExactly(executor.cleanupFailure);
    }

    @Test
    void a_re_read_that_throws_instead_of_returning_a_mono_after_the_append_reaches_the_caller_when_the_retry_matches_every_exception() {
        RuntimeException failure = new RuntimeException("store unavailable");
        eventStore.throwFromReReadAfterNextAppend(failure);
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new RecordingDispatcher())
                .transactionExecutor(ReactiveTransactionExecutor.noTransaction())
                .retry(RETRY_EVERYTHING)
                .build();

        Throwable thrown = failureOf(applicationService.execute(CRITERIA, this::decide));

        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(1);
        assertThat(thrown).as("[exception reaching the caller]").isSameAs(failure);
        assertThat(eventsInBoundary()).as("[events in the boundary]").isEqualTo(decidedEventIds);
    }

    @Test
    void the_command_writes_and_dispatches_once_when_the_executor_answers_nothing_to_whether_it_is_transactional() {
        RecordingDispatcher dispatcher = new RecordingDispatcher();
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(dispatcher)
                .transactionExecutor(new AnswersNothing())
                .build();

        DcbAppendResult result = applicationService.execute(CRITERIA, this::decide).block(TIMEOUT);

        assertThat(result).as("[what the command returns]").isNotNull();
        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(1);
        assertThat(eventsInBoundary()).as("[events in the boundary]").isEqualTo(decidedEventIds);
        assertThat(dispatcher.dispatchedEventIds).as("[dispatched events]").isEqualTo(decidedEventIds);
    }

    @Test
    void a_failure_after_the_append_reaches_the_caller_without_a_second_append_when_the_executor_subscribes_to_the_action_again_with_retry() {
        RuntimeException failure = new RuntimeException("handler failed");
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new FailingOnceDispatcher(failure))
                .transactionExecutor(new SubscribesToTheActionAgain())
                .build();

        Throwable thrown = failureOf(applicationService.execute(CRITERIA, this::decide));

        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(1);
        assertThat(eventStore.appendCalls.get()).as("[appends attempted]").isEqualTo(1);
        assertThat(eventsInBoundary()).as("[events in the boundary]").isEqualTo(decidedEventIds);
        assertThat(thrown).as("[exception reaching the caller]").isSameAs(failure);
    }

    @Test
    void a_failure_after_the_append_reaches_the_caller_without_a_second_append_when_the_executor_subscribes_to_the_action_again_with_retry_when() {
        RuntimeException failure = new RuntimeException("handler failed");
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new FailingOnceDispatcher(failure))
                .transactionExecutor(new SubscribesToTheActionAgainWithRetryWhen())
                .build();

        Throwable thrown = failureOf(applicationService.execute(CRITERIA, this::decide));

        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(1);
        assertThat(eventStore.appendCalls.get()).as("[appends attempted]").isEqualTo(1);
        assertThat(eventsInBoundary()).as("[events in the boundary]").isEqualTo(decidedEventIds);
        assertThat(thrown).as("[exception reaching the caller]").isSameAs(failure);
    }

    @Test
    void a_failure_after_the_append_reaches_the_caller_rather_than_a_result_when_the_executor_answers_with_the_result_of_an_earlier_command() {
        RuntimeException failure = new RuntimeException("handler failed");
        DcbAppendResult earlierResult = requireNonNull(mongoEventStore.append(cloudEventConverter.toCloudEvents(List.of(nameWasChanged())).stream().map(event -> DcbCloudEvents.withTags(event, Set.of(UNREAD_TAG))).toList()).block(TIMEOUT));
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new FailingOnceDispatcher(failure))
                .transactionExecutor(new AnswersWithAnEarlierResultWhenTheActionFails(earlierResult))
                .build();

        Throwable thrown = failureOf(applicationService.execute(CRITERIA, this::decide));

        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(1);
        assertThat(eventsInBoundary()).as("[events in the boundary]").isEqualTo(decidedEventIds);
        assertThat(thrown).as("[exception reaching the caller]").isSameAs(failure);
    }

    @Test
    void a_failure_after_the_append_carries_the_exception_that_replaced_it_in_the_executor_when_it_reaches_the_caller() {
        RuntimeException failure = new RuntimeException("handler failed");
        ReplacesWhatPassesThrough executor = new ReplacesWhatPassesThrough();
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new FailingOnceDispatcher(failure))
                .transactionExecutor(executor)
                .build();

        Throwable thrown = failureOf(applicationService.execute(CRITERIA, this::decide));

        assertThat(thrown).as("[exception reaching the caller]").isSameAs(failure);
        assertThat(requireNonNull(thrown).getSuppressed()).as("[exceptions suppressed by the failure]").containsExactly(executor.replacement);
        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(1);
    }

    @Test
    void a_failure_after_the_append_reaches_the_caller_with_nothing_suppressed_when_the_executor_wraps_it() {
        RuntimeException failure = new RuntimeException("handler failed");
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new FailingOnceDispatcher(failure))
                .transactionExecutor(new TranslatesExceptions())
                .build();

        Throwable thrown = failureOf(applicationService.execute(CRITERIA, this::decide));

        assertThat(thrown).as("[exception reaching the caller]").isSameAs(failure);
        assertThat(requireNonNull(thrown).getSuppressed()).as("[exceptions suppressed by the failure]").isEmpty();
        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(1);
    }

    @Test
    void a_failure_to_read_the_transaction_before_the_append_reaches_the_caller_without_an_append_when_the_retry_does_not_match_it() {
        FailsToReportInside executor = new FailsToReportInside();
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new RecordingDispatcher())
                .transactionExecutor(executor)
                .build();

        Throwable thrown = failureOf(applicationService.execute(CRITERIA, this::decide));

        assertThat(thrown).as("[exception reaching the caller]").isSameAs(executor.failure);
        assertThat(eventsInBoundary()).as("[events in the boundary]").isEmpty();
    }

    @Test
    void a_failure_after_the_append_reaches_the_caller_with_nothing_suppressed_when_the_executor_wraps_it_twice() {
        RuntimeException failure = new RuntimeException("handler failed");
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new FailingOnceDispatcher(failure))
                .transactionExecutor(new WrapsTwice())
                .build();

        Throwable thrown = failureOf(applicationService.execute(CRITERIA, this::decide));

        assertThat(thrown).as("[exception reaching the caller]").isSameAs(failure);
        assertThat(requireNonNull(thrown).getSuppressed()).as("[exceptions suppressed by the failure]").isEmpty();
        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(1);
    }

    private List<DomainEvent> decide(List<DomainEvent> events) {
        decisions.incrementAndGet();
        DomainEvent decided = nameWasChanged();
        decidedEventIds.add(decided.eventId());
        return List.of(decided);
    }

    private static DomainEvent nameWasChanged() {
        return new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Johan");
    }

    private List<String> eventsInBoundary() {
        return eventsMatching(CRITERIA);
    }

    private List<String> eventsMatching(DcbCriteria criteria) {
        return requireNonNull(mongoEventStore.read(criteria).block(TIMEOUT)).events().stream().map(CloudEvent::getId).toList();
    }

    private static @Nullable Throwable failureOf(Mono<?> execution) {
        return execution.then(Mono.<Throwable>empty()).onErrorResume(Mono::just).block(TIMEOUT);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable throwable) throws T {
        throw (T) throwable;
    }

    private static final class FailingOnceDispatcher implements ReactiveSynchronousEventDispatcher {
        private final AtomicReference<Throwable> failure;

        FailingOnceDispatcher(Throwable failure) {
            this.failure = new AtomicReference<>(failure);
        }

        @Override
        public Mono<Void> dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
            return Mono.defer(() -> {
                Throwable toThrow = failure.getAndSet(null);
                return toThrow == null ? Mono.empty() : Mono.error(toThrow);
            });
        }

        @Override
        public boolean hasSubscriptions() {
            return true;
        }
    }

    // A handler written in Kotlin can throw a checked exception out of dispatch without declaring it
    private static final class SneakilyThrowingDispatcher implements ReactiveSynchronousEventDispatcher {
        private final AtomicReference<Throwable> failure;

        SneakilyThrowingDispatcher(Throwable failure) {
            this.failure = new AtomicReference<>(failure);
        }

        @Override
        public Mono<Void> dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
            Throwable toThrow = failure.getAndSet(null);
            if (toThrow != null) {
                ReactiveGenericDcbApplicationServiceFailureAfterWriteTest.<RuntimeException>sneakyThrow(toThrow);
            }
            return Mono.empty();
        }

        @Override
        public boolean hasSubscriptions() {
            return true;
        }
    }

    private static final class RecordingDispatcher implements ReactiveSynchronousEventDispatcher {
        private final List<String> dispatchedEventIds = new CopyOnWriteArrayList<>();

        @Override
        public Mono<Void> dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
            return Mono.fromRunnable(() -> writtenCloudEvents.forEach(event -> dispatchedEventIds.add(event.getId())));
        }

        @Override
        public boolean hasSubscriptions() {
            return true;
        }
    }

    // Stand-in for an executor that opens a transaction. It keeps the default isTransactional of false.
    private static class OpensTransaction implements ReactiveTransactionExecutor {
        final AtomicInteger commits = new AtomicInteger();
        final AtomicInteger rollbacks = new AtomicInteger();

        @Override
        public <T> Mono<T> inTransaction(Supplier<Mono<T>> action) {
            return Mono.defer(action).doOnError(__ -> rollbacks.incrementAndGet()).doOnSuccess(__ -> commits.incrementAndGet());
        }
    }

    // Stand-in for an executor that owns its transaction and reports it. It answers true only inside inTransaction.
    private static final class OwnsTransaction extends OpensTransaction {
        private static final String IN_TRANSACTION = "inTransaction";

        @Override
        public <T> Mono<T> inTransaction(Supplier<Mono<T>> action) {
            return super.inTransaction(action).contextWrite(context -> context.put(IN_TRANSACTION, true));
        }

        @Override
        public Mono<Boolean> isTransactional() {
            return Mono.deferContextual(context -> Mono.just(context.hasKey(IN_TRANSACTION)));
        }
    }

    // Stand-in for an executor that runs in a transaction the caller opened. It answers true before and inside inTransaction, and runs the action once.
    private static final class JoinsTransaction implements ReactiveTransactionExecutor {
        @Override
        public <T> Mono<T> inTransaction(Supplier<Mono<T>> action) {
            return Mono.defer(action);
        }

        @Override
        public Mono<Boolean> isTransactional() {
            return Mono.just(true);
        }
    }

    // Stand-in for an executor that translates what passes through it and opens no transaction
    private static final class TranslatesExceptions implements ReactiveTransactionExecutor {
        @Override
        public <T> Mono<T> inTransaction(Supplier<Mono<T>> action) {
            return Mono.defer(action).onErrorMap(RuntimeException.class, e -> new IllegalStateException(e));
        }
    }

    // Stand-in for an executor with a retry of its own that opens no transaction and keeps the default isTransactional of false
    private static final class RetriesOnItsOwn implements ReactiveTransactionExecutor {
        @Override
        public <T> Mono<T> inTransaction(Supplier<Mono<T>> action) {
            return Mono.defer(action).retry(2);
        }
    }

    // Stand-in for an executor whose cleanup fails after the action did, and attaches that to what passes through it
    private static final class SuppressesCleanupFailure implements ReactiveTransactionExecutor {
        final RuntimeException cleanupFailure = new IllegalStateException("cleanup failed");

        @Override
        public <T> Mono<T> inTransaction(Supplier<Mono<T>> action) {
            return Mono.defer(action).doOnError(failure -> failure.addSuppressed(cleanupFailure));
        }
    }

    // Stand-in for an executor whose isTransactional completes without an answer
    private static final class AnswersNothing implements ReactiveTransactionExecutor {
        @Override
        public <T> Mono<T> inTransaction(Supplier<Mono<T>> action) {
            return Mono.defer(action);
        }

        @Override
        public Mono<Boolean> isTransactional() {
            return Mono.empty();
        }
    }

    // Stand-in for an executor that subscribes to what it runs again when it fails and opens no transaction. It keeps the default isTransactional of false.
    private static final class SubscribesToTheActionAgain implements ReactiveTransactionExecutor {
        @Override
        public <T> Mono<T> inTransaction(Supplier<Mono<T>> action) {
            return action.get().retry(2);
        }
    }

    // The same with retryWhen
    private static final class SubscribesToTheActionAgainWithRetryWhen implements ReactiveTransactionExecutor {
        @Override
        public <T> Mono<T> inTransaction(Supplier<Mono<T>> action) {
            return action.get().retryWhen(Retry.max(2));
        }
    }

    // Stand-in for an executor that swallows what the action fails with and answers with a result it kept from an earlier command. It opens no transaction.
    private static final class AnswersWithAnEarlierResultWhenTheActionFails implements ReactiveTransactionExecutor {
        private final Object earlierResult;

        AnswersWithAnEarlierResultWhenTheActionFails(Object earlierResult) {
            this.earlierResult = earlierResult;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Mono<T> inTransaction(Supplier<Mono<T>> action) {
            return Mono.defer(action).onErrorResume(__ -> Mono.just((T) earlierResult));
        }
    }

    // Stand-in for an executor that replaces what passes through it, as a rollback that fails does, and opens no transaction
    private static final class ReplacesWhatPassesThrough implements ReactiveTransactionExecutor {
        final RuntimeException replacement = new IllegalStateException("rollback failed");

        @Override
        public <T> Mono<T> inTransaction(Supplier<Mono<T>> action) {
            return Mono.defer(action).onErrorMap(RuntimeException.class, __ -> replacement);
        }
    }

    // Stand-in for an executor that answers before entry and fails inside, as one whose transaction can't be read there does. It opens no transaction.
    private static final class FailsToReportInside implements ReactiveTransactionExecutor {
        final RuntimeException failure = new IllegalStateException("transaction state unavailable");
        private final AtomicInteger asked = new AtomicInteger();

        @Override
        public <T> Mono<T> inTransaction(Supplier<Mono<T>> action) {
            return Mono.defer(action);
        }

        @Override
        public Mono<Boolean> isTransactional() {
            return Mono.defer(() -> asked.incrementAndGet() > 1 ? Mono.<Boolean>error(failure) : Mono.just(false));
        }
    }

    // Stand-in for an executor that wraps what passes through it twice and opens no transaction
    private static final class WrapsTwice implements ReactiveTransactionExecutor {
        @Override
        public <T> Mono<T> inTransaction(Supplier<Mono<T>> action) {
            return Mono.defer(action).onErrorMap(RuntimeException.class, e -> new IllegalStateException(new RuntimeException(e)));
        }
    }

    // Fails the first read after the next append succeeded, which is the re-read of the just-appended block.
    // It can also throw from that read instead of returning a failing Mono.
    // It can also append another event just before the next conditional append, which makes that append lose its condition.
    private static final class InterferingEventStore implements DcbEventStore {
        private final DcbEventStore delegate;
        private final AtomicReference<@Nullable RuntimeException> failAfterNextAppend = new AtomicReference<>();
        private final AtomicReference<@Nullable RuntimeException> failNextRead = new AtomicReference<>();
        private final AtomicReference<@Nullable RuntimeException> throwAfterNextAppend = new AtomicReference<>();
        private final AtomicReference<@Nullable RuntimeException> throwFromNextRead = new AtomicReference<>();
        private final AtomicReference<@Nullable List<CloudEvent>> concurrentEvents = new AtomicReference<>();
        final AtomicInteger appendCalls = new AtomicInteger();

        InterferingEventStore(DcbEventStore delegate) {
            this.delegate = delegate;
        }

        void failReReadAfterNextAppend(RuntimeException failure) {
            failAfterNextAppend.set(failure);
        }

        void throwFromReReadAfterNextAppend(RuntimeException failure) {
            throwAfterNextAppend.set(failure);
        }

        void appendBeforeNextConditionalAppend(List<CloudEvent> events) {
            concurrentEvents.set(events);
        }

        @Override
        public Mono<DcbEventStream> read(DcbCriteria criteria, DcbReadOptions options) {
            RuntimeException thrown = throwFromNextRead.getAndSet(null);
            if (thrown != null) {
                throw thrown;
            }
            return Mono.defer(() -> {
                RuntimeException failure = failNextRead.getAndSet(null);
                return failure == null ? delegate.read(criteria, options) : Mono.error(failure);
            });
        }

        private void armReReadFailures() {
            failNextRead.set(failAfterNextAppend.getAndSet(null));
            throwFromNextRead.set(throwAfterNextAppend.getAndSet(null));
        }

        @Override
        public Mono<DcbAppendResult> append(List<CloudEvent> events) {
            return Mono.defer(() -> {
                appendCalls.incrementAndGet();
                return delegate.append(events).doOnNext(__ -> armReReadFailures());
            });
        }

        @Override
        public Mono<DcbAppendResult> append(List<CloudEvent> events, DcbAppendCondition condition) {
            return Mono.defer(() -> {
                appendCalls.incrementAndGet();
                List<CloudEvent> concurrent = concurrentEvents.getAndSet(null);
                Mono<Void> before = concurrent == null ? Mono.empty() : delegate.append(concurrent).then();
                return before.then(delegate.append(events, condition)).doOnNext(__ -> armReReadFailures());
            });
        }
    }
}
