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

package org.occurrent.application.service.blocking.dcb;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.generic.GenericCloudEventConverter;
import org.occurrent.application.service.blocking.SynchronousEventDispatcher;
import org.occurrent.application.service.blocking.TransactionExecutor;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.DomainEventConverter;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.retry.RetryStrategy;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;

/**
 * Without a transaction the append has committed by the time the re-read and the synchronous dispatch run, so a failure
 * there must reach the caller as it is. Running the command again would decide and append it a second time.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class GenericDcbApplicationServiceFailureAfterWriteTest {

    private static final Tag TAG = Tag.parse("name:1");
    private static final DcbCriteria CRITERIA = tags(TAG);
    // The events of the own-retry test carry this tag, so the append condition built from a read of CRITERIA can't fail on them
    private static final Tag UNREAD_TAG = Tag.parse("name:unread");
    private static final RetryStrategy.Retry RETRY_EVERYTHING = RetryStrategy.fixed(Duration.ofMillis(1)).maxAttempts(5).retryIf(__ -> true);

    private InMemoryEventStore inMemoryEventStore;
    private FailingReReadEventStore eventStore;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private final AtomicInteger decisions = new AtomicInteger();
    private final List<String> decidedEventIds = new ArrayList<>();

    @BeforeEach
    void initialize() {
        DomainEventConverter domainEventConverter = new DomainEventConverter(new ObjectMapper());
        cloudEventConverter = new GenericCloudEventConverter<>(domainEventConverter::convertToDomainEvent, domainEventConverter::convertToCloudEvent);
        inMemoryEventStore = new InMemoryEventStore();
        eventStore = new FailingReReadEventStore(inMemoryEventStore);
    }

    @Test
    void a_retried_conflict_thrown_by_a_handler_after_the_append_reaches_the_caller_without_running_the_command_again() {
        DcbAppendConditionNotFulfilledException conflict = new DcbAppendConditionNotFulfilledException(DcbAppendCondition.failIfEventsMatch(CRITERIA), 1);
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new FailingOnceDispatcher(conflict))
                .transactionExecutor(TransactionExecutor.noTransaction())
                .build();

        Throwable thrown = catchThrowable(() -> applicationService.execute(CRITERIA, this::decide));

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
                .transactionExecutor(TransactionExecutor.noTransaction())
                .retryStrategy(RETRY_EVERYTHING)
                .build();

        Throwable thrown = catchThrowable(() -> applicationService.execute(CRITERIA, this::decide));

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
                .transactionExecutor(TransactionExecutor.noTransaction())
                .retryStrategy(RETRY_EVERYTHING)
                .build();

        Throwable thrown = catchThrowable(() -> applicationService.execute(CRITERIA, this::decide));

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
                .transactionExecutor(TransactionExecutor.noTransaction())
                .build();
        AtomicInteger sideEffectRuns = new AtomicInteger();

        catchThrowable(() -> applicationService.execute(CRITERIA, DcbExecuteOptions.<DomainEvent>options().sideEffect(events -> sideEffectRuns.incrementAndGet()), this::decide));

        assertThat(sideEffectRuns.get()).as("side effect runs").isZero();
    }

    @Test
    void a_checked_exception_thrown_sneakily_by_a_handler_reaches_the_caller_when_the_retry_matches_every_throwable() {
        IOException failure = new IOException("handler failed");
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new FailingOnceDispatcher(failure))
                .transactionExecutor(TransactionExecutor.noTransaction())
                .retryStrategy(RetryStrategy.fixed(Duration.ofMillis(1)).maxAttempts(5).retryIf(Throwable.class::isInstance))
                .build();

        Throwable thrown = catchThrowable(() -> applicationService.execute(CRITERIA, this::decide));

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

        Throwable thrown = catchThrowable(() -> applicationService.execute(CRITERIA, this::decide));

        assertThat(thrown).as("exception reaching the caller").isSameAs(failure);
        assertThat(executor.commits).as("commits").isZero();
        assertThat(executor.rollbacks).as("rollbacks").isEqualTo(1);
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

        catchThrowable(() -> applicationService.execute(CRITERIA, this::decide));

        assertThat(decisions.get()).as("domain function runs").isEqualTo(2);
        assertThat(executor.rollbacks).as("rollbacks").isEqualTo(1);
        assertThat(executor.commits).as("commits").isEqualTo(1);
    }

    @Test
    void a_conflict_on_the_append_itself_still_runs_the_command_again_from_a_fresh_read_without_a_transaction() {
        RecordingDispatcher dispatcher = new RecordingDispatcher();
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(dispatcher)
                .transactionExecutor(TransactionExecutor.noTransaction())
                .build();
        List<String> concurrentEventIds = new ArrayList<>();

        Throwable thrown = catchThrowable(() -> applicationService.execute(CRITERIA, events -> {
            if (decisions.get() == 0) {
                DomainEvent concurrent = nameWasChanged();
                concurrentEventIds.add(concurrent.eventId());
                inMemoryEventStore.append(cloudEventConverter.toCloudEvents(List.of(concurrent)).stream().map(event -> DcbCloudEvents.withTags(event, Set.of(TAG))).toList());
            }
            return decide(events);
        }));

        assertThat(decisions.get()).as("domain function runs").isEqualTo(2);
        assertThat(thrown).as("exception reaching the caller").isNull();
        assertThat(eventsInBoundary()).as("events in the boundary").isEqualTo(List.of(concurrentEventIds.getFirst(), decidedEventIds.getLast()));
        assertThat(dispatcher.dispatchedEventIds).as("dispatched events").containsExactly(decidedEventIds.getLast());
    }

    @Test
    void a_failure_after_the_append_reaches_the_caller_as_it_is_when_the_executor_translates_exceptions_and_the_retry_matches_every_exception() {
        RuntimeException failure = new RuntimeException("handler failed");
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new FailingOnceDispatcher(failure))
                .transactionExecutor(new TranslatesExceptions())
                .retryStrategy(RETRY_EVERYTHING)
                .build();

        Throwable thrown = catchThrowable(() -> applicationService.execute(CRITERIA, this::decide));

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
                .retryStrategy(RETRY_EVERYTHING)
                .build();

        Throwable thrown = catchThrowable(() -> applicationService.execute(CRITERIA, this::decide));

        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(1);
        assertThat(thrown).as("[exception reaching the caller]").isSameAs(failure);
        assertThat(eventStore.appendCalls).as("[appends attempted]").isEqualTo(1);
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

        Throwable thrown = catchThrowable(() -> applicationService.execute(CRITERIA, DcbExecuteOptions.<DomainEvent>options().sideEffect(events -> sideEffectRuns.incrementAndGet()), this::decide));

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
        List<String> concurrentEventIds = new ArrayList<>();

        Throwable thrown = catchThrowable(() -> applicationService.execute(CRITERIA, events -> {
            if (decisions.get() == 0) {
                DomainEvent concurrent = nameWasChanged();
                concurrentEventIds.add(concurrent.eventId());
                inMemoryEventStore.append(cloudEventConverter.toCloudEvents(List.of(concurrent)).stream().map(event -> DcbCloudEvents.withTags(event, Set.of(TAG))).toList());
            }
            return decide(events);
        }));

        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(2);
        assertThat(thrown).as("[exception reaching the caller]").isNull();
        assertThat(eventsInBoundary()).as("[events in the boundary]").isEqualTo(List.of(concurrentEventIds.getFirst(), decidedEventIds.getLast()));
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

        Throwable thrown = catchThrowable(() -> applicationService.execute(CRITERIA, this::decide));

        assertThat(thrown).as("[exception reaching the caller]").isSameAs(failure);
        assertThat(thrown.getSuppressed()).as("[exceptions suppressed by the failure]").containsExactly(executor.cleanupFailure);
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

        Throwable thrown = catchThrowable(() -> applicationService.execute(CRITERIA, this::decide));

        assertThat(thrown).as("[exception reaching the caller]").isSameAs(failure);
        assertThat(thrown.getSuppressed()).as("[exceptions suppressed by the failure]").containsExactly(executor.replacement);
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

        Throwable thrown = catchThrowable(() -> applicationService.execute(CRITERIA, this::decide));

        assertThat(thrown).as("[exception reaching the caller]").isSameAs(failure);
        assertThat(thrown.getSuppressed()).as("[exceptions suppressed by the failure]").isEmpty();
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

        Throwable thrown = catchThrowable(() -> applicationService.execute(CRITERIA, this::decide));

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

        Throwable thrown = catchThrowable(() -> applicationService.execute(CRITERIA, this::decide));

        assertThat(thrown).as("[exception reaching the caller]").isSameAs(failure);
        assertThat(thrown.getSuppressed()).as("[exceptions suppressed by the failure]").isEmpty();
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
        return inMemoryEventStore.read(criteria).events().stream().map(CloudEvent::getId).toList();
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable throwable) throws T {
        throw (T) throwable;
    }

    private static final class FailingOnceDispatcher implements SynchronousEventDispatcher {
        private @Nullable Throwable failure;

        FailingOnceDispatcher(Throwable failure) {
            this.failure = failure;
        }

        @Override
        public void dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
            Throwable toThrow = failure;
            failure = null;
            if (toThrow != null) {
                GenericDcbApplicationServiceFailureAfterWriteTest.<RuntimeException>sneakyThrow(toThrow);
            }
        }

        @Override
        public boolean hasSubscriptions() {
            return true;
        }
    }

    private static final class RecordingDispatcher implements SynchronousEventDispatcher {
        private final List<String> dispatchedEventIds = new ArrayList<>();

        @Override
        public void dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
            writtenCloudEvents.forEach(event -> dispatchedEventIds.add(event.getId()));
        }

        @Override
        public boolean hasSubscriptions() {
            return true;
        }
    }

    // Stand-in for an executor that opens a transaction. It keeps the default isTransactional of false.
    private static class OpensTransaction implements TransactionExecutor {
        int commits;
        int rollbacks;

        @Override
        public <T> T inTransaction(Supplier<T> action) {
            T result;
            try {
                result = action.get();
            } catch (Throwable throwable) {
                rollbacks++;
                throw throwable;
            }
            commits++;
            return result;
        }
    }

    // Stand-in for an executor that owns its transaction and reports it. It answers true only inside inTransaction.
    private static final class OwnsTransaction extends OpensTransaction {
        private boolean inside;

        @Override
        public <T> T inTransaction(Supplier<T> action) {
            inside = true;
            try {
                return super.inTransaction(action);
            } finally {
                inside = false;
            }
        }

        @Override
        public boolean isTransactional() {
            return inside;
        }
    }

    // Stand-in for an executor that runs in a transaction the caller opened. It answers true before and inside inTransaction, and runs the action once.
    private static final class JoinsTransaction implements TransactionExecutor {
        @Override
        public <T> T inTransaction(Supplier<T> action) {
            return action.get();
        }

        @Override
        public boolean isTransactional() {
            return true;
        }
    }

    // Stand-in for an executor that translates what passes through it and opens no transaction
    private static final class TranslatesExceptions implements TransactionExecutor {
        @Override
        public <T> T inTransaction(Supplier<T> action) {
            try {
                return action.get();
            } catch (RuntimeException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    // Stand-in for an executor with a retry loop of its own that opens no transaction and keeps the default isTransactional of false
    private static final class RetriesOnItsOwn implements TransactionExecutor {
        @Override
        public <T> T inTransaction(Supplier<T> action) {
            int attempts = 0;
            while (true) {
                try {
                    return action.get();
                } catch (RuntimeException e) {
                    if (++attempts == 3) {
                        throw e;
                    }
                }
            }
        }
    }

    // Stand-in for an executor whose cleanup fails after the action did, and attaches that to what passes through it
    private static final class SuppressesCleanupFailure implements TransactionExecutor {
        final RuntimeException cleanupFailure = new IllegalStateException("cleanup failed");

        @Override
        public <T> T inTransaction(Supplier<T> action) {
            try {
                return action.get();
            } catch (RuntimeException e) {
                e.addSuppressed(cleanupFailure);
                throw e;
            }
        }
    }

    // Stand-in for an executor that replaces what passes through it, as a rollback that fails does, and opens no transaction
    private static final class ReplacesWhatPassesThrough implements TransactionExecutor {
        final RuntimeException replacement = new IllegalStateException("rollback failed");

        @Override
        public <T> T inTransaction(Supplier<T> action) {
            try {
                return action.get();
            } catch (RuntimeException e) {
                throw replacement;
            }
        }
    }

    // Stand-in for an executor that answers before entry and fails inside, as one whose transaction can't be read there does. It opens no transaction.
    private static final class FailsToReportInside implements TransactionExecutor {
        final RuntimeException failure = new IllegalStateException("transaction state unavailable");
        private int asked;

        @Override
        public <T> T inTransaction(Supplier<T> action) {
            return action.get();
        }

        @Override
        public boolean isTransactional() {
            if (++asked > 1) {
                throw failure;
            }
            return false;
        }
    }

    // Stand-in for an executor that wraps what passes through it twice and opens no transaction
    private static final class WrapsTwice implements TransactionExecutor {
        @Override
        public <T> T inTransaction(Supplier<T> action) {
            try {
                return action.get();
            } catch (RuntimeException e) {
                throw new IllegalStateException(new RuntimeException(e));
            }
        }
    }

    // Fails the first read after the next append succeeded, which is the re-read of the just-appended block
    private static final class FailingReReadEventStore implements DcbEventStore {
        private final DcbEventStore delegate;
        private @Nullable RuntimeException failAfterNextAppend;
        private @Nullable RuntimeException failNextRead;
        int appendCalls;

        FailingReReadEventStore(DcbEventStore delegate) {
            this.delegate = delegate;
        }

        void failReReadAfterNextAppend(RuntimeException failure) {
            this.failAfterNextAppend = failure;
        }

        @Override
        public DcbEventStream read(DcbCriteria criteria, DcbReadOptions options) {
            RuntimeException failure = failNextRead;
            if (failure != null) {
                failNextRead = null;
                throw failure;
            }
            return delegate.read(criteria, options);
        }

        @Override
        public DcbAppendResult append(List<CloudEvent> events) {
            appendCalls++;
            return armReReadFailure(delegate.append(events));
        }

        @Override
        public DcbAppendResult append(List<CloudEvent> events, DcbAppendCondition condition) {
            appendCalls++;
            return armReReadFailure(delegate.append(events, condition));
        }

        private DcbAppendResult armReReadFailure(DcbAppendResult result) {
            failNextRead = failAfterNextAppend;
            failAfterNextAppend = null;
            return result;
        }
    }
}
