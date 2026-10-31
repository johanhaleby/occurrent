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
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.generic.GenericCloudEventConverter;
import org.occurrent.application.service.blocking.SynchronousEventDispatcher;
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
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;

/**
 * The executor's own conflict retry matches the exceptions Spring uses for transient data access failures. With a
 * template that opens no transaction, a synchronous handler throwing one of them after the write must not make that
 * retry run the command, and so the write, again. The same goes for a handler failing after the write inside a transaction
 * the caller opened, since the executor only joins that one and its rollback is not this command's to undo.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringTransactionExecutorFailureAfterWriteTest {

    private static final Tag TAG = Tag.parse("name:1");
    private static final DcbCriteria CRITERIA = tags(TAG);

    private InMemoryEventStore eventStore;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private StubTransactionManager transactionManager;
    private SpringTransactionExecutor executor;
    private String streamId;
    private final AtomicInteger decisions = new AtomicInteger();
    private final List<String> decidedEventIds = new ArrayList<>();

    @BeforeEach
    void initialize() {
        DomainEventConverter domainEventConverter = new DomainEventConverter(new ObjectMapper());
        cloudEventConverter = new GenericCloudEventConverter<>(domainEventConverter::convertToDomainEvent, domainEventConverter::convertToCloudEvent);
        eventStore = new InMemoryEventStore();
        transactionManager = new StubTransactionManager();
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_NOT_SUPPORTED);
        executor = new SpringTransactionExecutor(transactionTemplate);
        streamId = UUID.randomUUID().toString();
    }

    @Test
    void a_transient_data_access_failure_in_a_handler_after_the_write_reaches_the_caller_without_running_the_command_again_under_propagation_not_supported() {
        TransientHandlerFailure failure = new TransientHandlerFailure();
        var applicationService = GenericApplicationService.builder(eventStore, cloudEventConverter)
                .synchronousSubscriptions(new FailingOnceDispatcher(failure))
                .transactionExecutor(executor)
                .build();

        Throwable thrown = catchThrowable(() -> applicationService.execute(streamId, this::decide));

        assertThat(thrown).as("exception reaching the caller").isSameAs(failure);
        assertThat(decisions.get()).as("domain function runs").isEqualTo(1);
        assertThat(eventsInStream()).as("events in the stream").hasSize(1).isEqualTo(decidedEventIds);
    }

    @Test
    void a_transient_data_access_failure_in_a_handler_after_the_append_reaches_the_caller_without_running_the_command_again_under_propagation_not_supported() {
        TransientHandlerFailure failure = new TransientHandlerFailure();
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new FailingOnceDispatcher(failure))
                .transactionExecutor(executor)
                .build();

        Throwable thrown = catchThrowable(() -> applicationService.execute(CRITERIA, this::decide));

        assertThat(thrown).as("exception reaching the caller").isSameAs(failure);
        assertThat(decisions.get()).as("domain function runs").isEqualTo(1);
        assertThat(eventsInBoundary()).as("events in the boundary").hasSize(1).isEqualTo(decidedEventIds);
    }

    @Test
    void a_retried_conflict_thrown_by_a_handler_after_the_write_reaches_the_caller_without_running_the_command_again_in_the_callers_open_transaction() {
        WriteConditionNotFulfilledException conflict = new WriteConditionNotFulfilledException(streamId, 1, WriteCondition.anyStreamVersion());
        var applicationService = GenericApplicationService.builder(eventStore, cloudEventConverter)
                .synchronousSubscriptions(new FailingOnceDispatcher(conflict))
                .transactionExecutor(new SpringTransactionExecutor(transactionManager))
                .build();

        Throwable thrown = catchThrowable(() -> new TransactionTemplate(transactionManager).executeWithoutResult(status -> applicationService.execute(streamId, this::decide)));

        assertThat(thrown).as("[exception reaching the caller]").isSameAs(conflict);
        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(1);
        assertThat(transactionManager.begun).as("[transactions begun]").isEqualTo(1);
    }

    @Test
    void a_retried_conflict_thrown_by_a_handler_after_the_append_reaches_the_caller_without_running_the_command_again_in_the_callers_open_transaction() {
        DcbAppendConditionNotFulfilledException conflict = new DcbAppendConditionNotFulfilledException(DcbAppendCondition.failIfEventsMatch(CRITERIA), 1);
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new FailingOnceDispatcher(conflict))
                .transactionExecutor(new SpringTransactionExecutor(transactionManager))
                .build();

        Throwable thrown = catchThrowable(() -> new TransactionTemplate(transactionManager).executeWithoutResult(status -> applicationService.execute(CRITERIA, this::decide)));

        assertThat(thrown).as("[exception reaching the caller]").isSameAs(conflict);
        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(1);
        assertThat(transactionManager.begun).as("[transactions begun]").isEqualTo(1);
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

    private static final class TransientHandlerFailure extends TransientDataAccessException {
        TransientHandlerFailure() {
            super("handler failed");
        }
    }

    // Fails the first dispatch, which would make a second attempt succeed if the retry ran the command again
    private static final class FailingOnceDispatcher implements SynchronousEventDispatcher {
        private final AtomicBoolean failed = new AtomicBoolean();
        private final RuntimeException failure;

        FailingOnceDispatcher(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public void dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
            if (failed.compareAndSet(false, true)) {
                throw failure;
            }
        }

        @Override
        public boolean hasSubscriptions() {
            return true;
        }
    }

    // A transaction manager with no real resource to manage. PROPAGATION_NOT_SUPPORTED never begins a transaction on it.
    // While one is open it binds a marker, so a template with PROPAGATION_REQUIRED joins it rather than beginning another.
    private static final class StubTransactionManager extends AbstractPlatformTransactionManager {
        private static final Object OPEN = new Object();
        int begun;

        @Override
        protected Object doGetTransaction() {
            return TransactionSynchronizationManager.hasResource(this) ? OPEN : new Object();
        }

        @Override
        protected boolean isExistingTransaction(Object transaction) {
            return transaction == OPEN;
        }

        @Override
        protected void doBegin(Object transaction, TransactionDefinition definition) {
            begun++;
            TransactionSynchronizationManager.bindResource(this, OPEN);
        }

        @Override
        protected void doSetRollbackOnly(DefaultTransactionStatus status) {
        }

        @Override
        protected void doCommit(DefaultTransactionStatus status) {
        }

        @Override
        protected void doRollback(DefaultTransactionStatus status) {
        }

        @Override
        protected void doCleanupAfterCompletion(Object transaction) {
            TransactionSynchronizationManager.unbindResource(this);
        }
    }
}
