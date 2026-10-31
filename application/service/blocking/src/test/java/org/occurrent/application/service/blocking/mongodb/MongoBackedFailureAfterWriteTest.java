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

package org.occurrent.application.service.blocking.mongodb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.generic.GenericCloudEventConverter;
import org.occurrent.application.service.blocking.SynchronousEventDispatcher;
import org.occurrent.application.service.blocking.TransactionExecutor;
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService;
import org.occurrent.application.service.blocking.generic.GenericApplicationService;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.DomainEventConverter;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;

/**
 * Without a transaction the write has committed by the time a synchronous handler runs, so a retried conflict it throws
 * must reach the caller as it is, over a real MongoDB event store. Running the command again would decide and write it
 * a second time. The subclasses supply the store.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
abstract class MongoBackedFailureAfterWriteTest {

    private static final Tag TAG = Tag.parse("name:1");
    private static final DcbCriteria CRITERIA = tags(TAG);

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    MongoClient mongoClient;
    String databaseName;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private final AtomicInteger decisions = new AtomicInteger();
    private final List<String> decidedEventIds = new ArrayList<>();

    @BeforeEach
    void connect() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".failureafterwrite");
        mongoClient = MongoClients.create(connectionString);
        databaseName = requireNonNull(connectionString.getDatabase());
        DomainEventConverter domainEventConverter = new DomainEventConverter(new ObjectMapper());
        cloudEventConverter = new GenericCloudEventConverter<>(domainEventConverter::convertToDomainEvent, domainEventConverter::convertToCloudEvent);
    }

    @AfterEach
    void closeClient() {
        mongoClient.close();
    }

    /**
     * An event store over a real MongoDB that supports streams and DCB.
     */
    abstract <S extends EventStore & DcbEventStore> S eventStore();

    @Test
    void a_retried_conflict_thrown_by_a_handler_after_the_write_reaches_the_caller_without_running_the_command_again_on_a_stream() {
        EventStore eventStore = eventStore();
        String streamId = UUID.randomUUID().toString();
        WriteConditionNotFulfilledException conflict = new WriteConditionNotFulfilledException(streamId, 1, WriteCondition.anyStreamVersion());
        var applicationService = GenericApplicationService.builder(eventStore, cloudEventConverter)
                .synchronousSubscriptions(new FailingOnceDispatcher(conflict))
                .transactionExecutor(TransactionExecutor.noTransaction())
                .build();

        Throwable thrown = catchThrowable(() -> applicationService.execute(streamId, this::decide));

        assertThat(decisions.get()).as("domain function runs").isEqualTo(1);
        assertThat(thrown).as("exception reaching the caller").isSameAs(conflict);
        assertThat(eventStore.read(streamId).events().map(CloudEvent::getId).toList()).as("events in the stream").isEqualTo(decidedEventIds);
    }

    @Test
    void a_retried_conflict_thrown_by_a_handler_after_the_append_reaches_the_caller_without_running_the_command_again_on_a_dcb_boundary() {
        DcbEventStore eventStore = eventStore();
        DcbAppendConditionNotFulfilledException conflict = new DcbAppendConditionNotFulfilledException(DcbAppendCondition.failIfEventsMatch(CRITERIA), 1);
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(new FailingOnceDispatcher(conflict))
                .transactionExecutor(TransactionExecutor.noTransaction())
                .build();

        Throwable thrown = catchThrowable(() -> applicationService.execute(CRITERIA, this::decide));

        assertThat(decisions.get()).as("domain function runs").isEqualTo(1);
        assertThat(thrown).as("exception reaching the caller").isSameAs(conflict);
        assertThat(eventStore.read(CRITERIA).events().stream().map(CloudEvent::getId).toList()).as("events in the boundary").isEqualTo(decidedEventIds);
    }

    private List<DomainEvent> decide(List<DomainEvent> events) {
        decisions.incrementAndGet();
        DomainEvent decided = new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Johan");
        decidedEventIds.add(decided.eventId());
        return List.of(decided);
    }

    private static final class FailingOnceDispatcher implements SynchronousEventDispatcher {
        private @Nullable RuntimeException failure;

        FailingOnceDispatcher(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public void dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
            RuntimeException toThrow = failure;
            failure = null;
            if (toThrow != null) {
                throw toThrow;
            }
        }

        @Override
        public boolean hasSubscriptions() {
            return true;
        }
    }
}
