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

package org.occurrent.springboot.mongo.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.service.blocking.SynchronousEventDispatcher;
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService;
import org.occurrent.application.service.spring.SpringTransactionExecutor;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.DomainEventConverter;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.ClassUtils;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * A DCB command that asks for no marker versions reads and decides inside the transaction a {@code SpringTransactionExecutor}
 * opens for synchronous subscriptions, as it did in 0.34.0. A write the domain function makes then rolls back with that
 * transaction, and the executor can run the command again after a transient read failure.
 */
@DisplayName("A DCB command in a Spring transaction executor's transaction")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(120)
class DcbCommandInSpringTransactionExecutorMongoTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private static final SynchronousEventDispatcher DISPATCHER = new SynchronousEventDispatcher() {
        @Override
        public void dispatch(List<CloudEvent> written, boolean transactional) {
        }

        @Override
        public boolean hasSubscriptions() {
            return true;
        }
    };

    private MongoClient mongoClient;
    private MongoTransactionManager transactionManager;
    private MongoTemplate mongoTemplate;
    private SpringMongoEventStore eventStore;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private Tag tag;

    @BeforeEach
    void create_event_store() {
        String databaseName = "dcb-command-in-executor-" + UUID.randomUUID();
        mongoClient = MongoClients.create(new ConnectionString(mongoDBContainer.getReplicaSetUrl()));
        SimpleMongoClientDatabaseFactory databaseFactory = new SimpleMongoClientDatabaseFactory(mongoClient, databaseName);
        transactionManager = new MongoTransactionManager(databaseFactory);
        mongoTemplate = new MongoTemplate(databaseFactory);
        mongoTemplate.createCollection("domain-writes");
        eventStore = new SpringMongoEventStore(mongoTemplate, new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build());
        DomainEventConverter domainEventConverter = new DomainEventConverter(new ObjectMapper());
        cloudEventConverter = new CloudEventConverter<>() {
            @Override
            public CloudEvent toCloudEvent(DomainEvent domainEvent) {
                return domainEventConverter.convertToCloudEvent(domainEvent);
            }

            @Override
            public DomainEvent toDomainEvent(CloudEvent cloudEvent) {
                return domainEventConverter.convertToDomainEvent(cloudEvent);
            }

            @Override
            public String getCloudEventType(Class<? extends DomainEvent> type) {
                return type.getName();
            }
        };
        tag = Tag.parse("command:" + UUID.randomUUID().toString().replace("-", ""));
    }

    @AfterEach
    void close_client() {
        mongoClient.close();
    }

    enum CallersScope {
        NONE, SUPPORTS, NOT_SUPPORTED
    }

    @ParameterizedTest
    @EnumSource(CallersScope.class)
    void a_write_the_domain_function_makes_before_it_throws_rolls_back_with_the_executors_transaction(CallersScope callersScope) {
        String domainWrite = UUID.randomUUID().toString();
        var failure = new IllegalStateException("domain failure");
        var service = service(eventStore);

        Throwable thrown = catchThrowable(() -> inCallersScope(callersScope, () -> service.execute(DcbCriteria.tags(tag), events -> {
            mongoTemplate.insert(new Document("_id", domainWrite), "domain-writes");
            throw failure;
        })));

        assertAll(
                () -> assertThat(thrown).as("what reached the caller").isSameAs(failure),
                () -> assertThat(mongoTemplate.count(Query.query(Criteria.where("_id").is(domainWrite)), "domain-writes"))
                        .as("domain writes stored").isZero()
        );
    }

    @Test
    void the_executor_runs_the_command_again_after_a_transient_read_failure() {
        var reads = new AtomicInteger();
        DcbEventStore failingOnTheFirstRead = (DcbEventStore) Proxy.newProxyInstance(getClass().getClassLoader(),
                ClassUtils.getAllInterfacesForClass(SpringMongoEventStore.class), (proxy, method, args) -> {
                    if (method.getName().equals("read") && reads.incrementAndGet() == 1) {
                        throw new QueryTimeoutException("transient read failure");
                    }
                    try {
                        return method.invoke(eventStore, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        var service = service(failingOnTheFirstRead);

        Throwable thrown = catchThrowable(() -> service.execute(DcbCriteria.tags(tag), events -> List.of(new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "changed"))));

        assertAll(
                () -> assertThat(thrown).as("what reached the caller").isNull(),
                () -> assertThat(eventStore.read(DcbCriteria.tags(tag)).events()).as("events stored").hasSize(1)
        );
    }

    // No retry in the service, so a command that runs again was run again by the executor
    private GenericDcbApplicationService<DomainEvent> service(DcbEventStore store) {
        return GenericDcbApplicationService.builder(store, cloudEventConverter)
                .tagGenerator(event -> Set.of(tag))
                .synchronousSubscriptions(DISPATCHER)
                .transactionExecutor(new SpringTransactionExecutor(new TransactionTemplate(transactionManager)))
                .retryStrategy(RetryStrategy.none())
                .build();
    }

    private void inCallersScope(CallersScope callersScope, Runnable action) {
        if (callersScope == CallersScope.NONE) {
            action.run();
            return;
        }
        TransactionTemplate callersTemplate = new TransactionTemplate(transactionManager);
        callersTemplate.setPropagationBehavior(callersScope == CallersScope.SUPPORTS ? TransactionDefinition.PROPAGATION_SUPPORTS : TransactionDefinition.PROPAGATION_NOT_SUPPORTED);
        callersTemplate.executeWithoutResult(status -> action.run());
    }
}
