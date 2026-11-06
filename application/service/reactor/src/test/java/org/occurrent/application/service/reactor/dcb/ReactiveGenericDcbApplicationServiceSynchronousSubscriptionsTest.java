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
import org.junit.jupiter.api.AfterEach;
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
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.api.dcb.Tag;
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
import reactor.test.StepVerifier;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactiveGenericDcbApplicationServiceSynchronousSubscriptionsTest {

    private static final Tag TAG = Tag.parse("name:1");

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flush = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private ReactorMongoEventStore eventStore;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private MongoClient mongoClient;

    @BeforeEach
    void initialize() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcbsynchronoussubscriptions");
        mongoClient = MongoClients.create(connectionString);
        ReactiveMongoTemplate mongoTemplate = new ReactiveMongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        ReactiveMongoTransactionManager transactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        eventStore = new ReactorMongoEventStore(mongoTemplate, config);
        DomainEventConverter domainEventConverter = new DomainEventConverter(new ObjectMapper());
        cloudEventConverter = new GenericCloudEventConverter<>(domainEventConverter::convertToDomainEvent, domainEventConverter::convertToCloudEvent);
    }

    @AfterEach
    void closeMongoClient() {
        mongoClient.close();
    }

    @Test
    void tells_the_dispatcher_there_is_no_transaction_when_the_default_executor_is_used() {
        RegimeRecordingDispatcher dispatcher = new RegimeRecordingDispatcher();
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(dispatcher)
                .build();

        StepVerifier.create(applicationService.execute(tags(TAG), events -> List.of(nameDefined())))
                .expectNextCount(1)
                .verifyComplete();

        // The default is ReactiveTransactionExecutor.noTransaction(), so handlers must be isolated from each other.
        assertThat(dispatcher.toldTransactional).isFalse();
    }

    @Test
    void tells_the_dispatcher_there_is_no_transaction_when_the_executor_only_joins_one_and_does_not_say_a_failure_rolls_it_back() {
        assertThat(toldTransactionalWith(new JoiningExecutor(false))).isFalse();
    }

    @Test
    void tells_the_dispatcher_there_is_a_transaction_when_the_executor_only_joins_one_and_says_a_failure_rolls_it_back() {
        assertThat(toldTransactionalWith(new JoiningExecutor(true))).isTrue();
    }

    @Test
    void tells_the_dispatcher_there_is_a_transaction_when_the_executor_opens_one() {
        assertThat(toldTransactionalWith(new OpeningExecutor())).isTrue();
    }

    @Test
    void tells_the_dispatcher_there_is_a_transaction_when_the_executor_tells_the_attempt_that_it_opened_one() {
        ReactiveTransactionExecutor executor = new JoiningExecutor(false) {
            @Override
            public <T> Mono<T> inTransactionTellingIfOpened(Function<Boolean, Mono<T>> action) {
                return inTransaction(() -> action.apply(true));
            }
        };

        assertThat(toldTransactionalWith(executor)).isTrue();
    }

    private @Nullable Boolean toldTransactionalWith(ReactiveTransactionExecutor executor) {
        RegimeRecordingDispatcher dispatcher = new RegimeRecordingDispatcher();
        var applicationService = GenericDcbApplicationService.builder(eventStore, cloudEventConverter)
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(dispatcher)
                .transactionExecutor(executor)
                .build();

        StepVerifier.create(applicationService.execute(tags(TAG), events -> List.of(nameDefined())))
                .expectNextCount(1)
                .verifyComplete();

        return dispatcher.toldTransactional;
    }

    private static DomainEvent nameDefined() {
        return new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Johan");
    }

    // Reports a transaction throughout, as an executor does that joins one the caller already opened
    private static class JoiningExecutor implements ReactiveTransactionExecutor {
        private final boolean rollsBackAJoinedTransactionOnFailure;

        JoiningExecutor(boolean rollsBackAJoinedTransactionOnFailure) {
            this.rollsBackAJoinedTransactionOnFailure = rollsBackAJoinedTransactionOnFailure;
        }

        @Override
        public <T> Mono<T> inTransaction(Supplier<Mono<T>> action) {
            return Mono.defer(action);
        }

        @Override
        public Mono<Boolean> isTransactional() {
            return Mono.just(true);
        }

        @Override
        public boolean rollsBackAJoinedTransactionOnFailure() {
            return rollsBackAJoinedTransactionOnFailure;
        }
    }

    // Emits no transaction until its own inTransaction has opened one
    private static final class OpeningExecutor implements ReactiveTransactionExecutor {
        private boolean open;

        @Override
        public <T> Mono<T> inTransaction(Supplier<Mono<T>> action) {
            return Mono.defer(() -> {
                open = true;
                return action.get();
            }).doFinally(__ -> open = false);
        }

        @Override
        public Mono<Boolean> isTransactional() {
            return Mono.fromSupplier(() -> open);
        }
    }

    private static final class RegimeRecordingDispatcher implements ReactiveSynchronousEventDispatcher {
        private @Nullable Boolean toldTransactional;

        @Override
        public Mono<Void> dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
            return Mono.fromRunnable(() -> toldTransactional = transactional);
        }

        @Override
        public boolean hasSubscriptions() {
            return true;
        }
    }
}
