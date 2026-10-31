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

package org.occurrent.dsl.snapshot.reactor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.reactor.ReactiveSynchronousEventDispatcher;
import org.occurrent.application.service.reactor.ReactiveTransactionExecutor;
import org.occurrent.application.service.reactor.dcb.GenericDcbApplicationService;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.dsl.dcb.DcbDecider;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.snapshot.SnapshotOptions;
import org.occurrent.dsl.snapshot.SnapshotPolicy;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
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

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * Without a transaction the append has committed when a synchronous handler fails, so the snapshot decider must hand
 * that failure to the caller instead of deciding the command a second time.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactiveSnapshotDcbDeciderFailureAfterAppendTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);
    private static final Tag ACCOUNT = Tag.of("name", "acct");
    private static final DcbCriteria CRITERIA = DcbCriteria.tags(ACCOUNT);

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private ReactorMongoEventStore eventStore;
    private CloudEventConverter<DomainEvent> converter;
    private final AtomicInteger decides = new AtomicInteger();

    @BeforeEach
    void initialize() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".snapshotfailureafterappend");
        MongoClient mongoClient = MongoClients.create(connectionString);
        ReactiveMongoTemplate mongoTemplate = new ReactiveMongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        ReactiveMongoTransactionManager transactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        eventStore = new ReactorMongoEventStore(mongoTemplate, config);
        converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
    }

    @Test
    void a_retried_conflict_thrown_by_a_handler_after_the_commit_reaches_the_caller_when_a_snapshot_exists() {
        DcbAppendConditionNotFulfilledException conflict = new DcbAppendConditionNotFulfilledException(DcbAppendCondition.failIfEventsMatch(CRITERIA), 1);
        FailingOnceDispatcher dispatcher = new FailingOnceDispatcher();
        var service = new ReactiveSnapshotDcbDeciderApplicationService<>(GenericDcbApplicationService.builder(eventStore, converter)
                .tagGenerator((DomainEvent event) -> Set.of(ACCOUNT))
                .synchronousSubscriptions(dispatcher)
                .transactionExecutor(ReactiveTransactionExecutor.noTransaction())
                .build());
        ReactiveSnapshotDcbDecider<String, Integer, DomainEvent> decider = ReactiveSnapshotDcbDecider.from(DcbDecider.from(counting(), command -> CRITERIA, event -> Set.of(ACCOUNT)),
                ReactiveSnapshotStore.inMemory(), SnapshotOptions.of(1, SnapshotPolicy.always()));
        service.execute("first", decider).block(TIMEOUT);
        service.execute("second", decider).block(TIMEOUT);
        decides.set(0);
        dispatcher.failWith(conflict);

        Throwable thrown = service.execute("third", decider).then(Mono.<Throwable>empty()).onErrorResume(Mono::just).block(TIMEOUT);

        assertThat(decides.get()).as("decide runs").isEqualTo(1);
        assertThat(thrown).as("exception reaching the caller").isSameAs(conflict);
        assertThat(requireNonNull(eventStore.read(CRITERIA).block(TIMEOUT)).events()).as("events in the boundary").hasSize(3);
    }

    private Decider<String, Integer, DomainEvent> counting() {
        return new Decider<>() {
            @Override
            public Integer initialState() {
                return 0;
            }

            @NonNull
            @Override
            public List<DomainEvent> decide(@NonNull String command, Integer state) {
                decides.incrementAndGet();
                return List.of(new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", command));
            }

            @Override
            public Integer evolve(Integer state, @NonNull DomainEvent event) {
                return state + 1;
            }
        };
    }

    private static final class FailingOnceDispatcher implements ReactiveSynchronousEventDispatcher {
        private final AtomicReference<@Nullable RuntimeException> failure = new AtomicReference<>();

        void failWith(RuntimeException failure) {
            this.failure.set(failure);
        }

        @Override
        public Mono<Void> dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
            return Mono.defer(() -> {
                RuntimeException toThrow = failure.getAndSet(null);
                return toThrow == null ? Mono.empty() : Mono.error(toThrow);
            });
        }

        @Override
        public boolean hasSubscriptions() {
            return true;
        }
    }
}
