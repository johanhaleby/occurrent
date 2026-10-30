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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.reactor.dcb.GenericDcbApplicationService;
import org.occurrent.application.service.spring.reactor.SpringReactiveTransactionExecutor;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.dsl.dcb.DcbDecider;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.snapshot.DcbSnapshotKeys;
import org.occurrent.dsl.snapshot.Snapshot;
import org.occurrent.dsl.snapshot.SnapshotOptions;
import org.occurrent.dsl.snapshot.SnapshotPolicy;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.synchronous.reactor.SynchronousSubscriptionModel;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * A reactive snapshot decider with a synchronous subscription, run inside a transaction the caller owns. The store
 * refuses to read after the saved marker versions there, so the decider reads the whole boundary instead.
 */
@Testcontainers
@DisplayName("ReactiveSnapshotDcbDeciderApplicationService in a caller's transaction over ReactorMongoEventStore")
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactiveSnapshotDcbDeciderInCallerTransactionTest {

    private static final String ACCOUNT = "acct";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private ReactiveMongoTransactionManager transactionManager;
    private CloudEventConverter<DomainEvent> converter;
    private ReactorMongoEventStore eventStore;
    private ReactiveSnapshotStore<List<String>> snapshots;
    private List<CloudEvent> seenBySubscription;
    private ReactiveSnapshotDcbDeciderApplicationService<DomainEvent> service;
    private ReactiveSnapshotDcbDecider<Add, List<String>, DomainEvent> decider;

    @BeforeEach
    void wire_a_snapshot_decider_with_a_synchronous_subscription() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reactivesnapshotdecidertx");
        String database = requireNonNull(connectionString.getDatabase());
        mongoClient = MongoClients.create(connectionString);
        SimpleReactiveMongoDatabaseFactory databaseFactory = new SimpleReactiveMongoDatabaseFactory(mongoClient, database);
        transactionManager = new ReactiveMongoTransactionManager(databaseFactory);
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(TransactionalOperator.create(transactionManager))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        eventStore = new ReactorMongoEventStore(new ReactiveMongoTemplate(databaseFactory), config);
        converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        SynchronousSubscriptionModel subscriptions = new SynchronousSubscriptionModel();
        seenBySubscription = new CopyOnWriteArrayList<>();
        subscriptions.subscribe("names", event -> {
            seenBySubscription.add(event);
            return Mono.empty();
        });
        snapshots = ReactiveSnapshotStore.inMemory();
        service = new ReactiveSnapshotDcbDeciderApplicationService<>(GenericDcbApplicationService.builder(eventStore, converter)
                .tagGenerator((DomainEvent event) -> Set.of(tag()))
                .synchronousSubscriptions(subscriptions)
                .transactionExecutor(new SpringReactiveTransactionExecutor(transactionManager))
                .build());
        decider = ReactiveSnapshotDcbDecider.from(DcbDecider.from(names(), command -> criteria(), event -> Set.of(tag())), snapshots, SnapshotOptions.of(1, SnapshotPolicy.always()));
    }

    @AfterEach
    void close_client() {
        mongoClient.close();
    }

    @Test
    void the_callers_transaction_commits_when_the_store_refuses_to_resume_from_the_snapshot() {
        saveASnapshotThatDisagreesWithTheStore();

        Object[] state = new Object[1];
        Throwable thrown = catchThrowable(() -> TransactionalOperator.create(transactionManager)
                .execute(status -> service.executeAndReturnState(new Add("second"), decider).doOnNext(names -> state[0] = names))
                .single().block(TIMEOUT));

        assertAll(
                () -> assertThat(thrown).as("what the caller's commit threw").isNull(),
                () -> assertThat(state[0]).as("the state, read in full since the store refused the snapshot").asList().containsExactlyInAnyOrder("seed", "second"),
                () -> assertThat(eventStore.read(criteria()).block(TIMEOUT).events()).as("events stored after the commit").hasSize(2),
                () -> assertThat(seenBySubscription).as("events the subscription saw").hasSize(1)
        );
    }

    @Test
    void a_caller_that_rolls_back_stores_nothing_when_the_store_refuses_to_resume_from_the_snapshot() {
        saveASnapshotThatDisagreesWithTheStore();

        Throwable thrown = catchThrowable(() -> TransactionalOperator.create(transactionManager).execute(status -> {
            status.setRollbackOnly();
            return service.execute(new Add("second"), decider);
        }).collectList().block(TIMEOUT));

        assertAll(
                () -> assertThat(thrown).as("what the caller's rollback threw").isNull(),
                () -> assertThat(eventStore.read(criteria()).block(TIMEOUT).events()).as("events stored after the rollback").hasSize(1)
        );
    }

    @Test
    void a_transaction_the_executor_owns_saves_no_snapshot_since_the_store_reports_no_marker_versions_inside_it() {
        ListAppender<ILoggingEvent> logged = new ListAppender<>();
        ch.qos.logback.classic.Logger serviceLog = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ReactiveSnapshotDcbDeciderApplicationService.class);
        logged.start();
        serviceLog.addAppender(logged);
        try {
            service.execute(new Add("first"), decider).block(TIMEOUT);
            service.execute(new Add("second"), decider).block(TIMEOUT);
        } finally {
            serviceLog.detachAppender(logged);
        }

        assertAll(
                () -> assertThat(eventStore.read(criteria()).block(TIMEOUT).events()).as("events stored").hasSize(2),
                () -> assertThat(seenBySubscription).as("events the subscription saw").hasSize(2),
                () -> assertThat(snapshots.findLatest(DcbSnapshotKeys.canonicalKey(criteria())).block(TIMEOUT)).as("the snapshot saved").isNull(),
                () -> assertThat(logged.list.stream().filter(event -> event.getLevel() == Level.WARN && event.getFormattedMessage().contains("reported no marker versions")).count())
                        .as("warnings about the missing marker versions").isEqualTo(1)
        );
    }

    /**
     * The snapshot holds a state the store doesn't, so a read that resumed from it would show up in the state.
     */
    private void saveASnapshotThatDisagreesWithTheStore() {
        eventStore.append(new ArrayList<>(converter.toCloudEvents(List.of(nameWasChanged("seed"))).stream().map(ce -> DcbCloudEvents.withTags(ce, List.of(tag()))).toList())).block(TIMEOUT);
        DcbMarkerVersions versions = eventStore.read(criteria(), DcbReadOptions.fromBeginning().withVersions()).block(TIMEOUT).markerVersions().orElseThrow();
        snapshots.save(DcbSnapshotKeys.canonicalKey(criteria()), Snapshot.atMarkerVersions(List.of("snapshot-only"), versions, 1)).block(TIMEOUT);
    }

    private static NameWasChanged nameWasChanged(String name) {
        return new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", name);
    }

    private static DcbCriteria criteria() {
        return DcbCriteria.tags(tag());
    }

    private static Tag tag() {
        return Tag.of("name", ACCOUNT);
    }

    private record Add(String name) {
    }

    private static Decider<Add, List<String>, DomainEvent> names() {
        return new Decider<>() {
            @Override
            public List<String> initialState() {
                return List.of();
            }

            @NonNull
            @Override
            public List<DomainEvent> decide(@NonNull Add command, List<String> state) {
                return List.of(nameWasChanged(command.name()));
            }

            @Override
            public List<String> evolve(List<String> state, @NonNull DomainEvent event) {
                List<String> next = new ArrayList<>(state);
                next.add(event.name());
                return next;
            }
        };
    }
}
