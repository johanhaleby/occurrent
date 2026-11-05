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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.reactor.ReactiveTransactionExecutor;
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
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
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
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * A reactive snapshot decider with a synchronous subscription, run inside a transaction the caller owns. The store
 * refuses to read after the saved marker versions there, so the decider reads the whole boundary instead. It also runs in
 * a caller's scope that has transaction synchronization but no transaction.
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
    private SynchronousSubscriptionModel subscriptions;
    private List<CloudEvent> seenBySubscription;
    private GenericDcbApplicationService<DomainEvent> applicationService;
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
        subscriptions = new SynchronousSubscriptionModel();
        seenBySubscription = new CopyOnWriteArrayList<>();
        subscriptions.subscribe("names", event -> {
            seenBySubscription.add(event);
            return Mono.empty();
        });
        snapshots = ReactiveSnapshotStore.inMemory();
        applicationService = applicationServiceWith(new SpringReactiveTransactionExecutor(transactionManager));
        service = new ReactiveSnapshotDcbDeciderApplicationService<>(applicationService);
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
    void a_transaction_the_executor_owns_saves_a_snapshot_at_marker_versions_and_the_next_execute_resumes_from_it() {
        appendSeed();
        service.execute(new Add("first"), decider).block(TIMEOUT);

        assertThat(snapshots.findLatest(DcbSnapshotKeys.canonicalKey(criteria())).block(TIMEOUT)).as("the snapshot saved by the first execute").isNotNull();
        Snapshot<List<String>> saved = snapshots.findLatest(DcbSnapshotKeys.canonicalKey(criteria())).block(TIMEOUT);
        snapshots.save(DcbSnapshotKeys.canonicalKey(criteria()), Snapshot.atMarkerVersions(List.of("snapshot-only"), saved.markerVersions().orElseThrow(), saved.highestFoldedPosition(), 1)).block(TIMEOUT);

        List<String> state = service.executeAndReturnState(new Add("second"), decider).block(TIMEOUT);

        assertAll(
                () -> assertThat(state).as("the state, resumed from the snapshot so only the tail was read").containsExactly("snapshot-only", "second"),
                () -> assertThat(eventStore.read(criteria()).block(TIMEOUT).events()).as("events stored").hasSize(3),
                () -> assertThat(seenBySubscription).as("events the subscription saw").hasSize(2),
                () -> assertThat(snapshots.findLatest(DcbSnapshotKeys.canonicalKey(criteria())).block(TIMEOUT).state()).as("the snapshot saved by the second execute").containsExactly("snapshot-only", "second")
        );
    }

    @Test
    void a_callers_own_transaction_still_saves_no_snapshot_since_the_store_reports_no_marker_versions_inside_it() {
        appendSeed();

        TransactionalOperator.create(transactionManager).execute(status -> service.execute(new Add("first"), decider)).collectList().block(TIMEOUT);

        assertAll(
                () -> assertThat(eventStore.read(criteria()).block(TIMEOUT).events()).as("events stored").hasSize(2),
                () -> assertThat(seenBySubscription).as("events the subscription saw").hasSize(1),
                () -> assertThat(snapshots.findLatest(DcbSnapshotKeys.canonicalKey(criteria())).block(TIMEOUT)).as("the snapshot saved").isNull()
        );
    }

    @ParameterizedTest(name = "propagation behavior {0}")
    @ValueSource(ints = {TransactionDefinition.PROPAGATION_REQUIRED, TransactionDefinition.PROPAGATION_REQUIRES_NEW, TransactionDefinition.PROPAGATION_NOT_SUPPORTED})
    void a_second_command_in_the_callers_transaction_sees_the_first_ones_event_whatever_the_executor_does_with_that_transaction(int propagationBehavior) {
        GenericDcbApplicationService<DomainEvent> service = applicationServiceOnPropagation(propagationBehavior);
        List<Integer> eventsSeen = new CopyOnWriteArrayList<>();

        TransactionalOperator.create(transactionManager).execute(status ->
                service.execute(criteria(), appendingAndRecordingWhatItSees("first", eventsSeen))
                        .then(service.execute(criteria(), appendingAndRecordingWhatItSees("second", eventsSeen)))
        ).collectList().block(TIMEOUT);

        assertAll(
                () -> assertThat(eventsSeen).as("events each decision saw").containsExactly(0, 1),
                () -> assertThat(eventStore.read(criteria()).block(TIMEOUT).events()).as("events stored after the commit").hasSize(2)
        );
    }

    @ParameterizedTest(name = "propagation behavior {0}")
    @ValueSource(ints = {TransactionDefinition.PROPAGATION_REQUIRES_NEW, TransactionDefinition.PROPAGATION_NOT_SUPPORTED})
    void a_command_in_the_callers_transaction_sees_what_another_writer_committed_when_the_executor_leaves_that_transaction(int propagationBehavior) {
        GenericDcbApplicationService<DomainEvent> service = applicationServiceOnPropagation(propagationBehavior);
        List<Integer> eventsSeen = new CopyOnWriteArrayList<>();

        // A read of something else starts the caller's snapshot, and the other writer commits after it. A future drops the
        // subscriber's context, so its append isn't part of the caller's transaction.
        TransactionalOperator.create(transactionManager).execute(status ->
                eventStore.read(DcbCriteria.tags(Tag.of("name", "unrelated")))
                        .then(Mono.fromFuture(() -> appendingSeed().toFuture()))
                        .then(service.execute(criteria(), appendingAndRecordingWhatItSees("mine", eventsSeen)))
        ).collectList().block(TIMEOUT);

        assertAll(
                () -> assertThat(eventsSeen).as("events the decision saw").containsExactly(1),
                () -> assertThat(eventStore.read(criteria()).block(TIMEOUT).events()).as("events stored after the commit").hasSize(2)
        );
    }

    @ParameterizedTest(name = "propagation behavior {0}")
    @ValueSource(ints = {TransactionDefinition.PROPAGATION_REQUIRES_NEW, TransactionDefinition.PROPAGATION_NOT_SUPPORTED})
    void a_snapshot_deciders_second_command_folds_the_first_ones_event_when_the_executor_leaves_the_callers_transaction(int propagationBehavior) {
        ReactiveSnapshotDcbDeciderApplicationService<DomainEvent> service = new ReactiveSnapshotDcbDeciderApplicationService<>(applicationServiceOnPropagation(propagationBehavior));

        List<String> state = TransactionalOperator.create(transactionManager).execute(status ->
                service.execute(new Add("first"), decider).then(service.executeAndReturnState(new Add("second"), decider))
        ).single().block(TIMEOUT);

        assertThat(state).as("the state after the second command").containsExactly("first", "second");
    }

    @ParameterizedTest(name = "caller {0}, executor {1}")
    @MethodSource("callerScopesWithoutATransactionAndExecutorPropagations")
    void a_snapshot_deciders_command_in_a_callers_scope_without_a_transaction_stores_its_event_whatever_the_executor_does_with_that_scope(String callerScope, String executorPropagation) {
        appendSeed();
        TransactionalOperator caller = TransactionalOperator.create(transactionManager, new DefaultTransactionDefinition(propagation(callerScope)));
        ReactiveSnapshotDcbDeciderApplicationService<DomainEvent> service = new ReactiveSnapshotDcbDeciderApplicationService<>(applicationServiceOnPropagation(propagation(executorPropagation)));

        Throwable thrown = catchThrowable(() -> caller.execute(status -> service.execute(new Add("first"), decider)).collectList().block(TIMEOUT));

        assertAll(
                () -> assertThat(thrown).as("what execute threw").isNull(),
                () -> assertThat(eventStore.read(criteria()).block(TIMEOUT).events()).as("events stored").hasSize(2),
                () -> assertThat(seenBySubscription).as("events the subscription saw").hasSize(1)
        );
    }

    // A plain execute reads with no versions through the caller's synchronization, unlike the decider above, so its read
    // can bind a session the executor then fails on
    @ParameterizedTest(name = "caller {0}, executor {1}")
    @MethodSource("callerScopesWithoutATransactionAndExecutorPropagations")
    void an_execute_with_no_snapshot_in_a_callers_scope_without_a_transaction_stores_its_event_whatever_the_executor_does_with_that_scope(String callerScope, String executorPropagation) {
        appendSeed();
        TransactionalOperator caller = TransactionalOperator.create(transactionManager, new DefaultTransactionDefinition(propagation(callerScope)));
        GenericDcbApplicationService<DomainEvent> service = applicationServiceOnPropagation(propagation(executorPropagation));

        Throwable thrown = catchThrowable(() -> caller.execute(status -> service.execute(criteria(), events -> List.of(nameWasChanged("first")))).collectList().block(TIMEOUT));

        assertAll(
                () -> assertThat(thrown).as("what execute threw").isNull(),
                () -> assertThat(eventStore.read(criteria()).block(TIMEOUT).events()).as("events stored").hasSize(2),
                () -> assertThat(seenBySubscription).as("events the subscription saw").hasSize(1)
        );
    }

    private static Stream<Arguments> callerScopesWithoutATransactionAndExecutorPropagations() {
        return Stream.of("PROPAGATION_NOT_SUPPORTED", "PROPAGATION_SUPPORTS").flatMap(callerScope ->
                Stream.of("PROPAGATION_NOT_SUPPORTED", "PROPAGATION_SUPPORTS", "PROPAGATION_NEVER").map(executorPropagation -> Arguments.of(callerScope, executorPropagation)));
    }

    private static int propagation(String name) {
        return switch (name) {
            case "PROPAGATION_NOT_SUPPORTED" -> TransactionDefinition.PROPAGATION_NOT_SUPPORTED;
            case "PROPAGATION_SUPPORTS" -> TransactionDefinition.PROPAGATION_SUPPORTS;
            case "PROPAGATION_NEVER" -> TransactionDefinition.PROPAGATION_NEVER;
            default -> throw new IllegalArgumentException(name);
        };
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("failuresFromTheDomainFunction")
    void the_callers_transaction_commits_when_it_handles_a_failure_from_the_domain_function(Throwable failure) {
        AtomicReference<Throwable> handled = new AtomicReference<>();

        Throwable thrown = catchThrowable(() -> TransactionalOperator.create(transactionManager).execute(status ->
                applicationService.execute(criteria(), events -> rethrow(failure))
                        .onErrorResume(error -> {
                            handled.set(error);
                            return Mono.empty();
                        })
                        .then(appendingSeed())
        ).collectList().block(TIMEOUT));

        assertAll(
                () -> assertThat(handled.get()).as("what the caller handled").isSameAs(failure),
                () -> assertThat(thrown).as("what the caller's commit threw").isNull(),
                () -> assertThat(eventStore.read(criteria()).block(TIMEOUT).events()).as("events stored after the commit").hasSize(1)
        );
    }

    private static Stream<Throwable> failuresFromTheDomainFunction() {
        return Stream.of(new IllegalStateException("from the domain function"), new AssertionError("from the domain function"));
    }

    private static List<DomainEvent> rethrow(Throwable failure) {
        if (failure instanceof Error error) {
            throw error;
        }
        throw (RuntimeException) failure;
    }

    private Function<List<DomainEvent>, List<DomainEvent>> appendingAndRecordingWhatItSees(String name, List<Integer> eventsSeen) {
        return events -> {
            eventsSeen.add(events.size());
            return List.of(nameWasChanged(name));
        };
    }

    private GenericDcbApplicationService<DomainEvent> applicationServiceOnPropagation(int propagationBehavior) {
        return applicationServiceWith(new SpringReactiveTransactionExecutor(TransactionalOperator.create(transactionManager, new DefaultTransactionDefinition(propagationBehavior))));
    }

    private GenericDcbApplicationService<DomainEvent> applicationServiceWith(ReactiveTransactionExecutor transactionExecutor) {
        return GenericDcbApplicationService.builder(eventStore, converter)
                .tagGenerator((DomainEvent event) -> Set.of(tag()))
                .synchronousSubscriptions(subscriptions)
                .transactionExecutor(transactionExecutor)
                .build();
    }

    /**
     * The snapshot holds a state the store doesn't, so a read that resumed from it would show up in the state.
     */
    private void saveASnapshotThatDisagreesWithTheStore() {
        long position = appendSeed();
        DcbMarkerVersions versions = eventStore.read(criteria(), DcbReadOptions.fromBeginning().withVersions()).block(TIMEOUT).markerVersions().orElseThrow();
        snapshots.save(DcbSnapshotKeys.canonicalKey(criteria()), Snapshot.atMarkerVersions(List.of("snapshot-only"), versions, position, 1)).block(TIMEOUT);
    }

    private long appendSeed() {
        return appendingSeed().block(TIMEOUT).lastSequencePosition();
    }

    private Mono<DcbAppendResult> appendingSeed() {
        return eventStore.append(new ArrayList<>(converter.toCloudEvents(List.of(nameWasChanged("seed"))).stream().map(ce -> DcbCloudEvents.withTags(ce, List.of(tag()))).toList()));
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
