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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.reactor.ReactiveSynchronousEventDispatcher;
import org.occurrent.application.service.reactor.ReactiveTransactionExecutor;
import org.occurrent.application.service.reactor.dcb.DcbApplicationService;
import org.occurrent.application.service.reactor.dcb.DcbExecuteOptions;
import org.occurrent.application.service.reactor.dcb.GenericDcbApplicationService;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.dsl.dcb.DcbDecider;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.snapshot.DcbSnapshotKeys;
import org.occurrent.dsl.snapshot.SnapshotOptions;
import org.occurrent.dsl.snapshot.SnapshotPolicy;
import org.occurrent.dsl.snapshot.Snapshot;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.DcbSnapshotInvalidException;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.reactor.DcbAppendConfirmation;
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
import org.slf4j.LoggerFactory;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

@Testcontainers
@DisplayName("ReactiveSnapshotDcbDeciderApplicationService")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ReactiveSnapshotDcbDeciderApplicationServiceTest {

    private static final String ACCOUNT = "acct";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private ReactorMongoEventStore eventStore;
    private CloudEventConverter<DomainEvent> converter;
    private DcbApplicationService<DomainEvent> applicationService;
    private ReactiveSnapshotDcbDeciderApplicationService<DomainEvent> service;
    private ReactiveSnapshotStore<String> store;
    private AtomicInteger evolveCount;
    private DcbDecider<Cmd, String, DomainEvent> dcbDecider;
    private String key;
    private LocalDateTime time;

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private ListAppender<ILoggingEvent> logAppender;
    private ch.qos.logback.classic.Logger log;
    private Level originalLevel;

    @BeforeEach
    void attach_log_appender() {
        log = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ReactiveSnapshotDcbDeciderApplicationService.class);
        originalLevel = log.getLevel();
        log.setLevel(Level.DEBUG);
        logAppender = new ListAppender<>();
        logAppender.start();
        log.addAppender(logAppender);
    }

    @AfterEach
    void detach_log_appender() {
        log.detachAppender(logAppender);
        log.setLevel(originalLevel);
    }

    @BeforeEach
    void create_instances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reactivesnapshotdcb");
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
        applicationService = new GenericDcbApplicationService<>(eventStore, converter, (DomainEvent event) -> Set.of(tag()), GenericDcbApplicationService.defaultRetry());
        service = new ReactiveSnapshotDcbDeciderApplicationService<>(applicationService);
        store = ReactiveSnapshotStore.inMemory();
        evolveCount = new AtomicInteger();
        Decider<Cmd, String, DomainEvent> decider = countingDecider(evolveCount, time = LocalDateTime.now());
        dcbDecider = DcbDecider.from(decider, command -> criteria(), event -> Set.of(tag()));
        key = DcbSnapshotKeys.canonicalKey(criteria());
    }

    @Test
    void first_execute_on_an_empty_boundary_appends_but_saves_no_snapshot() {
        // The read of an empty boundary has no marker to compare the append's epoch with
        DcbAppendResult result = service.execute(new Define("A"), ReactiveSnapshotDcbDecider.from(dcbDecider, store, SnapshotOptions.of(1, SnapshotPolicy.always()))).block();

        assertAll(
                () -> assertThat(result).isNotNull(),
                () -> assertThat(store.findLatest(key).blockOptional()).isEmpty()
        );
    }

    @Test
    void execute_on_a_non_empty_boundary_saves_a_snapshot_keyed_by_criteria_at_marker_versions() {
        seed();

        service.execute(new Define("A"), ReactiveSnapshotDcbDecider.from(dcbDecider, store, SnapshotOptions.of(1, SnapshotPolicy.always()))).block();

        Snapshot<String> snapshot = store.findLatest(key).blockOptional().orElseThrow();
        assertAll(
                () -> assertThat(snapshot.state()).isEqualTo("A"),
                () -> assertThat(snapshot.markerVersions()).isEqualTo(requireNonNull(eventStore.read(criteria(), DcbReadOptions.fromBeginning().withVersions()).block(TIMEOUT)).markerVersions()),
                () -> assertThat(snapshot.version()).isZero()
        );
    }

    @Test
    void execute_with_the_default_key_function_resolves_the_canonical_criteria_key() {
        // The 3-arg from(...) defaults keyFunction to DcbSnapshotKeys::canonicalKey; observe the same key as an explicit one.
        var account = ReactiveSnapshotDcbDecider.from(dcbDecider, store, SnapshotOptions.of(1, SnapshotPolicy.always()));
        seed();

        service.execute(new Define("A"), account).block();

        assertThat(store.findLatest(DcbSnapshotKeys.canonicalKey(criteria())).blockOptional()).hasValueSatisfying(s -> assertThat(s.state()).isEqualTo("A"));
    }

    @Test
    void a_snapshot_saved_at_a_position_by_0_34_is_not_used_and_is_replaced_by_one_with_marker_versions() {
        seed();
        store.save(key, new Snapshot<>("stale", 1000, 1)).block();

        evolveCount.set(0);
        service.execute(new Change("B"), ReactiveSnapshotDcbDecider.from(dcbDecider, store, SnapshotOptions.of(1, SnapshotPolicy.always()))).block();

        Snapshot<String> replaced = store.findLatest(key).blockOptional().orElseThrow();
        assertAll(
                // The seed (1) and the produced B (1) are folded, so the boundary was read in full
                () -> assertThat(evolveCount.get()).isEqualTo(2),
                () -> assertThat(replaced.state()).isEqualTo("B"),
                () -> assertThat(replaced.markerVersions()).isPresent(),
                () -> assertThat(replaced.version()).isZero()
        );
    }

    @Test
    void a_store_that_refuses_to_read_after_the_saved_marker_versions_is_read_in_full_and_the_snapshot_replaced() {
        ControlledStore controlled = new ControlledStore(eventStore);
        var refusingService = new ReactiveSnapshotDcbDeciderApplicationService<>(new GenericDcbApplicationService<>(controlled, converter, (DomainEvent event) -> Set.of(tag()), GenericDcbApplicationService.defaultRetry()));
        var account = ReactiveSnapshotDcbDecider.from(dcbDecider, store, SnapshotOptions.of(1, SnapshotPolicy.always()));
        seed();
        refusingService.execute(new Define("A"), account).block(TIMEOUT);
        DcbMarkerVersions savedVersions = store.findLatest(key).blockOptional().orElseThrow().markerVersions().orElseThrow();
        controlled.refuseReadsAfterVersions = true;
        controlled.reads.clear();

        evolveCount.set(0);
        refusingService.execute(new Change("B"), account).block(TIMEOUT);

        Snapshot<String> replaced = store.findLatest(key).blockOptional().orElseThrow();
        assertAll(
                () -> assertThat(controlled.reads).containsExactly(DcbReadOptions.afterVersions(savedVersions), DcbReadOptions.fromBeginning().withVersions()),
                // The seed (1), A (1) and the produced B (1), so the state was folded from the initial state
                () -> assertThat(evolveCount.get()).isEqualTo(3),
                () -> assertThat(replaced.state()).isEqualTo("B"),
                () -> assertThat(replaced.markerVersions()).isPresent().isNotEqualTo(Optional.of(savedVersions))
        );
    }

    @Test
    void a_conflict_followed_by_a_refused_resume_read_reads_in_full_and_decides_on_that_state() {
        ControlledStore controlled = new ControlledStore(eventStore);
        controlled.refuseReadsAfterVersions = true;

        assertConflictThenRefusedResumeReadIsReadInFull(controlled);
    }

    @Test
    void a_conflict_followed_by_a_resume_read_with_an_event_below_the_saved_position_reads_in_full_and_decides_on_that_state() {
        ControlledStore controlled = new ControlledStore(eventStore);
        controlled.refuseReadsAfterVersions = true;
        controlled.answerRefusedReadsWithEventsBelowTheSavedPosition = true;

        assertConflictThenRefusedResumeReadIsReadInFull(controlled);
    }

    @Test
    void a_conflict_followed_by_a_refused_resume_read_reads_in_full_and_decides_on_that_state_when_a_wrapper_passes_other_options_for_the_same_function() {
        ControlledStore controlled = new ControlledStore(eventStore);
        controlled.refuseReadsAfterVersions = true;

        assertConflictThenRefusedResumeReadIsReadInFull(controlled, wrapped -> new RewritingService(wrapped, options -> options.sideEffect(events -> Mono.empty()), function -> function));
    }

    @Test
    void a_conflict_followed_by_a_refused_resume_read_reaches_the_caller_and_the_command_is_not_run_a_second_time_when_a_wrapper_passes_another_function_that_calls_the_same_function() {
        // The refusal is marked for the wrapper's function, which the decider can't tell from another service's, so it fails safe
        ControlledStore controlled = new ControlledStore(eventStore);
        controlled.refuseReadsAfterVersions = true;
        var wrapping = new RewritingService(new GenericDcbApplicationService<>(controlled, converter, (DomainEvent event) -> Set.of(tag()), GenericDcbApplicationService.defaultRetry()), options -> options, function -> tail -> function.apply(tail));
        var wrappingService = new ReactiveSnapshotDcbDeciderApplicationService<>(wrapping);
        AtomicInteger decideCount = new AtomicInteger();
        var account = ReactiveSnapshotDcbDecider.from(DcbDecider.from(decideCounting(countingDecider(evolveCount, time), decideCount), command -> criteria(), event -> Set.of(tag())), store, SnapshotOptions.of(1, SnapshotPolicy.always()));
        seed();
        wrappingService.execute(new Define("A"), account).block(TIMEOUT);
        decideCount.set(0);
        wrapping.given.clear();
        wrapping.passedOn.clear();
        // The first resume read succeeds, then the append loses to this event, and the retry's resume read is refused
        controlled.afterVersionsReadsAllowed = 1;
        controlled.beforeNextAppend = appendOutOfBandLater(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "X")).then();

        Throwable thrown = catchThrowable(() -> wrappingService.execute(new Change("B"), account).block(TIMEOUT));

        assertAll(
                () -> assertThat(thrown).as("what reached the caller").isInstanceOfSatisfying(DcbSnapshotInvalidException.class, refused -> assertAll(
                        () -> assertThat(wrapping.passedOn).as("functions the wrapper passed on").hasSize(1).allMatch(refused::isBeforeAppendOf),
                        () -> assertThat(wrapping.given).as("functions given to the wrapper by the execute").hasSize(1).noneMatch(refused::isBeforeAppendOf))),
                () -> assertThat(decideCount.get()).as("times the command was decided").isEqualTo(1),
                // The seed, A and X, with no B since the append lost to X
                () -> assertThat(eventStore.count(criteria()).block(TIMEOUT)).as("events in the boundary").isEqualTo(3L),
                () -> assertThat(store.findLatest(key).blockOptional().orElseThrow().state()).isEqualTo("A")
        );
    }

    private void assertConflictThenRefusedResumeReadIsReadInFull(ControlledStore controlled) {
        assertConflictThenRefusedResumeReadIsReadInFull(controlled, wrapped -> wrapped);
    }

    private void assertConflictThenRefusedResumeReadIsReadInFull(ControlledStore controlled, UnaryOperator<DcbApplicationService<DomainEvent>> wrapper) {
        List<String> decidedOnStates = new CopyOnWriteArrayList<>();
        var conflictingService = new ReactiveSnapshotDcbDeciderApplicationService<>(wrapper.apply(new GenericDcbApplicationService<>(controlled, converter, (DomainEvent event) -> Set.of(tag()), GenericDcbApplicationService.defaultRetry())));
        var account = ReactiveSnapshotDcbDecider.from(DcbDecider.from(decideRecordingStates(countingDecider(evolveCount, time), decidedOnStates), command -> criteria(), event -> Set.of(tag())), store, SnapshotOptions.of(1, SnapshotPolicy.always()));
        seed();
        conflictingService.execute(new Define("A"), account).block(TIMEOUT);
        DcbMarkerVersions savedVersions = store.findLatest(key).blockOptional().orElseThrow().markerVersions().orElseThrow();
        decidedOnStates.clear();
        controlled.reads.clear();
        // The first resume read succeeds, then the append loses to this event, and the retry's resume read is refused
        controlled.afterVersionsReadsAllowed = 1;
        controlled.beforeNextAppend = appendOutOfBandLater(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "X")).then();

        Throwable thrown = catchThrowable(() -> conflictingService.execute(new Change("B"), account).block(TIMEOUT));

        Snapshot<String> replaced = store.findLatest(key).blockOptional().orElseThrow();
        assertAll(
                () -> assertThat(thrown).as("what escaped execute after the conflicted append").isNull(),
                () -> assertThat(controlled.reads).containsExactly(DcbReadOptions.afterVersions(savedVersions), DcbReadOptions.afterVersions(savedVersions), DcbReadOptions.fromBeginning().withVersions()),
                // The first attempt decides on the snapshot's state and the full read on the seed, A and X
                () -> assertThat(decidedOnStates).containsExactly("A", "X"),
                // The seed, A, X and the one B that was appended
                () -> assertThat(eventStore.count(criteria()).block(TIMEOUT)).as("events in the boundary").isEqualTo(4L),
                () -> assertThat(replaced.state()).isEqualTo("B"),
                () -> assertThat(replaced.markerVersions()).isPresent().isNotEqualTo(Optional.of(savedVersions))
        );
    }

    @Test
    void a_refusal_to_read_without_a_saved_snapshot_is_not_swallowed() {
        ControlledStore controlled = new ControlledStore(eventStore);
        controlled.refuseEveryRead = true;
        var refusingService = new ReactiveSnapshotDcbDeciderApplicationService<>(new GenericDcbApplicationService<>(controlled, converter, (DomainEvent event) -> Set.of(tag()), GenericDcbApplicationService.defaultRetry()));

        Throwable thrown = catchThrowable(() -> refusingService.execute(new Define("A"), ReactiveSnapshotDcbDecider.from(dcbDecider, store, SnapshotOptions.of(1, SnapshotPolicy.always()))).block(TIMEOUT));

        assertThat(thrown).isInstanceOf(DcbSnapshotInvalidException.class);
    }

    @Test
    void a_refusal_after_the_append_reaches_the_caller_and_the_command_is_not_run_a_second_time() {
        // A synchronous subscription without a transaction fails after the append has committed
        AtomicBoolean refuseAfterAppend = new AtomicBoolean();
        DcbSnapshotInvalidException refusal = new DcbSnapshotInvalidException(criteria(), DcbMarkerVersions.of(Map.of("tag:name:" + ACCOUNT, new DcbMarkerVersions.MarkerVersion("epoch", 1, 0))), "refused by a synchronous subscription (test double)");
        ReactiveSynchronousEventDispatcher refusingDispatcher = new ReactiveSynchronousEventDispatcher() {
            @Override
            public Mono<Void> dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
                return refuseAfterAppend.get() ? Mono.error(refusal) : Mono.empty();
            }

            @Override
            public boolean hasSubscriptions() {
                return true;
            }
        };
        var dispatchingService = new ReactiveSnapshotDcbDeciderApplicationService<>(GenericDcbApplicationService.builder(eventStore, converter)
                .tagGenerator((DomainEvent event) -> Set.of(tag()))
                .synchronousSubscriptions(refusingDispatcher)
                .transactionExecutor(ReactiveTransactionExecutor.noTransaction())
                .build());
        AtomicInteger decideCount = new AtomicInteger();
        var account = ReactiveSnapshotDcbDecider.from(DcbDecider.from(decideCounting(countingDecider(evolveCount, time), decideCount), command -> criteria(), event -> Set.of(tag())), store, SnapshotOptions.of(1, SnapshotPolicy.always()));
        seed();
        dispatchingService.execute(new Define("A"), account).block(TIMEOUT);
        decideCount.set(0);
        refuseAfterAppend.set(true);

        Throwable thrown = catchThrowable(() -> dispatchingService.execute(new Change("B"), account).block(TIMEOUT));

        assertAll(
                () -> assertThat(thrown).isSameAs(refusal),
                () -> assertThat(decideCount.get()).as("times the command was decided").isEqualTo(1),
                // The seed, A and the one B that committed before the refusal
                () -> assertThat(requireNonNull(eventStore.read(criteria()).block(TIMEOUT)).events()).as("events in the boundary").hasSize(3),
                // The refusal escapes before the snapshot is saved, so the one from A is left in place
                () -> assertThat(store.findLatest(key).blockOptional().orElseThrow().state()).isEqualTo("A")
        );
    }

    @Test
    void a_refusal_marked_by_another_service_in_a_subscription_reaches_the_caller_and_the_command_is_not_run_a_second_time() {
        // The subscription calls a second service whose read is refused, so the refusal it lets through is marked for that service's function
        ControlledStore refusingStore = new ControlledStore(eventStore);
        refusingStore.refuseEveryRead = true;
        var otherService = new GenericDcbApplicationService<>(refusingStore, converter, (DomainEvent event) -> Set.of(tag()), GenericDcbApplicationService.defaultRetry());
        AtomicBoolean callOtherService = new AtomicBoolean();
        Function<List<DomainEvent>, List<DomainEvent>> otherServicesFunction = events -> List.of();
        ReactiveSynchronousEventDispatcher dispatcherCallingOtherService = new ReactiveSynchronousEventDispatcher() {
            @Override
            public Mono<Void> dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
                return callOtherService.get() ? otherService.execute(criteria(), otherServicesFunction).then() : Mono.empty();
            }

            @Override
            public boolean hasSubscriptions() {
                return true;
            }
        };
        var recordingService = new FunctionRecordingService(GenericDcbApplicationService.builder(eventStore, converter)
                .tagGenerator((DomainEvent event) -> Set.of(tag()))
                .synchronousSubscriptions(dispatcherCallingOtherService)
                .transactionExecutor(ReactiveTransactionExecutor.noTransaction())
                .build());
        var dispatchingService = new ReactiveSnapshotDcbDeciderApplicationService<>(recordingService);
        AtomicInteger decideCount = new AtomicInteger();
        var account = ReactiveSnapshotDcbDecider.from(DcbDecider.from(decideCounting(countingDecider(evolveCount, time), decideCount), command -> criteria(), event -> Set.of(tag())), store, SnapshotOptions.of(1, SnapshotPolicy.always()));
        seed();
        dispatchingService.execute(new Define("A"), account).block(TIMEOUT);
        decideCount.set(0);
        callOtherService.set(true);
        recordingService.given.clear();

        Throwable thrown = catchThrowable(() -> dispatchingService.execute(new Change("B"), account).block(TIMEOUT));

        assertAll(
                () -> assertThat(thrown).isInstanceOf(DcbSnapshotInvalidException.class),
                () -> assertThat(thrown).as("what reached the caller").isInstanceOfSatisfying(DcbSnapshotInvalidException.class, refused -> assertAll(
                        () -> assertThat(recordingService.given).as("functions given to the service by the execute").hasSize(1).noneMatch(refused::isBeforeAppendOf),
                        () -> assertThat(refused.isBeforeAppendOf(otherServicesFunction)).as("marked for the other service's function").isTrue())),
                () -> assertThat(decideCount.get()).as("times the command was decided").isEqualTo(1),
                // The seed, A and the one B that committed before the other service's refusal
                () -> assertThat(requireNonNull(eventStore.read(criteria()).block(TIMEOUT)).events()).as("events in the boundary").hasSize(3),
                // The refusal escapes before the snapshot is saved, so the one from A is left in place
                () -> assertThat(store.findLatest(key).blockOptional().orElseThrow().state()).isEqualTo("A")
        );
    }

    @Test
    void a_refusal_marked_by_another_service_in_a_side_effect_reaches_the_caller_and_the_command_is_not_run_a_second_time() {
        // The side-effect calls a second service whose read is refused, so the refusal it lets through is marked for that service's function
        ControlledStore refusingStore = new ControlledStore(eventStore);
        refusingStore.refuseEveryRead = true;
        var otherService = new GenericDcbApplicationService<>(refusingStore, converter, (DomainEvent event) -> Set.of(tag()), GenericDcbApplicationService.defaultRetry());
        Function<List<DomainEvent>, List<DomainEvent>> otherServicesFunction = events -> List.of();

        assertSideEffectRefusalReachesTheCallerWithoutRunningTheCommandAgain(otherServicesFunction, events -> otherService.execute(criteria(), otherServicesFunction).then());
    }

    @Test
    void a_refusal_marked_by_another_service_thrown_synchronously_from_a_side_effect_reaches_the_caller_and_the_command_is_not_run_a_second_time() {
        // The function throws instead of returning an erroring Mono, which the service has to defer to see
        Function<List<DomainEvent>, List<DomainEvent>> otherServicesFunction = events -> List.of();
        DcbSnapshotInvalidException refusal = new DcbSnapshotInvalidException(criteria(), DcbMarkerVersions.of(Map.of("tag:name:" + ACCOUNT, new DcbMarkerVersions.MarkerVersion("epoch", 1, 0))), "refused by another service (test double)").beforeAppend(otherServicesFunction);

        assertSideEffectRefusalReachesTheCallerWithoutRunningTheCommandAgain(otherServicesFunction, events -> {
            throw refusal;
        });
    }

    private void assertSideEffectRefusalReachesTheCallerWithoutRunningTheCommandAgain(Function<List<DomainEvent>, List<DomainEvent>> otherServicesFunction, Function<List<DomainEvent>, Mono<Void>> sideEffect) {
        AtomicBoolean addSideEffect = new AtomicBoolean();
        List<Function<List<DomainEvent>, List<DomainEvent>>> given = new CopyOnWriteArrayList<>();
        // The decider sets no side-effect, so a decorator adds one before it delegates
        DcbApplicationService<DomainEvent> addingSideEffect = new DcbApplicationService<>() {
            @Override
            public Mono<DcbAppendResult> execute(DcbCriteria criteria, DcbExecuteOptions<DomainEvent> options, Function<List<DomainEvent>, List<DomainEvent>> functionThatCallsDomainModel) {
                given.add(functionThatCallsDomainModel);
                return applicationService.execute(criteria, addSideEffect.get() ? options.sideEffect(sideEffect) : options, functionThatCallsDomainModel);
            }

            @Override
            public Mono<Boolean> isAppendConfirmed(DcbAppendResult appendResult) {
                return applicationService.isAppendConfirmed(appendResult);
            }
        };
        var decoratedService = new ReactiveSnapshotDcbDeciderApplicationService<>(addingSideEffect);
        AtomicInteger decideCount = new AtomicInteger();
        var account = ReactiveSnapshotDcbDecider.from(DcbDecider.from(decideCounting(countingDecider(evolveCount, time), decideCount), command -> criteria(), event -> Set.of(tag())), store, SnapshotOptions.of(1, SnapshotPolicy.always()));
        seed();
        decoratedService.execute(new Define("A"), account).block(TIMEOUT);
        decideCount.set(0);
        addSideEffect.set(true);
        given.clear();

        Throwable thrown = catchThrowable(() -> decoratedService.execute(new Change("B"), account).block(TIMEOUT));

        assertAll(
                () -> assertThat(thrown).isInstanceOf(DcbSnapshotInvalidException.class),
                () -> assertThat(thrown).as("what reached the caller").isInstanceOfSatisfying(DcbSnapshotInvalidException.class, refused -> assertAll(
                        () -> assertThat(given).as("functions given to the service by the execute").hasSize(1).noneMatch(refused::isBeforeAppendOf),
                        () -> assertThat(refused.isBeforeAppendOf(otherServicesFunction)).as("marked for the other service's function").isTrue())),
                () -> assertThat(decideCount.get()).as("times the command was decided").isEqualTo(1),
                // The seed, A and the one B that committed before the other service's refusal
                () -> assertThat(requireNonNull(eventStore.read(criteria()).block(TIMEOUT)).events()).as("events in the boundary").hasSize(3),
                // The refusal escapes before the snapshot is saved, so the one from A is left in place
                () -> assertThat(store.findLatest(key).blockOptional().orElseThrow().state()).isEqualTo("A")
        );
    }

    @Test
    void a_refusal_marked_by_another_service_after_the_delegate_completed_reaches_the_caller_and_the_command_is_not_run_a_second_time() {
        // The decorator calls a second service after the append has committed, and that service's read is refused
        ControlledStore refusingStore = new ControlledStore(eventStore);
        refusingStore.refuseEveryRead = true;
        var otherService = new GenericDcbApplicationService<>(refusingStore, converter, (DomainEvent event) -> Set.of(tag()), GenericDcbApplicationService.defaultRetry());
        Function<List<DomainEvent>, List<DomainEvent>> otherServicesFunction = events -> List.of();
        AtomicBoolean callOtherService = new AtomicBoolean();
        List<Function<List<DomainEvent>, List<DomainEvent>>> given = new CopyOnWriteArrayList<>();
        DcbApplicationService<DomainEvent> callingOtherServiceAfterTheDelegate = new DcbApplicationService<>() {
            @Override
            public Mono<DcbAppendResult> execute(DcbCriteria criteria, DcbExecuteOptions<DomainEvent> options, Function<List<DomainEvent>, List<DomainEvent>> functionThatCallsDomainModel) {
                given.add(functionThatCallsDomainModel);
                return applicationService.execute(criteria, options, functionThatCallsDomainModel)
                        .flatMap(result -> callOtherService.get() ? otherService.execute(criteria(), otherServicesFunction).then(Mono.just(result)) : Mono.just(result));
            }

            @Override
            public Mono<Boolean> isAppendConfirmed(DcbAppendResult appendResult) {
                return applicationService.isAppendConfirmed(appendResult);
            }
        };
        var decoratedService = new ReactiveSnapshotDcbDeciderApplicationService<>(callingOtherServiceAfterTheDelegate);
        AtomicInteger decideCount = new AtomicInteger();
        var account = ReactiveSnapshotDcbDecider.from(DcbDecider.from(decideCounting(countingDecider(evolveCount, time), decideCount), command -> criteria(), event -> Set.of(tag())), store, SnapshotOptions.of(1, SnapshotPolicy.always()));
        seed();
        decoratedService.execute(new Define("A"), account).block(TIMEOUT);
        decideCount.set(0);
        callOtherService.set(true);
        given.clear();

        Throwable thrown = catchThrowable(() -> decoratedService.execute(new Change("B"), account).block(TIMEOUT));

        assertAll(
                () -> assertThat(thrown).as("what reached the caller").isInstanceOfSatisfying(DcbSnapshotInvalidException.class, refused -> assertAll(
                        () -> assertThat(given).as("functions given to the service by the execute").hasSize(1).noneMatch(refused::isBeforeAppendOf),
                        () -> assertThat(refused.isBeforeAppendOf(otherServicesFunction)).as("marked for the other service's function").isTrue())),
                () -> assertThat(decideCount.get()).as("times the command was decided").isEqualTo(1),
                // The seed, A and the one B that committed before the other service's refusal
                () -> assertThat(requireNonNull(eventStore.read(criteria()).block(TIMEOUT)).events()).as("events in the boundary").hasSize(3),
                // The refusal escapes before the snapshot is saved, so the one from A is left in place
                () -> assertThat(store.findLatest(key).blockOptional().orElseThrow().state()).isEqualTo("A")
        );
    }

    @Test
    void a_refusal_marked_by_a_second_service_that_is_given_the_same_options_after_the_delegate_completed_reaches_the_caller_and_the_command_is_not_run_a_second_time() {
        // The options are shared, and the second service marks its refusal for its own function
        ControlledStore refusingStore = new ControlledStore(eventStore);
        refusingStore.refuseEveryRead = true;
        var mirror = new GenericDcbApplicationService<>(refusingStore, converter, (DomainEvent event) -> Set.of(tag()), GenericDcbApplicationService.defaultRetry());
        Function<List<DomainEvent>, List<DomainEvent>> mirrorsFunction = events -> List.of();
        AtomicBoolean callMirror = new AtomicBoolean();
        List<DcbExecuteOptions<DomainEvent>> optionsGiven = new CopyOnWriteArrayList<>();
        DcbApplicationService<DomainEvent> mirroring = new DcbApplicationService<>() {
            @Override
            public Mono<DcbAppendResult> execute(DcbCriteria criteria, DcbExecuteOptions<DomainEvent> options, Function<List<DomainEvent>, List<DomainEvent>> functionThatCallsDomainModel) {
                optionsGiven.add(options);
                return applicationService.execute(criteria, options, functionThatCallsDomainModel)
                        .flatMap(result -> callMirror.get() ? mirror.execute(criteria, options, mirrorsFunction).then(Mono.just(result)) : Mono.just(result));
            }

            @Override
            public Mono<Boolean> isAppendConfirmed(DcbAppendResult appendResult) {
                return applicationService.isAppendConfirmed(appendResult);
            }
        };
        var mirroringService = new ReactiveSnapshotDcbDeciderApplicationService<>(mirroring);
        AtomicInteger decideCount = new AtomicInteger();
        var account = ReactiveSnapshotDcbDecider.from(DcbDecider.from(decideCounting(countingDecider(evolveCount, time), decideCount), command -> criteria(), event -> Set.of(tag())), store, SnapshotOptions.of(1, SnapshotPolicy.always()));
        seed();
        mirroringService.execute(new Define("A"), account).block(TIMEOUT);
        decideCount.set(0);
        callMirror.set(true);
        optionsGiven.clear();

        Throwable thrown = catchThrowable(() -> mirroringService.execute(new Change("B"), account).block(TIMEOUT));

        assertAll(
                () -> assertThat(thrown).as("what reached the caller").isInstanceOfSatisfying(DcbSnapshotInvalidException.class, refused -> assertAll(
                        () -> assertThat(optionsGiven).as("options given to the wrapper by the execute").hasSize(1).noneMatch(refused::isBeforeAppendOf),
                        () -> assertThat(refused.isBeforeAppendOf(mirrorsFunction)).as("marked for the second service's function").isTrue())),
                () -> assertThat(decideCount.get()).as("times the command was decided").isEqualTo(1),
                // The seed, A and the one B that committed before the second service's refusal
                () -> assertThat(requireNonNull(eventStore.read(criteria()).block(TIMEOUT)).events()).as("events in the boundary").hasSize(3),
                // The refusal escapes before the snapshot is saved, so the one from A is left in place
                () -> assertThat(store.findLatest(key).blockOptional().orElseThrow().state()).isEqualTo("A")
        );
    }

    @Test
    void a_refusal_marked_by_another_service_and_raised_by_the_transaction_executor_after_the_action_completed_reaches_the_caller_and_the_command_is_not_run_a_second_time() {
        // The executor raises it after the action committed the append, for example when it runs more work in the same transaction
        Function<List<DomainEvent>, List<DomainEvent>> otherServicesFunction = events -> List.of();
        DcbSnapshotInvalidException refusal = new DcbSnapshotInvalidException(criteria(), DcbMarkerVersions.of(Map.of("tag:name:" + ACCOUNT, new DcbMarkerVersions.MarkerVersion("epoch", 1, 0))), "refused by another service (test double)").beforeAppend(otherServicesFunction);
        AtomicBoolean refuseAfterTheAction = new AtomicBoolean();
        ReactiveTransactionExecutor raisingAfterTheAction = new ReactiveTransactionExecutor() {
            @Override
            public <T> Mono<T> inTransaction(Supplier<Mono<T>> action) {
                return Mono.defer(action).flatMap(result -> refuseAfterTheAction.get() ? Mono.<T>error(refusal) : Mono.just(result));
            }
        };
        ReactiveSynchronousEventDispatcher dispatcherWithoutEffect = new ReactiveSynchronousEventDispatcher() {
            @Override
            public Mono<Void> dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
                return Mono.empty();
            }

            @Override
            public boolean hasSubscriptions() {
                return true;
            }
        };
        var recordingService = new FunctionRecordingService(GenericDcbApplicationService.builder(eventStore, converter)
                .tagGenerator((DomainEvent event) -> Set.of(tag()))
                .synchronousSubscriptions(dispatcherWithoutEffect)
                .transactionExecutor(raisingAfterTheAction)
                .build());
        var executorService = new ReactiveSnapshotDcbDeciderApplicationService<>(recordingService);
        AtomicInteger decideCount = new AtomicInteger();
        var account = ReactiveSnapshotDcbDecider.from(DcbDecider.from(decideCounting(countingDecider(evolveCount, time), decideCount), command -> criteria(), event -> Set.of(tag())), store, SnapshotOptions.of(1, SnapshotPolicy.always()));
        seed();
        executorService.execute(new Define("A"), account).block(TIMEOUT);
        decideCount.set(0);
        refuseAfterTheAction.set(true);
        recordingService.given.clear();

        Throwable thrown = catchThrowable(() -> executorService.execute(new Change("B"), account).block(TIMEOUT));

        assertAll(
                () -> assertThat(thrown).as("what reached the caller").isSameAs(refusal),
                () -> assertThat(recordingService.given).as("functions given to the service by the execute").hasSize(1).noneMatch(refusal::isBeforeAppendOf),
                () -> assertThat(decideCount.get()).as("times the command was decided").isEqualTo(1),
                // The seed, A and the one B that committed before the refusal
                () -> assertThat(requireNonNull(eventStore.read(criteria()).block(TIMEOUT)).events()).as("events in the boundary").hasSize(3),
                // The refusal escapes before the snapshot is saved, so the one from A is left in place
                () -> assertThat(store.findLatest(key).blockOptional().orElseThrow().state()).isEqualTo("A")
        );
    }

    @Test
    void an_unconfirmed_append_saves_no_snapshot_and_warns_once_per_service_then_logs_at_debug() {
        ControlledStore controlled = new ControlledStore(eventStore);
        controlled.confirms = false;
        var unconfirmingService = new ReactiveSnapshotDcbDeciderApplicationService<>(new GenericDcbApplicationService<>(controlled, converter, (DomainEvent event) -> Set.of(tag()), GenericDcbApplicationService.defaultRetry()));
        var account = ReactiveSnapshotDcbDecider.from(dcbDecider, store, SnapshotOptions.of(1, SnapshotPolicy.always()));
        seed();

        unconfirmingService.execute(new Define("A"), account).block(TIMEOUT);
        unconfirmingService.execute(new Change("B"), account).block(TIMEOUT);
        unconfirmingService.execute(new Change("C"), account).block(TIMEOUT);

        assertAll(
                () -> assertThat(store.findLatest(key).blockOptional()).isEmpty(),
                () -> assertThat(logged(Level.WARN)).singleElement().asString().contains("isn't confirmed as committed").contains(key),
                () -> assertThat(logged(Level.DEBUG)).hasSize(2)
        );
    }

    @Test
    void an_append_without_marker_versions_saves_no_snapshot_and_warns_once_per_service_then_logs_at_debug() {
        ControlledStore controlled = new ControlledStore(eventStore);
        controlled.reportsVersions = false;
        var unversionedService = new ReactiveSnapshotDcbDeciderApplicationService<>(new GenericDcbApplicationService<>(controlled, converter, (DomainEvent event) -> Set.of(tag()), GenericDcbApplicationService.defaultRetry()));
        var account = ReactiveSnapshotDcbDecider.from(dcbDecider, store, SnapshotOptions.of(1, SnapshotPolicy.always()));
        seed();

        unversionedService.execute(new Define("A"), account).block(TIMEOUT);
        unversionedService.execute(new Change("B"), account).block(TIMEOUT);
        unversionedService.execute(new Change("C"), account).block(TIMEOUT);

        assertAll(
                () -> assertThat(store.findLatest(key).blockOptional()).isEmpty(),
                () -> assertThat(logged(Level.WARN)).singleElement().asString().contains("reported no marker versions").contains(key),
                () -> assertThat(logged(Level.DEBUG)).hasSize(2)
        );
    }

    @Test
    void a_new_service_warns_again_for_the_same_reason() {
        ControlledStore controlled = new ControlledStore(eventStore);
        controlled.confirms = false;
        var account = ReactiveSnapshotDcbDecider.from(dcbDecider, store, SnapshotOptions.of(1, SnapshotPolicy.always()));
        seed();

        for (int i = 0; i < 2; i++) {
            new ReactiveSnapshotDcbDeciderApplicationService<>(new GenericDcbApplicationService<>(controlled, converter, (DomainEvent event) -> Set.of(tag()), GenericDcbApplicationService.defaultRetry()))
                    .execute(new Change("B" + i), account).block(TIMEOUT);
        }

        assertThat(logged(Level.WARN)).hasSize(2);
    }

    @Test
    void a_snapshot_save_that_throws_a_checked_exception_before_returning_a_mono_does_not_error_and_the_append_is_committed() {
        assertSnapshotSaveThrowingBeforeReturningAMonoIsSwallowed(new IOException("snapshot store unreachable (test double)"));
    }

    @Test
    void a_snapshot_save_that_throws_a_StackOverflowError_before_returning_a_mono_does_not_error_and_the_append_is_committed() {
        assertSnapshotSaveThrowingBeforeReturningAMonoIsSwallowed(new StackOverflowError("snapshot store save overflowed (test double)"));
    }

    private void assertSnapshotSaveThrowingBeforeReturningAMonoIsSwallowed(Throwable saveFailure) {
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicy.always());
        AtomicInteger saveAttempts = new AtomicInteger();
        ReactiveSnapshotStore<String> failingStore = new ReactiveSnapshotStore<>() {
            @Override
            public Mono<Snapshot<String>> findLatest(String key) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> save(String key, Snapshot<String> snapshot) {
                saveAttempts.incrementAndGet();
                throw ReactiveSnapshotDcbDeciderApplicationServiceTest.<RuntimeException>sneakyThrow(saveFailure);
            }
        };
        AtomicReference<DcbAppendResult> result = new AtomicReference<>();
        // A snapshot is saved only on a boundary that already has a marker, so the save is reached
        seed();

        Throwable thrown = catchThrowable(() -> result.set(service.execute(new Define("A"), ReactiveSnapshotDcbDecider.from(dcbDecider, failingStore, options)).block(TIMEOUT)));

        evolveCount.set(0);
        service.execute(new Change("B"), ReactiveSnapshotDcbDecider.from(dcbDecider, store, options)).block(TIMEOUT);
        // The seed (1) and A (1) folded from the boundary plus the produced B (1) = 3, so A was appended before the save failed
        assertThat(evolveCount.get()).as("events folded by the next command").isEqualTo(3);
        assertThat(saveAttempts.get()).as("attempted saves").isEqualTo(1);
        assertThat(thrown).as("what escaped execute after the append committed").isNull();
        assertThat(result.get()).isNotNull();
    }

    @Test
    void second_execute_resumes_from_the_snapshot_and_folds_only_the_tail() {
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicy.always());
        var account = ReactiveSnapshotDcbDecider.from(dcbDecider, store, options);
        seed();
        service.execute(new Define("A"), account).block();

        evolveCount.set(0);
        service.execute(new Change("B"), account).block();

        assertAll(
                // Empty tail after the snapshot's marker versions, so only the produced event is folded (1), not a full replay.
                () -> assertThat(evolveCount.get()).isEqualTo(1),
                () -> assertThat(store.findLatest(key).blockOptional().orElseThrow().state()).isEqualTo("B")
        );
    }

    @Test
    void the_resume_read_folds_events_appended_after_the_snapshot_by_another_writer() {
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicy.always());
        var account = ReactiveSnapshotDcbDecider.from(dcbDecider, store, options);
        seed();
        service.execute(new Define("A"), account).block();

        // An event lands in the same boundary out-of-band, so the snapshot is now behind the boundary head.
        appendOutOfBand(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "X"));

        evolveCount.set(0);
        service.execute(new Change("B"), account).block();

        assertAll(
                // The out-of-band X and the produced B are both folded (2), proving the tail after the snapshot was read.
                () -> assertThat(evolveCount.get()).isEqualTo(2),
                () -> assertThat(store.findLatest(key).blockOptional().orElseThrow().state()).isEqualTo("B")
        );
    }

    @Test
    void a_schema_version_bump_ignores_the_old_snapshot() {
        seed();
        service.execute(new Define("A"), ReactiveSnapshotDcbDecider.from(dcbDecider, store, SnapshotOptions.of(1, SnapshotPolicy.always()))).block();

        evolveCount.set(0);
        service.execute(new Change("B"), ReactiveSnapshotDcbDecider.from(dcbDecider, store, SnapshotOptions.of(2, SnapshotPolicy.always()))).block();

        assertAll(
                // Old schema-1 snapshot ignored, whole boundary replayed: seed (1) and A (1) plus produced B (1) = 3.
                () -> assertThat(evolveCount.get()).isEqualTo(3),
                () -> assertThat(store.findLatest(key).blockOptional().orElseThrow().schemaVersion()).isEqualTo(2),
                () -> assertThat(store.findLatest(key).blockOptional().orElseThrow().state()).isEqualTo("B")
        );
    }

    @Test
    void from_throws_NullPointerException_when_the_dcbDecider_is_null() {
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicy.always());
        assertThatThrownBy(() -> ReactiveSnapshotDcbDecider.from(null, store, options))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("dcbDecider");
    }

    @Test
    void from_throws_NullPointerException_when_the_store_is_null() {
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicy.always());
        assertThatThrownBy(() -> ReactiveSnapshotDcbDecider.from(dcbDecider, null, options))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("store");
    }

    @Test
    void from_throws_NullPointerException_when_the_options_are_null() {
        assertThatThrownBy(() -> ReactiveSnapshotDcbDecider.from(dcbDecider, store, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("options");
    }

    @Test
    void from_throws_NullPointerException_when_the_keyFunction_is_null() {
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicy.always());
        assertThatThrownBy(() -> ReactiveSnapshotDcbDecider.from(dcbDecider, store, options, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("keyFunction");
    }

    @Test
    void executeAndReturnState_with_a_single_command_refuses_a_null_state_before_anything_is_written() {
        var account = ReactiveSnapshotDcbDecider.from(foldsToNullDcbDecider(time), store, SnapshotOptions.of(1, SnapshotPolicy.always()));

        assertThatThrownBy(() -> service.executeAndReturnState(new Delete(), account).block(TIMEOUT))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Mono cannot carry null");

        assertThat(eventStore.count(criteria()).block(TIMEOUT)).as("nothing appended for a refused null state").isZero();
    }

    @Test
    void executeAndReturnState_with_a_command_list_also_refuses_a_null_state_before_anything_is_written() {
        var account = ReactiveSnapshotDcbDecider.from(foldsToNullDcbDecider(time), store, SnapshotOptions.of(1, SnapshotPolicy.always()));

        assertThatThrownBy(() -> service.executeAndReturnState(List.of(new Delete()), account).block(TIMEOUT))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Mono cannot carry null");

        assertThat(eventStore.count(criteria()).block(TIMEOUT)).as("nothing appended for a refused null state").isZero();
    }

    @Test
    void executeAndReturnState_with_several_commands_checks_only_the_state_after_the_last_command() {
        var account = ReactiveSnapshotDcbDecider.from(foldsToNullOnTheFirstOfSeveralCommandsDcbDecider(time), store, SnapshotOptions.of(1, SnapshotPolicy.always()));

        // FoldToNull makes the state null in the middle of the list, Keep makes it non-null again before the list ends
        Signal<String> signal = service.executeAndReturnState(List.of(new FoldToNull(), new Keep("Z")), account).materialize().block(TIMEOUT);

        assertAll(
                () -> assertThat(requireNonNull(signal).get()).as("state emitted by %s", signal).isEqualTo("Z"),
                () -> assertThat(eventStore.count(criteria()).block(TIMEOUT)).as("both events appended, including the one decided while the state was null").isEqualTo(2L)
        );
    }

    @Test
    void executeAndReturnDecision_still_allows_a_null_state_and_the_append_is_committed() {
        var account = ReactiveSnapshotDcbDecider.from(foldsToNullDcbDecider(time), store, SnapshotOptions.of(1, SnapshotPolicy.always()));

        Decider.Decision<@Nullable String, DomainEvent> decision = service.executeAndReturnDecision(new Delete(), account).block(TIMEOUT);

        assertAll(
                () -> assertThat(decision).isNotNull(),
                () -> assertThat(decision.state()).isNull(),
                () -> assertThat(eventStore.count(criteria()).block(TIMEOUT)).as("the committed append").isEqualTo(1L)
        );
    }

    private static DcbDecider<Delete, @Nullable String, DomainEvent> foldsToNullDcbDecider(LocalDateTime time) {
        Decider<Delete, @Nullable String, DomainEvent> decider = new Decider<>() {
            @Override
            public @Nullable String initialState() {
                return "";
            }

            @NonNull
            @Override
            public List<DomainEvent> decide(@NonNull Delete command, @Nullable String state) {
                return List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "DELETED"));
            }

            @Override
            public @Nullable String evolve(@Nullable String state, @NonNull DomainEvent event) {
                return null;
            }
        };
        return DcbDecider.from(decider, command -> criteria(), event -> Set.of(tag()));
    }

    private record Delete() {
    }

    private static DcbDecider<MultiFoldCmd, @Nullable String, DomainEvent> foldsToNullOnTheFirstOfSeveralCommandsDcbDecider(LocalDateTime time) {
        Decider<MultiFoldCmd, @Nullable String, DomainEvent> decider = new Decider<>() {
            @Override
            public @Nullable String initialState() {
                return "";
            }

            @NonNull
            @Override
            public List<DomainEvent> decide(@NonNull MultiFoldCmd command, @Nullable String state) {
                return switch (command) {
                    case Keep k -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", k.name()));
                    case FoldToNull ignored -> List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "DELETED"));
                };
            }

            @Override
            public @Nullable String evolve(@Nullable String state, @NonNull DomainEvent event) {
                return event instanceof NameWasChanged ? null : event.name();
            }
        };
        return DcbDecider.from(decider, command -> criteria(), event -> Set.of(tag()));
    }

    private sealed interface MultiFoldCmd {
    }

    private record Keep(String name) implements MultiFoldCmd {
    }

    private record FoldToNull() implements MultiFoldCmd {
    }

    private void seed() {
        appendOutOfBand(new NameDefined(UUID.randomUUID().toString(), time, "name", "seed"));
    }

    private List<String> logged(Level level) {
        return logAppender.list.stream().filter(event -> event.getLevel() == level).map(ILoggingEvent::getFormattedMessage).toList();
    }

    private void appendOutOfBand(DomainEvent event) {
        appendOutOfBandLater(event).block();
    }

    private Mono<DcbAppendResult> appendOutOfBandLater(DomainEvent event) {
        List<Tag> tags = List.of(tag());
        return Mono.defer(() -> eventStore.append(converter.toCloudEvents(List.of(event)).stream().map(ce -> DcbCloudEvents.withTags(ce, tags)).toList()));
    }

    private static DcbCriteria criteria() {
        return DcbCriteria.tags(tag());
    }

    private static Tag tag() {
        return Tag.of("name", ACCOUNT);
    }

    private sealed interface Cmd {
    }

    private record Define(String name) implements Cmd {
    }

    private record Change(String name) implements Cmd {
    }

    private static Decider<Cmd, String, DomainEvent> countingDecider(AtomicInteger evolveCount, LocalDateTime time) {
        return new Decider<>() {
            @Override
            public String initialState() {
                return "";
            }

            @NonNull
            @Override
            public List<DomainEvent> decide(@NonNull Cmd command, String state) {
                return switch (command) {
                    case Define d -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", d.name()));
                    case Change c -> List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", c.name()));
                };
            }

            @Override
            public String evolve(String state, @NonNull DomainEvent event) {
                evolveCount.incrementAndGet();
                return event.name();
            }
        };
    }

    private static Decider<Cmd, String, DomainEvent> decideCounting(Decider<Cmd, String, DomainEvent> delegate, AtomicInteger decideCount) {
        return new Decider<>() {
            @Override
            public String initialState() {
                return delegate.initialState();
            }

            @NonNull
            @Override
            public List<DomainEvent> decide(@NonNull Cmd command, String state) {
                decideCount.incrementAndGet();
                return delegate.decide(command, state);
            }

            @Override
            public String evolve(String state, @NonNull DomainEvent event) {
                return delegate.evolve(state, event);
            }
        };
    }

    private static Decider<Cmd, String, DomainEvent> decideRecordingStates(Decider<Cmd, String, DomainEvent> delegate, List<String> decidedOnStates) {
        return new Decider<>() {
            @Override
            public String initialState() {
                return delegate.initialState();
            }

            @NonNull
            @Override
            public List<DomainEvent> decide(@NonNull Cmd command, String state) {
                decidedOnStates.add(state);
                return delegate.decide(command, state);
            }

            @Override
            public String evolve(String state, @NonNull DomainEvent event) {
                return delegate.evolve(state, event);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> T sneakyThrow(Throwable failure) throws T {
        throw (T) failure;
    }

    /**
     * Records the domain function each execute gives the service it decorates.
     */
    private static final class FunctionRecordingService implements DcbApplicationService<DomainEvent> {
        final List<Function<List<DomainEvent>, List<DomainEvent>>> given = new CopyOnWriteArrayList<>();
        private final DcbApplicationService<DomainEvent> delegate;

        FunctionRecordingService(DcbApplicationService<DomainEvent> delegate) {
            this.delegate = delegate;
        }

        @Override
        public Mono<DcbAppendResult> execute(DcbCriteria criteria, DcbExecuteOptions<DomainEvent> options, Function<List<DomainEvent>, List<DomainEvent>> functionThatCallsDomainModel) {
            given.add(functionThatCallsDomainModel);
            return delegate.execute(criteria, options, functionThatCallsDomainModel);
        }

        @Override
        public Mono<Boolean> isAppendConfirmed(DcbAppendResult appendResult) {
            return delegate.isAppendConfirmed(appendResult);
        }
    }

    /**
     * Passes on what each execute gives it after rewriting the options and the domain function, as a wrapper that
     * derives them would.
     */
    private static final class RewritingService implements DcbApplicationService<DomainEvent> {
        final List<Function<List<DomainEvent>, List<DomainEvent>>> given = new CopyOnWriteArrayList<>();
        final List<Function<List<DomainEvent>, List<DomainEvent>>> passedOn = new CopyOnWriteArrayList<>();
        private final DcbApplicationService<DomainEvent> delegate;
        private final UnaryOperator<DcbExecuteOptions<DomainEvent>> rewriteOptions;
        private final UnaryOperator<Function<List<DomainEvent>, List<DomainEvent>>> rewriteFunction;

        RewritingService(DcbApplicationService<DomainEvent> delegate, UnaryOperator<DcbExecuteOptions<DomainEvent>> rewriteOptions, UnaryOperator<Function<List<DomainEvent>, List<DomainEvent>>> rewriteFunction) {
            this.delegate = delegate;
            this.rewriteOptions = rewriteOptions;
            this.rewriteFunction = rewriteFunction;
        }

        @Override
        public Mono<DcbAppendResult> execute(DcbCriteria criteria, DcbExecuteOptions<DomainEvent> options, Function<List<DomainEvent>, List<DomainEvent>> functionThatCallsDomainModel) {
            given.add(functionThatCallsDomainModel);
            Function<List<DomainEvent>, List<DomainEvent>> function = rewriteFunction.apply(functionThatCallsDomainModel);
            passedOn.add(function);
            return delegate.execute(criteria, rewriteOptions.apply(options), function);
        }

        @Override
        public Mono<Boolean> isAppendConfirmed(DcbAppendResult appendResult) {
            return delegate.isAppendConfirmed(appendResult);
        }
    }

    /**
     * Delegates to the Mongo store and can refuse a resume, drop the marker versions of a read, decline to confirm or
     * run an action just before the next append.
     */
    private static final class ControlledStore implements DcbEventStore, DcbAppendConfirmation {
        final List<DcbReadOptions> reads = new ArrayList<>();
        volatile boolean refuseReadsAfterVersions;
        // Resume reads up to this many are answered, the ones after are refused
        volatile int afterVersionsReadsAllowed;
        // A refused resume read gets the whole boundary instead, which holds events below the saved position
        volatile boolean answerRefusedReadsWithEventsBelowTheSavedPosition;
        volatile @Nullable Mono<Void> beforeNextAppend;
        private final AtomicInteger afterVersionsReads = new AtomicInteger();
        volatile boolean refuseEveryRead;
        volatile boolean reportsVersions = true;
        volatile boolean confirms = true;
        private final ReactorMongoEventStore delegate;

        ControlledStore(ReactorMongoEventStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public Mono<DcbEventStream> read(DcbCriteria criteria, DcbReadOptions options) {
            reads.add(options);
            boolean refuseThisResume = refuseReadsAfterVersions && options.afterVersions().isPresent() && afterVersionsReads.incrementAndGet() > afterVersionsReadsAllowed;
            if (refuseThisResume && answerRefusedReadsWithEventsBelowTheSavedPosition) {
                return delegate.read(criteria, DcbReadOptions.fromBeginning().withVersions()).map(stream -> reportsVersions ? stream : new DcbEventStream(stream.events(), stream.lastSequencePosition(), stream.consistencyToken()));
            }
            if (refuseEveryRead || refuseThisResume) {
                DcbMarkerVersions refused = options.afterVersions().orElseGet(() -> DcbMarkerVersions.of(Map.of("tag:name:" + ACCOUNT, new DcbMarkerVersions.MarkerVersion("epoch", 1, 0))));
                return Mono.error(new DcbSnapshotInvalidException(criteria, refused, "refused (test double)"));
            }
            return delegate.read(criteria, options).map(stream -> reportsVersions ? stream : new DcbEventStream(stream.events(), stream.lastSequencePosition(), stream.consistencyToken()));
        }

        @Override
        public Mono<DcbAppendResult> append(List<CloudEvent> events) {
            return delegate.append(events);
        }

        @Override
        public Mono<DcbAppendResult> append(List<CloudEvent> events, DcbAppendCondition condition) {
            return Mono.defer(() -> {
                Mono<Void> action = beforeNextAppend;
                beforeNextAppend = null;
                return (action == null ? Mono.<Void>empty() : action).then(Mono.defer(() -> delegate.append(events, condition)));
            });
        }

        @Override
        public Mono<Boolean> isConfirmed(DcbAppendResult appendResult) {
            return confirms ? delegate.isConfirmed(appendResult) : Mono.just(false);
        }
    }
}
