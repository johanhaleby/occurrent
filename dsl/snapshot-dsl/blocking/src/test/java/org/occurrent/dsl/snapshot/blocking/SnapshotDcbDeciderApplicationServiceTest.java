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

package org.occurrent.dsl.snapshot.blocking;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.*;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.blocking.SynchronousEventDispatcher;
import org.occurrent.application.service.blocking.TransactionExecutor;
import org.occurrent.application.service.blocking.dcb.DcbApplicationService;
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.dsl.dcb.DcbDecider;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.snapshot.DcbSnapshotKeys;
import org.occurrent.dsl.snapshot.Snapshot;
import org.occurrent.dsl.snapshot.SnapshotOptions;
import org.occurrent.dsl.snapshot.SnapshotPolicy;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendConfirmation;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.DcbSnapshotInvalidException;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayName("SnapshotDcbDeciderApplicationService")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class SnapshotDcbDeciderApplicationServiceTest {

    private static final String ACCOUNT = "acct";

    private InMemoryEventStore eventStore;
    private CloudEventConverter<DomainEvent> converter;
    private DcbApplicationService<DomainEvent> applicationService;
    private SnapshotDcbDeciderApplicationService<DomainEvent> service;
    private SnapshotStore<String> store;
    private AtomicInteger evolveCount;
    private DcbDecider<Cmd, String, DomainEvent> dcbDecider;
    private String key;
    private LocalDateTime time;
    private ListAppender<ILoggingEvent> logAppender;
    private ch.qos.logback.classic.Logger log;
    private Level originalLevel;

    @BeforeEach
    void attachLogAppender() {
        log = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(SnapshotDcbDeciderApplicationService.class);
        originalLevel = log.getLevel();
        log.setLevel(Level.DEBUG);
        logAppender = new ListAppender<>();
        logAppender.start();
        log.addAppender(logAppender);
    }

    @AfterEach
    void detachLogAppender() {
        log.detachAppender(logAppender);
        log.setLevel(originalLevel);
    }

    @BeforeEach
    void setup() {
        time = LocalDateTime.now();
        eventStore = new InMemoryEventStore();
        converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        applicationService = new GenericDcbApplicationService<>(eventStore, converter, (DomainEvent event) -> Set.of(tag()), GenericDcbApplicationService.defaultRetryStrategy());
        service = new SnapshotDcbDeciderApplicationService<>(applicationService);
        store = SnapshotStore.inMemory();
        evolveCount = new AtomicInteger();
        Decider<Cmd, String, DomainEvent> decider = countingDecider(evolveCount, time);
        dcbDecider = DcbDecider.from(decider, command -> criteria(), event -> Set.of(tag()));
        key = DcbSnapshotKeys.canonicalKey(criteria());
    }

    @Test
    void executeAndReturnState_returns_the_folded_state() {
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicy.always());
        var account = SnapshotDcbDecider.from(dcbDecider, store, options);
        service.execute(new Define("A"), account);

        String state = service.executeAndReturnState(new Change("B"), account);

        assertThat(state).isEqualTo("B");
    }

    @Test
    void executeAndReturnDecision_returns_state_and_events() {
        Decider.Decision<String, DomainEvent> decision = service.executeAndReturnDecision(new Define("A"), SnapshotDcbDecider.from(dcbDecider, store, SnapshotOptions.of(1, SnapshotPolicy.always())));

        assertAll(
                () -> assertThat(decision.state()).isEqualTo("A"),
                () -> assertThat(decision.events()).hasSize(1)
        );
    }

    @Test
    void first_execute_on_an_empty_boundary_appends_but_saves_no_snapshot() {
        // The read of an empty boundary has no marker to compare the append's epoch with
        Optional<DcbAppendResult> result = service.execute(new Define("A"), SnapshotDcbDecider.from(dcbDecider, store, SnapshotOptions.of(1, SnapshotPolicy.always())));

        assertAll(
                () -> assertThat(result).isPresent(),
                () -> assertThat(store.findLatest(key)).isEmpty()
        );
    }

    @Test
    void execute_on_a_non_empty_boundary_saves_a_snapshot_keyed_by_criteria_at_marker_versions() {
        seed();

        service.execute(new Define("A"), SnapshotDcbDecider.from(dcbDecider, store, SnapshotOptions.of(1, SnapshotPolicy.always())));

        Snapshot<String> snapshot = store.findLatest(key).orElseThrow();
        assertAll(
                () -> assertThat(snapshot.state()).isEqualTo("A"),
                () -> assertThat(snapshot.markerVersions()).isEqualTo(eventStore.read(criteria(), DcbReadOptions.fromBeginning().withVersions()).markerVersions()),
                () -> assertThat(snapshot.version()).isZero()
        );
    }

    @Test
    void execute_with_the_default_key_function_resolves_the_canonical_criteria_key() {
        // The 3-arg from(...) defaults keyFunction to DcbSnapshotKeys::canonicalKey; observe the same key as an explicit one.
        var account = SnapshotDcbDecider.from(dcbDecider, store, SnapshotOptions.of(1, SnapshotPolicy.always()));
        seed();

        service.execute(new Define("A"), account);

        assertThat(store.findLatest(DcbSnapshotKeys.canonicalKey(criteria()))).hasValueSatisfying(s -> assertThat(s.state()).isEqualTo("A"));
    }

    @Test
    void second_execute_resumes_from_the_snapshot_and_folds_only_the_tail() {
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicy.always());
        var account = SnapshotDcbDecider.from(dcbDecider, store, options);
        seed();
        service.execute(new Define("A"), account);

        evolveCount.set(0);
        service.execute(new Change("B"), account);

        assertAll(
                // Empty tail after the snapshot's marker versions, so only the produced event is folded (1), not a full replay.
                () -> assertThat(evolveCount.get()).isEqualTo(1),
                () -> assertThat(store.findLatest(key).orElseThrow().state()).isEqualTo("B")
        );
    }

    @Test
    void the_resume_read_folds_events_appended_after_the_snapshot_by_another_writer() {
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicy.always());
        var account = SnapshotDcbDecider.from(dcbDecider, store, options);
        seed();
        service.execute(new Define("A"), account);

        // An event lands in the same boundary out-of-band, so the snapshot is now behind the boundary head.
        appendOutOfBand(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "X"));

        evolveCount.set(0);
        service.execute(new Change("B"), account);

        assertAll(
                // The out-of-band X and the produced B are both folded (2), proving the tail after the snapshot was read.
                () -> assertThat(evolveCount.get()).isEqualTo(2),
                () -> assertThat(store.findLatest(key).orElseThrow().state()).isEqualTo("B")
        );
    }

    @Test
    void a_snapshot_saved_at_a_position_by_0_34_is_not_used_and_is_replaced_by_one_with_marker_versions() {
        seed();
        store.save(key, new Snapshot<>("stale", 1000, 1));

        evolveCount.set(0);
        service.execute(new Change("B"), SnapshotDcbDecider.from(dcbDecider, store, SnapshotOptions.of(1, SnapshotPolicy.always())));

        Snapshot<String> replaced = store.findLatest(key).orElseThrow();
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
        var refusingService = new SnapshotDcbDeciderApplicationService<>(new GenericDcbApplicationService<>(controlled, converter, (DomainEvent event) -> Set.of(tag()), GenericDcbApplicationService.defaultRetryStrategy()));
        var account = SnapshotDcbDecider.from(dcbDecider, store, SnapshotOptions.of(1, SnapshotPolicy.always()));
        seed();
        refusingService.execute(new Define("A"), account);
        DcbMarkerVersions savedVersions = store.findLatest(key).orElseThrow().markerVersions().orElseThrow();
        controlled.refuseReadsAfterVersions = true;
        controlled.reads.clear();

        evolveCount.set(0);
        refusingService.execute(new Change("B"), account);

        Snapshot<String> replaced = store.findLatest(key).orElseThrow();
        assertAll(
                () -> assertThat(controlled.reads).containsExactly(DcbReadOptions.afterVersions(savedVersions), DcbReadOptions.fromBeginning().withVersions()),
                // The seed (1), A (1) and the produced B (1), so the state was folded from the initial state
                () -> assertThat(evolveCount.get()).isEqualTo(3),
                () -> assertThat(replaced.state()).isEqualTo("B"),
                () -> assertThat(replaced.markerVersions()).isPresent().isNotEqualTo(Optional.of(savedVersions))
        );
    }

    @Test
    void a_refusal_to_read_without_a_saved_snapshot_is_not_swallowed() {
        ControlledStore controlled = new ControlledStore(eventStore);
        controlled.refuseEveryRead = true;
        var refusingService = new SnapshotDcbDeciderApplicationService<>(new GenericDcbApplicationService<>(controlled, converter, (DomainEvent event) -> Set.of(tag()), GenericDcbApplicationService.defaultRetryStrategy()));

        Throwable thrown = catchThrowable(() -> refusingService.execute(new Define("A"), SnapshotDcbDecider.from(dcbDecider, store, SnapshotOptions.of(1, SnapshotPolicy.always()))));

        assertThat(thrown).isInstanceOf(DcbSnapshotInvalidException.class);
    }

    @Test
    void a_refusal_after_the_append_reaches_the_caller_and_the_command_is_not_run_a_second_time() {
        // A synchronous subscription without a transaction throws after the append has committed
        AtomicBoolean refuseAfterAppend = new AtomicBoolean();
        DcbSnapshotInvalidException refusal = new DcbSnapshotInvalidException(criteria(), DcbMarkerVersions.of(Map.of("tag:name:" + ACCOUNT, new DcbMarkerVersions.MarkerVersion("epoch", 1, 0))), "refused by a synchronous subscription (test double)");
        SynchronousEventDispatcher refusingDispatcher = new SynchronousEventDispatcher() {
            @Override
            public void dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
                if (refuseAfterAppend.get()) {
                    throw refusal;
                }
            }

            @Override
            public boolean hasSubscriptions() {
                return true;
            }
        };
        var dispatchingService = new SnapshotDcbDeciderApplicationService<>(GenericDcbApplicationService.builder(eventStore, converter)
                .tagGenerator((DomainEvent event) -> Set.of(tag()))
                .synchronousSubscriptions(refusingDispatcher)
                .transactionExecutor(TransactionExecutor.noTransaction())
                .build());
        AtomicInteger decideCount = new AtomicInteger();
        var account = SnapshotDcbDecider.from(DcbDecider.from(decideCounting(countingDecider(evolveCount, time), decideCount), command -> criteria(), event -> Set.of(tag())), store, SnapshotOptions.of(1, SnapshotPolicy.always()));
        seed();
        dispatchingService.execute(new Define("A"), account);
        decideCount.set(0);
        refuseAfterAppend.set(true);

        Throwable thrown = catchThrowable(() -> dispatchingService.execute(new Change("B"), account));

        assertAll(
                () -> assertThat(thrown).isSameAs(refusal),
                () -> assertThat(decideCount.get()).as("times the command was decided").isEqualTo(1),
                // The seed, A and the one B that committed before the refusal
                () -> assertThat(eventStore.read(criteria()).events()).as("events in the boundary").hasSize(3),
                // The refusal escapes before the snapshot is saved, so the one from A is left in place
                () -> assertThat(store.findLatest(key).orElseThrow().state()).isEqualTo("A")
        );
    }

    @Test
    void an_unconfirmed_append_saves_no_snapshot_and_warns_once_per_service_then_logs_at_debug() {
        ControlledStore controlled = new ControlledStore(eventStore);
        controlled.confirms = false;
        var unconfirmingService = new SnapshotDcbDeciderApplicationService<>(new GenericDcbApplicationService<>(controlled, converter, (DomainEvent event) -> Set.of(tag()), GenericDcbApplicationService.defaultRetryStrategy()));
        var account = SnapshotDcbDecider.from(dcbDecider, store, SnapshotOptions.of(1, SnapshotPolicy.always()));
        seed();

        unconfirmingService.execute(new Define("A"), account);
        unconfirmingService.execute(new Change("B"), account);
        unconfirmingService.execute(new Change("C"), account);

        assertAll(
                () -> assertThat(store.findLatest(key)).isEmpty(),
                () -> assertThat(logged(Level.WARN)).singleElement().asString().contains("isn't confirmed as committed").contains(key),
                () -> assertThat(logged(Level.DEBUG)).hasSize(2)
        );
    }

    @Test
    void an_append_without_marker_versions_saves_no_snapshot_and_warns_once_per_service_then_logs_at_debug() {
        ControlledStore controlled = new ControlledStore(eventStore);
        controlled.reportsVersions = false;
        var unversionedService = new SnapshotDcbDeciderApplicationService<>(new GenericDcbApplicationService<>(controlled, converter, (DomainEvent event) -> Set.of(tag()), GenericDcbApplicationService.defaultRetryStrategy()));
        var account = SnapshotDcbDecider.from(dcbDecider, store, SnapshotOptions.of(1, SnapshotPolicy.always()));
        seed();

        unversionedService.execute(new Define("A"), account);
        unversionedService.execute(new Change("B"), account);
        unversionedService.execute(new Change("C"), account);

        assertAll(
                () -> assertThat(store.findLatest(key)).isEmpty(),
                () -> assertThat(logged(Level.WARN)).singleElement().asString().contains("reported no marker versions").contains(key),
                () -> assertThat(logged(Level.DEBUG)).hasSize(2)
        );
    }

    @Test
    void a_new_service_warns_again_for_the_same_reason() {
        ControlledStore controlled = new ControlledStore(eventStore);
        controlled.confirms = false;
        var account = SnapshotDcbDecider.from(dcbDecider, store, SnapshotOptions.of(1, SnapshotPolicy.always()));
        seed();

        for (int i = 0; i < 2; i++) {
            new SnapshotDcbDeciderApplicationService<>(new GenericDcbApplicationService<>(controlled, converter, (DomainEvent event) -> Set.of(tag()), GenericDcbApplicationService.defaultRetryStrategy()))
                    .execute(new Change("B" + i), account);
        }

        assertThat(logged(Level.WARN)).hasSize(2);
    }

    @Test
    void a_snapshot_save_that_throws_a_runtime_exception_does_not_fail_execute_and_the_append_is_committed() {
        assertSnapshotSaveFailureIsSwallowed(new IllegalStateException("snapshot store save failed (test double)"));
    }

    @Test
    void a_snapshot_save_that_throws_a_checked_exception_does_not_fail_execute_and_the_append_is_committed() {
        assertSnapshotSaveFailureIsSwallowed(new IOException("snapshot store unreachable (test double)"));
    }

    private void assertSnapshotSaveFailureIsSwallowed(Throwable saveFailure) {
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicy.always());
        AtomicReference<Optional<DcbAppendResult>> result = new AtomicReference<>();
        ThrowingSnapshotStore<String> throwingStore = new ThrowingSnapshotStore<>(saveFailure);
        // A snapshot is saved only on a boundary that already has a marker, so the save is reached
        seed();

        Throwable thrown = catchThrowable(() -> result.set(service.execute(new Define("A"), SnapshotDcbDecider.from(dcbDecider, throwingStore, options))));

        evolveCount.set(0);
        service.execute(new Change("B"), SnapshotDcbDecider.from(dcbDecider, store, options));
        // The seed (1) and A (1) folded from the boundary plus the produced B (1) = 3, so A was appended before the save failed
        assertThat(evolveCount.get()).as("events folded by the next command").isEqualTo(3);
        assertThat(throwingStore.saveAttempts()).as("attempted saves").isEqualTo(1);
        assertThat(thrown).as("what escaped execute after the append committed").isNull();
        assertThat(result.get()).isPresent();
    }

    @Test
    void a_schema_version_bump_ignores_the_old_snapshot() {
        seed();
        service.execute(new Define("A"), SnapshotDcbDecider.from(dcbDecider, store, SnapshotOptions.of(1, SnapshotPolicy.always())));

        evolveCount.set(0);
        service.execute(new Change("B"), SnapshotDcbDecider.from(dcbDecider, store, SnapshotOptions.of(2, SnapshotPolicy.always())));

        assertAll(
                // Old schema-1 snapshot ignored, whole boundary replayed: seed (1) and A (1) plus produced B (1) = 3.
                () -> assertThat(evolveCount.get()).isEqualTo(3),
                () -> assertThat(store.findLatest(key).orElseThrow().schemaVersion()).isEqualTo(2),
                () -> assertThat(store.findLatest(key).orElseThrow().state()).isEqualTo("B")
        );
    }

    @Test
    void from_throws_NullPointerException_when_the_dcbDecider_is_null() {
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicy.always());
        assertThatThrownBy(() -> SnapshotDcbDecider.from(null, store, options))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("dcbDecider");
    }

    @Test
    void from_throws_NullPointerException_when_the_store_is_null() {
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicy.always());
        assertThatThrownBy(() -> SnapshotDcbDecider.from(dcbDecider, null, options))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("store");
    }

    @Test
    void from_throws_NullPointerException_when_the_options_are_null() {
        assertThatThrownBy(() -> SnapshotDcbDecider.from(dcbDecider, store, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("options");
    }

    @Test
    void from_throws_NullPointerException_when_the_keyFunction_is_null() {
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicy.always());
        assertThatThrownBy(() -> SnapshotDcbDecider.from(dcbDecider, store, options, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("keyFunction");
    }

    private void seed() {
        appendOutOfBand(new NameDefined(UUID.randomUUID().toString(), time, "name", "seed"));
    }

    private List<String> logged(Level level) {
        return logAppender.list.stream().filter(event -> event.getLevel() == level).map(ILoggingEvent::getFormattedMessage).toList();
    }

    private void appendOutOfBand(DomainEvent event) {
        List<Tag> tags = List.of(tag());
        eventStore.append(converter.toCloudEvents(List.of(event)).stream().map(ce -> DcbCloudEvents.withTags(ce, tags)).toList());
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

    /**
     * Delegates to an in-memory store and can refuse a resume, drop the marker versions of a read or decline to confirm.
     */
    private static final class ControlledStore implements DcbEventStore, DcbAppendConfirmation {
        final List<DcbReadOptions> reads = new ArrayList<>();
        boolean refuseReadsAfterVersions;
        boolean refuseEveryRead;
        boolean reportsVersions = true;
        boolean confirms = true;
        private final InMemoryEventStore delegate;

        ControlledStore(InMemoryEventStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public DcbEventStream read(DcbCriteria criteria, DcbReadOptions options) {
            reads.add(options);
            if (refuseEveryRead || (refuseReadsAfterVersions && options.afterVersions().isPresent())) {
                DcbMarkerVersions refused = options.afterVersions().orElseGet(() -> DcbMarkerVersions.of(Map.of("tag:name:" + ACCOUNT, new DcbMarkerVersions.MarkerVersion("epoch", 1, 0))));
                throw new DcbSnapshotInvalidException(criteria, refused, "refused (test double)");
            }
            DcbEventStream stream = delegate.read(criteria, options);
            return reportsVersions ? stream : new DcbEventStream(stream.events(), stream.lastSequencePosition(), stream.consistencyToken());
        }

        @Override
        public DcbAppendResult append(List<CloudEvent> events) {
            return delegate.append(events);
        }

        @Override
        public DcbAppendResult append(List<CloudEvent> events, DcbAppendCondition condition) {
            return delegate.append(events, condition);
        }

        @Override
        public boolean isConfirmed(DcbAppendResult appendResult) {
            return confirms && delegate.isConfirmed(appendResult);
        }
    }
}
