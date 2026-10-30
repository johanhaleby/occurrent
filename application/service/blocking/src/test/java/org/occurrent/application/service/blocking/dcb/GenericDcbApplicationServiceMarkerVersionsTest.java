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

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.application.service.blocking.SynchronousEventDispatcher;
import org.occurrent.application.service.blocking.TransactionExecutor;
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.eventstore.api.dcb.*;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions.MarkerVersion;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;

@DisplayName("GenericDcbApplicationService marker versions")
@DisplayNameGeneration(ReplaceUnderscores.class)
class GenericDcbApplicationServiceMarkerVersionsTest {

    private static final String KEY = "tag:name:1";
    private static final DcbCriteria CRITERIA = tags(Tag.parse("name:1"));

    @Nested
    @DisplayName("when reading")
    class When_reading {

        @Test
        void reads_with_plain_read_when_no_option_is_set() {
            var store = new ScriptedStore(Optional.empty(), Optional.empty());

            service(store).execute(CRITERIA, events -> List.of(new DomainEvent("Changed", "name:1")));

            assertThat(store.readOptions).containsExactly(Optional.empty());
        }

        @Test
        void withVersions_reads_the_whole_boundary_and_asks_for_versions() {
            var store = new ScriptedStore(Optional.empty(), Optional.empty());

            service(store).execute(CRITERIA, DcbExecuteOptions.<DomainEvent>options().withVersions(), events -> List.of(new DomainEvent("Changed", "name:1")));

            assertThat(store.readOptions).containsExactly(Optional.of(DcbReadOptions.fromBeginning().withVersions()));
        }

        @Test
        void fromVersions_reads_after_the_given_versions() {
            var store = new ScriptedStore(Optional.empty(), Optional.empty());
            DcbMarkerVersions saved = versions("epoch-1", 2);

            service(store).execute(CRITERIA, DcbExecuteOptions.<DomainEvent>options().fromVersions(saved, 3), events -> List.of(new DomainEvent("Changed", "name:1")));

            assertThat(store.readOptions).containsExactly(Optional.of(DcbReadOptions.afterVersions(saved)));
        }

        @Test
        @SuppressWarnings("removal")
        void fromPosition_still_reads_after_the_position() {
            var store = new ScriptedStore(Optional.empty(), Optional.empty());

            service(store).execute(CRITERIA, DcbExecuteOptions.<DomainEvent>options().fromPosition(7L), events -> List.of(new DomainEvent("Changed", "name:1")));

            assertThat(store.readOptions).containsExactly(Optional.of(DcbReadOptions.afterPosition(7L)));
        }
    }

    @Nested
    @DisplayName("when returning the append result")
    class When_returning_the_append_result {

        @Test
        void keeps_the_versions_when_the_read_reported_the_same_epoch() {
            var store = new ScriptedStore(Optional.of(versions("epoch-1", 2)), Optional.of(versions("epoch-1", 3)));

            Optional<DcbAppendResult> result = executeWithVersions(store);

            assertThat(result.orElseThrow().markerVersions()).contains(versions("epoch-1", 3));
        }

        @Test
        void drops_the_versions_when_the_read_reported_another_epoch_for_the_key() {
            var store = new ScriptedStore(Optional.of(versions("epoch-1", 2)), Optional.of(versions("epoch-2", 3)));

            Optional<DcbAppendResult> result = executeWithVersions(store);

            assertThat(result.orElseThrow().markerVersions()).isEmpty();
        }

        @Test
        void drops_the_versions_when_the_read_did_not_report_a_key_of_the_append() {
            var store = new ScriptedStore(Optional.of(DcbMarkerVersions.of(Map.of("tag:other:1", new MarkerVersion("epoch-1", 2, 0)))), Optional.of(versions("epoch-1", 3)));

            Optional<DcbAppendResult> result = executeWithVersions(store);

            assertThat(result.orElseThrow().markerVersions()).isEmpty();
        }

        @Test
        void drops_the_versions_when_the_read_reported_none() {
            var store = new ScriptedStore(Optional.empty(), Optional.of(versions("epoch-1", 3)));

            Optional<DcbAppendResult> result = executeWithVersions(store);

            assertThat(result.orElseThrow().markerVersions()).isEmpty();
        }

        @Test
        void keeps_the_versions_when_the_read_reported_more_keys_than_the_append_has() {
            var readVersions = DcbMarkerVersions.of(Map.of(KEY, new MarkerVersion("epoch-1", 2, 0), "tag:other:1", new MarkerVersion("epoch-9", 5, 0)));
            var store = new ScriptedStore(Optional.of(readVersions), Optional.of(versions("epoch-1", 3)));

            Optional<DcbAppendResult> result = executeWithVersions(store);

            assertThat(result.orElseThrow().markerVersions()).contains(versions("epoch-1", 3));
        }

        @Test
        void drops_the_versions_when_they_were_not_requested_even_if_the_store_reports_them() {
            var store = new ScriptedStore(Optional.of(versions("epoch-1", 2)), Optional.of(versions("epoch-1", 3)));

            Optional<DcbAppendResult> result = service(store).execute(CRITERIA, events -> List.of(new DomainEvent("Changed", "name:1")));

            assertThat(result.orElseThrow().markerVersions()).isEmpty();
        }

        @Test
        void the_rest_of_the_result_is_unchanged_when_the_versions_are_dropped() {
            var store = new ScriptedStore(Optional.of(versions("epoch-1", 2)), Optional.of(versions("epoch-2", 3)));

            DcbAppendResult result = executeWithVersions(store).orElseThrow();

            assertAll(
                    () -> assertThat(result.firstSequencePosition()).isEqualTo(store.appended.firstSequencePosition()),
                    () -> assertThat(result.lastSequencePosition()).isEqualTo(store.appended.lastSequencePosition()),
                    () -> assertThat(result.eventCount()).isEqualTo(store.appended.eventCount()),
                    () -> assertThat(result.appendId()).isEqualTo(store.appended.appendId())
            );
        }
    }

    @Nested
    @DisplayName("when asked whether an append is confirmed")
    class When_asked_whether_an_append_is_confirmed {

        @Test
        void asks_a_store_that_confirms_appends() {
            var store = new ConfirmingStore(true);

            boolean confirmed = service(store).isAppendConfirmed(store.appended);

            assertAll(
                    () -> assertThat(confirmed).isTrue(),
                    () -> assertThat(store.asked).containsExactly(store.appended)
            );
        }

        @Test
        void returns_what_a_store_that_does_not_confirm_the_append_answers() {
            var store = new ConfirmingStore(false);

            assertThat(service(store).isAppendConfirmed(store.appended)).isFalse();
        }

        @Test
        void is_not_confirmed_when_the_store_cannot_confirm_appends() {
            var store = new ScriptedStore(Optional.empty(), Optional.empty());

            assertThat(service(store).isAppendConfirmed(store.appended)).isFalse();
        }
    }

    @Nested
    @DisplayName("when the store refuses the read")
    class When_the_store_refuses_the_read {

        @Test
        void the_refusal_reaches_the_caller_without_the_transaction_being_entered() {
            var store = new ScriptedStore(Optional.empty(), Optional.empty()).refusingTheFirstReads(1);
            var executor = new RecordingExecutor();
            var dispatcher = new RecordingDispatcher();
            var domainFunctionRan = new AtomicBoolean();

            var options = DcbExecuteOptions.<DomainEvent>options().fromVersions(versions("epoch-1", 2), 3);
            Function<List<DomainEvent>, List<DomainEvent>> domainFunction = events -> {
                domainFunctionRan.set(true);
                return List.of(new DomainEvent("Changed", "name:1"));
            };
            Function<List<DomainEvent>, List<DomainEvent>> anotherFunction = events -> List.of();
            Throwable thrown = catchThrowable(() -> transactionalService(store, executor, dispatcher).execute(CRITERIA, options, domainFunction));

            assertAll(
                    () -> assertThat(thrown).as("what reached the caller").isInstanceOfSatisfying(DcbSnapshotInvalidException.class, refused -> assertAll(
                            () -> assertThat(refused.getCause()).as("the store's refusal").isSameAs(store.refusal),
                            () -> assertThat(refused.isBeforeAppendOf(domainFunction)).as("marked for the function it was given").isTrue(),
                            () -> assertThat(refused.isBeforeAppendOf(anotherFunction)).as("marked for another function").isFalse(),
                            () -> assertThat(refused.isBeforeAppendOf(options)).as("marked for the options it was given").isFalse())),
                    () -> assertThat(executor.entered).as("transactions entered").isZero(),
                    () -> assertThat(domainFunctionRan).as("domain function ran").isFalse(),
                    () -> assertThat(store.appendCalls).as("appends").isZero(),
                    () -> assertThat(dispatcher.dispatched).as("dispatched events").isEmpty()
            );
        }

        @Test
        void the_refusal_reaches_the_caller_without_a_transaction_when_there_are_no_synchronous_subscriptions() {
            var store = new ScriptedStore(Optional.empty(), Optional.empty()).refusingTheFirstReads(1);

            var options = DcbExecuteOptions.<DomainEvent>options().fromVersions(versions("epoch-1", 2), 3);
            Function<List<DomainEvent>, List<DomainEvent>> domainFunction = events -> List.of(new DomainEvent("Changed", "name:1"));
            Function<List<DomainEvent>, List<DomainEvent>> anotherFunction = events -> List.of();
            Throwable thrown = catchThrowable(() -> service(store).execute(CRITERIA, options, domainFunction));

            assertAll(
                    () -> assertThat(thrown).as("what reached the caller").isInstanceOfSatisfying(DcbSnapshotInvalidException.class, refused -> assertAll(
                            () -> assertThat(refused.getCause()).as("the store's refusal").isSameAs(store.refusal),
                            () -> assertThat(refused.isBeforeAppendOf(domainFunction)).as("marked for the function it was given").isTrue(),
                            () -> assertThat(refused.isBeforeAppendOf(anotherFunction)).as("marked for another function").isFalse(),
                            () -> assertThat(refused.isBeforeAppendOf(options)).as("marked for the options it was given").isFalse())),
                    () -> assertThat(store.readOptions).as("reads, since a refusal isn't retried").hasSize(1),
                    () -> assertThat(store.appendCalls).as("appends").isZero()
            );
        }

        @Test
        void a_refusal_thrown_by_the_domain_function_reaches_the_caller_without_the_transaction_being_entered() {
            var store = new ScriptedStore(Optional.empty(), Optional.empty());
            var executor = new RecordingExecutor();
            var refusal = new DcbSnapshotInvalidException(CRITERIA, versions("epoch-1", 2), "refused by the domain function");

            Throwable thrown = catchThrowable(() -> transactionalService(store, executor, new RecordingDispatcher()).execute(CRITERIA, events -> {
                throw refusal;
            }));

            assertAll(
                    () -> assertThat(thrown).as("what reached the caller").isSameAs(refusal),
                    () -> assertThat(executor.entered).as("transactions entered").isZero(),
                    () -> assertThat(store.appendCalls).as("appends").isZero()
            );
        }

        @Test
        void a_refusal_thrown_by_the_synchronous_dispatch_after_the_append_rolls_the_transaction_back() {
            var store = new ScriptedStore(Optional.empty(), Optional.empty());
            var executor = new RecordingExecutor();
            var refusal = new DcbSnapshotInvalidException(CRITERIA, versions("epoch-1", 2), "refused by a handler");
            var dispatcher = new RecordingDispatcher();
            dispatcher.failure = refusal;

            Throwable thrown = catchThrowable(() -> transactionalService(store, executor, dispatcher).execute(CRITERIA, events -> List.of(new DomainEvent("Changed", "name:1"))));

            assertAll(
                    () -> assertThat(thrown).as("what reached the caller").isSameAs(refusal),
                    () -> assertThat(store.appendCalls).as("appends made before the rollback").isEqualTo(1),
                    () -> assertThat(executor.threw).as("transactions that saw the exception").isEqualTo(1),
                    () -> assertThat(executor.returned).as("transactions that returned normally").isZero()
            );
        }

        @Test
        void a_refusal_marked_by_another_service_and_thrown_by_the_synchronous_dispatch_reaches_the_caller_as_it_is_and_not_marked_for_this_execute_and_rolls_the_transaction_back() {
            var store = new ScriptedStore(Optional.empty(), Optional.empty());
            var executor = new RecordingExecutor();
            Function<List<DomainEvent>, List<DomainEvent>> anotherServicesFunction = events -> List.of();
            var markedByAnotherService = new DcbSnapshotInvalidException(CRITERIA, versions("epoch-1", 2), "refused by another service").beforeAppend(anotherServicesFunction);
            Function<List<DomainEvent>, List<DomainEvent>> thisExecutesFunction = events -> List.of(new DomainEvent("Changed", "name:1"));
            var dispatcher = new RecordingDispatcher();
            dispatcher.failure = markedByAnotherService;

            Throwable thrown = catchThrowable(() -> transactionalService(store, executor, dispatcher).execute(CRITERIA, thisExecutesFunction));

            assertAll(
                    () -> assertThat(thrown).as("what reached the caller").isSameAs(markedByAnotherService),
                    () -> assertThat(markedByAnotherService.isBeforeAppendOf(thisExecutesFunction)).as("marked for this execute").isFalse(),
                    () -> assertThat(markedByAnotherService.isBeforeAppendOf(anotherServicesFunction)).as("marked for the other service's execute").isTrue(),
                    () -> assertThat(store.appendCalls).as("appends made before the rollback").isEqualTo(1),
                    () -> assertThat(executor.threw).as("transactions that saw the exception").isEqualTo(1),
                    () -> assertThat(executor.returned).as("transactions that returned normally").isZero()
            );
        }

        @Test
        void the_refusal_reaches_the_caller_when_the_executor_would_have_run_its_action_twice() {
            var store = new ScriptedStore(Optional.empty(), Optional.empty()).refusingTheFirstReads(1);
            var service = GenericDcbApplicationService.builder(store, converter())
                    .tagGenerator(event -> Set.of(Tag.parse(event.name())))
                    .synchronousSubscriptions(new RecordingDispatcher())
                    .transactionExecutor(new TransactionExecutor() {
                        @Override
                        public <T> T inTransaction(Supplier<T> action) {
                            action.get();
                            return action.get();
                        }
                    })
                    .build();

            var options = DcbExecuteOptions.<DomainEvent>options().fromVersions(versions("epoch-1", 2), 3);
            Throwable thrown = catchThrowable(() -> service.execute(CRITERIA, options, events -> List.of(new DomainEvent("Changed", "name:1"))));

            assertAll(
                    () -> assertThat(thrown).as("what reached the caller").isInstanceOfSatisfying(DcbSnapshotInvalidException.class, refused -> assertAll(() -> assertThat(refused.getCause()).as("the store's refusal").isSameAs(store.refusal), () -> assertThat(refused.isBeforeAppendOf(options)).as("marked as before the append").isTrue())),
                    () -> assertThat(store.readOptions).as("reads, since a refusal isn't retried").hasSize(1),
                    () -> assertThat(store.appendCalls).as("appends").isZero()
            );
        }
    }

    @Nested
    @DisplayName("when synchronous subscriptions need a transaction")
    class When_synchronous_subscriptions_need_a_transaction {

        private final List<String> trace = new ArrayList<>();
        private final RecordingDispatcher dispatcher = new RecordingDispatcher(trace);

        @Test
        void the_read_and_the_domain_function_run_outside_the_transaction_and_the_append_and_dispatch_inside_it() {
            var store = new TracingStore(trace);

            transactionalService(store, new TracingExecutor(trace), dispatcher).execute(CRITERIA, events -> decide());

            assertThat(trace).containsExactly("read", "decide", "begin", "append", "read", "dispatch", "commit");
        }

        @Test
        void a_no_op_decision_never_enters_the_executor() {
            var store = new TracingStore(trace);
            var executor = new TracingExecutor(trace);

            Optional<DcbAppendResult> result = transactionalService(store, executor, dispatcher).execute(CRITERIA, events -> {
                trace.add("decide");
                return List.of();
            });

            assertAll(
                    () -> assertThat(result).as("append result").isEmpty(),
                    () -> assertThat(executor.entered).as("transactions entered").isZero(),
                    () -> assertThat(trace).containsExactly("read", "decide")
            );
        }

        @Test
        void an_executor_that_runs_its_action_again_repeats_only_the_append_with_the_same_token() {
            var store = new TracingStore(trace).conflictingOnTheFirstAppends(1);
            var executor = new TracingExecutor(trace).runningTheActionAgainAfterAConflict();
            var decisions = new AtomicInteger();

            transactionalService(store, executor, dispatcher).execute(CRITERIA, events -> {
                decisions.incrementAndGet();
                return decide();
            });

            DcbAppendCondition sameCondition = DcbAppendCondition.failIfEventsMatch(CRITERIA, DcbConsistencyToken.of(1));
            assertAll(
                    () -> assertThat(decisions).as("decisions").hasValue(1),
                    () -> assertThat(store.conditions).as("append conditions").containsExactly(sameCondition, sameCondition),
                    () -> assertThat(trace).containsExactly("read", "decide", "begin", "append", "append", "read", "dispatch", "commit")
            );
        }

        @Test
        void a_condition_failure_from_the_append_reaches_the_outer_retry_which_reads_and_decides_again() {
            var store = new TracingStore(trace).conflictingOnTheFirstAppends(1);
            var decisions = new AtomicInteger();

            transactionalService(store, new TracingExecutor(trace), dispatcher).execute(CRITERIA, events -> {
                decisions.incrementAndGet();
                return decide();
            });

            assertAll(
                    () -> assertThat(decisions).as("decisions").hasValue(2),
                    () -> assertThat(store.conditions).as("append conditions").containsExactly(
                            DcbAppendCondition.failIfEventsMatch(CRITERIA, DcbConsistencyToken.of(1)),
                            DcbAppendCondition.failIfEventsMatch(CRITERIA, DcbConsistencyToken.of(2))),
                    () -> assertThat(trace).containsExactly(
                            "read", "decide", "begin", "append", "rollback",
                            "read", "decide", "begin", "append", "read", "dispatch", "commit")
            );
        }

        private List<DomainEvent> decide() {
            trace.add("decide");
            return List.of(new DomainEvent("Changed", "name:1"));
        }
    }

    @Nested
    @DisplayName("when the read returns an event below the highest folded position")
    class When_the_read_returns_an_event_below_the_highest_folded_position {

        @Test
        void the_read_is_refused_before_the_domain_function_runs() {
            var store = new ScriptedStore(Optional.empty(), Optional.empty()).returningOnRead(eventAt(41));
            var domainFunctionRan = new AtomicBoolean();

            Throwable thrown = catchThrowable(() -> service(store).execute(CRITERIA, DcbExecuteOptions.<DomainEvent>options().fromVersions(versions("epoch-1", 2), 42), events -> {
                domainFunctionRan.set(true);
                return List.of(new DomainEvent("Changed", "name:1"));
            }));

            assertAll(
                    () -> assertThat(thrown).isInstanceOf(DcbSnapshotInvalidException.class).hasMessageContaining("below position 42"),
                    () -> assertThat(domainFunctionRan).as("domain function ran").isFalse(),
                    () -> assertThat(store.appendCalls).as("appends").isZero()
            );
        }

        @Test
        void the_refusal_is_not_retried() {
            var store = new ScriptedStore(Optional.empty(), Optional.empty()).returningOnRead(eventAt(41));

            catchThrowable(() -> service(store).execute(CRITERIA, DcbExecuteOptions.<DomainEvent>options().fromVersions(versions("epoch-1", 2), 42), events -> List.of(new DomainEvent("Changed", "name:1"))));

            assertThat(store.readOptions).as("reads").hasSize(1);
        }

        @Test
        void the_domain_function_sees_the_events_when_all_of_them_are_above_the_highest_folded_position() {
            var store = new ScriptedStore(Optional.empty(), Optional.empty()).returningOnRead(eventAt(43));
            var seen = new ArrayList<DomainEvent>();

            service(store).execute(CRITERIA, DcbExecuteOptions.<DomainEvent>options().fromVersions(versions("epoch-1", 2), 42), events -> {
                seen.addAll(events);
                return List.of(new DomainEvent("Changed", "name:1"));
            });

            assertAll(
                    () -> assertThat(seen).as("events seen by the domain function").hasSize(1),
                    () -> assertThat(store.appendCalls).as("appends").isEqualTo(1)
            );
        }

        @Test
        void a_read_of_the_whole_boundary_is_never_refused_for_an_event_below_a_position() {
            var store = new ScriptedStore(Optional.empty(), Optional.empty()).returningOnRead(eventAt(1));

            service(store).execute(CRITERIA, DcbExecuteOptions.<DomainEvent>options().withVersions(), events -> List.of(new DomainEvent("Changed", "name:1")));

            assertThat(store.appendCalls).as("appends").isEqualTo(1);
        }
    }

    private static Optional<DcbAppendResult> executeWithVersions(DcbEventStore store) {
        return service(store).execute(CRITERIA, DcbExecuteOptions.<DomainEvent>options().withVersions(), events -> List.of(new DomainEvent("Changed", "name:1")));
    }

    private static GenericDcbApplicationService<DomainEvent> transactionalService(DcbEventStore store, TransactionExecutor executor, SynchronousEventDispatcher dispatcher) {
        return GenericDcbApplicationService.builder(store, converter())
                .tagGenerator(event -> Set.of(Tag.parse(event.name())))
                .synchronousSubscriptions(dispatcher)
                .transactionExecutor(executor)
                .build();
    }

    private static GenericDcbApplicationService<DomainEvent> service(DcbEventStore store) {
        return new GenericDcbApplicationService<>(store, converter(), event -> Set.of(Tag.parse(event.name())), GenericDcbApplicationService.defaultRetryStrategy());
    }

    private static CloudEvent eventAt(long position) {
        return OccurrentCloudEventExtension.withPosition(converter().toCloudEvent(new DomainEvent("Changed", "name:1")), position);
    }

    private static DcbMarkerVersions versions(String epoch, long version) {
        return DcbMarkerVersions.of(Map.of(KEY, new MarkerVersion(epoch, version, 0)));
    }

    private static CloudEventConverter<DomainEvent> converter() {
        return new CloudEventConverter<>() {
            @Override
            public CloudEvent toCloudEvent(DomainEvent domainEvent) {
                return CloudEventBuilder.v1()
                        .withId(UUID.randomUUID().toString())
                        .withSource(URI.create("urn:test"))
                        .withType(domainEvent.type())
                        .withData(domainEvent.name().getBytes(UTF_8))
                        .build();
            }

            @Override
            public DomainEvent toDomainEvent(CloudEvent cloudEvent) {
                return new DomainEvent(cloudEvent.getType(), new String(cloudEvent.getData().toBytes(), UTF_8));
            }

            @Override
            public String getCloudEventType(Class<? extends DomainEvent> type) {
                return type.getName();
            }
        };
    }

    private record DomainEvent(String type, String name) {
    }

    /**
     * Reports the versions it was given on a read and on an append, whatever the read asked for.
     */
    private static class ScriptedStore implements DcbEventStore {
        final List<Optional<DcbReadOptions>> readOptions = new ArrayList<>();
        final DcbAppendResult appended;
        final DcbSnapshotInvalidException refusal = new DcbSnapshotInvalidException(CRITERIA, versions("epoch-1", 2), "refused (test double)");
        int appendCalls;
        private int refusedReads;
        private List<CloudEvent> readEvents = List.of();
        private final Optional<DcbMarkerVersions> readVersions;

        ScriptedStore(Optional<DcbMarkerVersions> readVersions, Optional<DcbMarkerVersions> appendVersions) {
            this.readVersions = readVersions;
            this.appended = new DcbAppendResult(4, 4, 1, Optional.of(new AppendId(UUID.randomUUID())), appendVersions);
        }

        ScriptedStore returningOnRead(CloudEvent... events) {
            this.readEvents = List.of(events);
            return this;
        }

        ScriptedStore refusingTheFirstReads(int reads) {
            this.refusedReads = reads;
            return this;
        }

        @Override
        public DcbEventStream read(DcbCriteria criteria, DcbReadOptions options) {
            readOptions.add(Optional.of(options));
            if (refusedReads > 0) {
                refusedReads--;
                throw refusal;
            }
            return stream(criteria);
        }

        @Override
        public DcbEventStream read(DcbCriteria criteria) {
            readOptions.add(Optional.empty());
            return stream(criteria);
        }

        DcbEventStream stream(DcbCriteria criteria) {
            return new DcbEventStream(readEvents, 3, DcbConsistencyToken.of(criteria, 3), readVersions);
        }

        @Override
        public DcbAppendResult append(List<CloudEvent> events) {
            appendCalls++;
            return appended;
        }

        @Override
        public DcbAppendResult append(List<CloudEvent> events, DcbAppendCondition condition) {
            appendCalls++;
            return appended;
        }
    }

    /**
     * Counts how each run of the action ended, as a transaction manager sees it.
     */
    private static final class RecordingExecutor implements TransactionExecutor {
        int entered;
        int returned;
        int threw;

        @Override
        public <T> T inTransaction(Supplier<T> action) {
            entered++;
            try {
                T result = action.get();
                returned++;
                return result;
            } catch (RuntimeException e) {
                threw++;
                throw e;
            }
        }
    }

    private static final class RecordingDispatcher implements SynchronousEventDispatcher {
        final List<CloudEvent> dispatched = new ArrayList<>();
        @Nullable RuntimeException failure;
        private final @Nullable List<String> trace;

        RecordingDispatcher() {
            this(null);
        }

        RecordingDispatcher(@Nullable List<String> trace) {
            this.trace = trace;
        }

        @Override
        public void dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
            if (trace != null) {
                trace.add("dispatch");
            }
            if (failure != null) {
                throw failure;
            }
            dispatched.addAll(writtenCloudEvents);
        }

        @Override
        public boolean hasSubscriptions() {
            return true;
        }
    }

    /**
     * Notes each read and append in the order they happen, and hands every read its own consistency token.
     */
    private static final class TracingStore extends ScriptedStore {
        final List<DcbAppendCondition> conditions = new ArrayList<>();
        private final List<String> trace;
        private int conflicts;
        private int reads;

        TracingStore(List<String> trace) {
            super(Optional.empty(), Optional.empty());
            this.trace = trace;
        }

        TracingStore conflictingOnTheFirstAppends(int appends) {
            this.conflicts = appends;
            return this;
        }

        @Override
        public DcbEventStream read(DcbCriteria criteria, DcbReadOptions options) {
            trace.add("read");
            return super.read(criteria, options);
        }

        @Override
        public DcbEventStream read(DcbCriteria criteria) {
            trace.add("read");
            return super.read(criteria);
        }

        @Override
        DcbEventStream stream() {
            return new DcbEventStream(List.of(), 3, DcbConsistencyToken.of(++reads), Optional.empty());
        }

        @Override
        public DcbAppendResult append(List<CloudEvent> events, DcbAppendCondition condition) {
            trace.add("append");
            conditions.add(condition);
            if (conflicts > 0) {
                conflicts--;
                throw new DcbAppendConditionNotFulfilledException(condition, 3);
            }
            return super.append(events, condition);
        }
    }

    /**
     * Notes where a transaction begins and how it ends. Like an executor that owns its transaction, it can run the action
     * again on a conflict.
     */
    private static final class TracingExecutor implements TransactionExecutor {
        int entered;
        private final List<String> trace;
        private boolean runAgainAfterAConflict;

        TracingExecutor(List<String> trace) {
            this.trace = trace;
        }

        TracingExecutor runningTheActionAgainAfterAConflict() {
            this.runAgainAfterAConflict = true;
            return this;
        }

        @Override
        public <T> T inTransaction(Supplier<T> action) {
            entered++;
            trace.add("begin");
            try {
                T result;
                try {
                    result = action.get();
                } catch (DcbAppendConditionNotFulfilledException conflict) {
                    if (!runAgainAfterAConflict) {
                        throw conflict;
                    }
                    result = action.get();
                }
                trace.add("commit");
                return result;
            } catch (RuntimeException e) {
                trace.add("rollback");
                throw e;
            }
        }
    }

    private static final class ConfirmingStore extends ScriptedStore implements DcbAppendConfirmation {
        final List<DcbAppendResult> asked = new ArrayList<>();
        private final boolean answer;

        ConfirmingStore(boolean answer) {
            super(Optional.empty(), Optional.empty());
            this.answer = answer;
        }

        @Override
        public boolean isConfirmed(DcbAppendResult appendResult) {
            asked.add(appendResult);
            return answer;
        }
    }
}
