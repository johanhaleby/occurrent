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

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.service.reactor.ReactiveSynchronousEventDispatcher;
import org.occurrent.application.service.reactor.ReactiveTransactionExecutor;
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.eventstore.api.dcb.*;
import org.occurrent.eventstore.api.dcb.reactor.DcbAppendConfirmation;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import reactor.core.publisher.Mono;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions.MarkerVersion;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
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

            service(store).execute(CRITERIA, events -> List.of(new DomainEvent("Changed", "name:1"))).blockOptional();

            assertThat(store.readOptions).containsExactly(Optional.empty());
        }

        @Test
        void withVersions_reads_the_whole_boundary_and_asks_for_versions() {
            var store = new ScriptedStore(Optional.empty(), Optional.empty());

            service(store).execute(CRITERIA, DcbExecuteOptions.<DomainEvent>options().withVersions(), events -> List.of(new DomainEvent("Changed", "name:1"))).block();

            assertThat(store.readOptions).containsExactly(Optional.of(DcbReadOptions.fromBeginning().withVersions()));
        }

        @Test
        void fromVersions_reads_after_the_given_versions() {
            var store = new ScriptedStore(Optional.empty(), Optional.empty());
            DcbMarkerVersions saved = versions("epoch-1", 2);

            service(store).execute(CRITERIA, DcbExecuteOptions.<DomainEvent>options().fromVersions(saved), events -> List.of(new DomainEvent("Changed", "name:1"))).block();

            assertThat(store.readOptions).containsExactly(Optional.of(DcbReadOptions.afterVersions(saved)));
        }

        @Test
        @SuppressWarnings("removal")
        void fromPosition_still_reads_after_the_position() {
            var store = new ScriptedStore(Optional.empty(), Optional.empty());

            service(store).execute(CRITERIA, DcbExecuteOptions.<DomainEvent>options().fromPosition(7L), events -> List.of(new DomainEvent("Changed", "name:1"))).block();

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

            Optional<DcbAppendResult> result = service(store).execute(CRITERIA, events -> List.of(new DomainEvent("Changed", "name:1"))).blockOptional();

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

            boolean confirmed = Boolean.TRUE.equals(service(store).isAppendConfirmed(store.appended).block());

            assertAll(
                    () -> assertThat(confirmed).isTrue(),
                    () -> assertThat(store.asked).containsExactly(store.appended)
            );
        }

        @Test
        void returns_what_a_store_that_does_not_confirm_the_append_answers() {
            var store = new ConfirmingStore(false);

            assertThat(service(store).isAppendConfirmed(store.appended).block()).isFalse();
        }

        @Test
        void is_not_confirmed_when_the_store_cannot_confirm_appends() {
            var store = new ScriptedStore(Optional.empty(), Optional.empty());

            assertThat(service(store).isAppendConfirmed(store.appended).block()).isFalse();
        }
    }

    @Nested
    @DisplayName("when the store refuses the read")
    class When_the_store_refuses_the_read {

        @Test
        void the_refusal_reaches_the_caller_after_the_transaction_completed_normally() {
            var store = new ScriptedStore(Optional.empty(), Optional.empty()).refusingTheFirstReads(1);
            var executor = new RecordingExecutor();
            var dispatcher = new RecordingDispatcher();
            var domainFunctionRan = new AtomicBoolean();

            Throwable thrown = catchThrowable(() -> transactionalService(store, executor, dispatcher).execute(CRITERIA, DcbExecuteOptions.<DomainEvent>options().fromVersions(versions("epoch-1", 2)), events -> {
                domainFunctionRan.set(true);
                return List.of(new DomainEvent("Changed", "name:1"));
            }).block());

            assertAll(
                    () -> assertThat(thrown).as("what reached the caller").isSameAs(store.refusal),
                    () -> assertThat(executor.completed).as("transactions that completed normally").isEqualTo(1),
                    () -> assertThat(executor.failed).as("transactions that saw an error").isZero(),
                    () -> assertThat(domainFunctionRan).as("domain function ran").isFalse(),
                    () -> assertThat(store.appendCalls).as("appends").isZero(),
                    () -> assertThat(dispatcher.dispatched).as("dispatched events").isEmpty()
            );
        }

        @Test
        void the_refusal_reaches_the_caller_without_a_transaction_when_there_are_no_synchronous_subscriptions() {
            var store = new ScriptedStore(Optional.empty(), Optional.empty()).refusingTheFirstReads(1);

            Throwable thrown = catchThrowable(() -> service(store).execute(CRITERIA, DcbExecuteOptions.<DomainEvent>options().fromVersions(versions("epoch-1", 2)), events -> List.of(new DomainEvent("Changed", "name:1"))).block());

            assertAll(
                    () -> assertThat(thrown).as("what reached the caller").isSameAs(store.refusal),
                    () -> assertThat(store.readOptions).as("reads, since a refusal isn't retried").hasSize(1),
                    () -> assertThat(store.appendCalls).as("appends").isZero()
            );
        }

        @Test
        void a_refusal_thrown_by_the_domain_function_rolls_the_transaction_back() {
            var store = new ScriptedStore(Optional.empty(), Optional.empty());
            var executor = new RecordingExecutor();
            var refusal = new DcbSnapshotInvalidException(CRITERIA, versions("epoch-1", 2), "refused by the domain function");

            Throwable thrown = catchThrowable(() -> transactionalService(store, executor, new RecordingDispatcher()).execute(CRITERIA, events -> {
                throw refusal;
            }).block());

            assertAll(
                    () -> assertThat(thrown).as("what reached the caller").isSameAs(refusal),
                    () -> assertThat(executor.failed).as("transactions that saw the error").isEqualTo(1),
                    () -> assertThat(executor.completed).as("transactions that completed normally").isZero()
            );
        }

        @Test
        void a_refusal_thrown_by_the_synchronous_dispatch_after_the_append_rolls_the_transaction_back() {
            var store = new ScriptedStore(Optional.empty(), Optional.empty());
            var executor = new RecordingExecutor();
            var refusal = new DcbSnapshotInvalidException(CRITERIA, versions("epoch-1", 2), "refused by a handler");
            var dispatcher = new RecordingDispatcher();
            dispatcher.failure = refusal;

            Throwable thrown = catchThrowable(() -> transactionalService(store, executor, dispatcher).execute(CRITERIA, events -> List.of(new DomainEvent("Changed", "name:1"))).block());

            assertAll(
                    () -> assertThat(thrown).as("what reached the caller").isSameAs(refusal),
                    () -> assertThat(store.appendCalls).as("appends made before the rollback").isEqualTo(1),
                    () -> assertThat(executor.failed).as("transactions that saw the error").isEqualTo(1),
                    () -> assertThat(executor.completed).as("transactions that completed normally").isZero()
            );
        }

        @Test
        void a_refusal_from_an_earlier_run_of_the_action_is_not_thrown_when_the_executor_runs_it_again() {
            var store = new ScriptedStore(Optional.empty(), Optional.empty()).refusingTheFirstReads(1);
            var service = GenericDcbApplicationService.builder(store, converter())
                    .tagGenerator(event -> Set.of(Tag.parse(event.name())))
                    .synchronousSubscriptions(new RecordingDispatcher())
                    .transactionExecutor(new ReactiveTransactionExecutor() {
                        @Override
                        public <T> Mono<T> inTransaction(Supplier<Mono<T>> action) {
                            return Mono.defer(action).then(Mono.defer(action));
                        }
                    })
                    .build();

            Throwable thrown = catchThrowable(() -> service.execute(CRITERIA, DcbExecuteOptions.<DomainEvent>options().fromVersions(versions("epoch-1", 2)), events -> List.of()).block());

            assertAll(
                    () -> assertThat(thrown).as("what reached the caller").isNull(),
                    () -> assertThat(store.readOptions).as("reads, one for each run of the action").hasSize(2)
            );
        }

        @Test
        void each_subscription_to_the_returned_mono_keeps_its_own_refusal() {
            var store = new ScriptedStore(Optional.empty(), Optional.empty()).refusingTheFirstReads(1);
            var execution = transactionalService(store, new RecordingExecutor(), new RecordingDispatcher())
                    .execute(CRITERIA, DcbExecuteOptions.<DomainEvent>options().fromVersions(versions("epoch-1", 2)), events -> List.of());

            Throwable first = catchThrowable(execution::block);
            Throwable second = catchThrowable(execution::block);

            assertAll(
                    () -> assertThat(first).as("first subscription").isSameAs(store.refusal),
                    () -> assertThat(second).as("second subscription, whose domain function has nothing to append").isNull(),
                    () -> assertThat(store.readOptions).as("reads, one for each subscription").hasSize(2)
            );
        }
    }

    private static Optional<DcbAppendResult> executeWithVersions(DcbEventStore store) {
        return service(store).execute(CRITERIA, DcbExecuteOptions.<DomainEvent>options().withVersions(), events -> List.of(new DomainEvent("Changed", "name:1"))).blockOptional();
    }

    private static GenericDcbApplicationService<DomainEvent> transactionalService(DcbEventStore store, ReactiveTransactionExecutor executor, ReactiveSynchronousEventDispatcher dispatcher) {
        return GenericDcbApplicationService.builder(store, converter())
                .tagGenerator(event -> Set.of(Tag.parse(event.name())))
                .synchronousSubscriptions(dispatcher)
                .transactionExecutor(executor)
                .build();
    }

    private static GenericDcbApplicationService<DomainEvent> service(DcbEventStore store) {
        return new GenericDcbApplicationService<>(store, converter(), event -> Set.of(Tag.parse(event.name())));
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
        private final Optional<DcbMarkerVersions> readVersions;

        ScriptedStore(Optional<DcbMarkerVersions> readVersions, Optional<DcbMarkerVersions> appendVersions) {
            this.readVersions = readVersions;
            this.appended = new DcbAppendResult(4, 4, 1, Optional.of(new AppendId(UUID.randomUUID())), appendVersions);
        }

        ScriptedStore refusingTheFirstReads(int reads) {
            this.refusedReads = reads;
            return this;
        }

        @Override
        public Mono<DcbEventStream> read(DcbCriteria criteria, DcbReadOptions options) {
            readOptions.add(Optional.of(options));
            if (refusedReads > 0) {
                refusedReads--;
                return Mono.error(refusal);
            }
            return Mono.just(stream());
        }

        @Override
        public Mono<DcbEventStream> read(DcbCriteria criteria) {
            readOptions.add(Optional.empty());
            return Mono.just(stream());
        }

        private DcbEventStream stream() {
            return new DcbEventStream(List.of(), 3, DcbConsistencyToken.of(3), readVersions);
        }

        @Override
        public Mono<DcbAppendResult> append(List<CloudEvent> events) {
            appendCalls++;
            return Mono.just(appended);
        }

        @Override
        public Mono<DcbAppendResult> append(List<CloudEvent> events, DcbAppendCondition condition) {
            appendCalls++;
            return Mono.just(appended);
        }
    }

    /**
     * Counts how each run of the action ended, as a transaction manager sees it.
     */
    private static final class RecordingExecutor implements ReactiveTransactionExecutor {
        int completed;
        int failed;

        @Override
        public <T> Mono<T> inTransaction(Supplier<Mono<T>> action) {
            return Mono.defer(action).doOnSuccess(result -> completed++).doOnError(error -> failed++);
        }
    }

    private static final class RecordingDispatcher implements ReactiveSynchronousEventDispatcher {
        final List<CloudEvent> dispatched = new ArrayList<>();
        @Nullable RuntimeException failure;

        @Override
        public Mono<Void> dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
            if (failure != null) {
                return Mono.error(failure);
            }
            dispatched.addAll(writtenCloudEvents);
            return Mono.empty();
        }

        @Override
        public boolean hasSubscriptions() {
            return true;
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
        public Mono<Boolean> isConfirmed(DcbAppendResult appendResult) {
            asked.add(appendResult);
            return Mono.just(answer);
        }
    }
}
