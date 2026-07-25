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

import org.occurrent.dsl.saga.mongodb.spring.SpringMongoSagaStateStore;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.SagaInput;
import org.occurrent.dsl.saga.flow.Continuation;
import org.occurrent.dsl.saga.flow.FlowSaga;
import org.occurrent.dsl.saga.flow.FlowState;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoOperations;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Verifies the MongoDB-backed {@link SpringMongoSagaStateStore}: state round-trips, compare-and-set enforces the version,
 * and the due-timer query returns only active instances with a due timer. Docker-based.
 */
@DisplayName("SpringMongoSagaStateStore")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = SpringMongoSagaStateStoreMongoTest.StoreApplication.class,
        properties = "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:saga-store-test"
)
@Import(SpringMongoSagaStateStoreMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class SpringMongoSagaStateStoreMongoTest {

    @Autowired
    private MongoOperations mongoOperations;

    @Autowired
    private CloudEventConverter<FlowEvent> cloudEventConverter;

    private SpringMongoSagaStateStore<Counter> store;

    @BeforeEach
    void setUp() {
        String collection = "saga-store-test-" + System.nanoTime();
        store = new SpringMongoSagaStateStore<>(mongoOperations, collection, Counter.class);
    }

    @Test
    void inserts_a_new_instance_at_version_1_and_rejects_a_second_insert() {
        SagaEnvelope<Counter> envelope = active("s1", new Counter(1), 1, List.of(new TimerEntry("t", 5_000)), 0);

        assertThat(store.compareAndSave("s1", envelope, 0)).isTrue();
        assertThat(store.compareAndSave("s1", active("s1", new Counter(9), 1, List.of(), 0), 0)).isFalse();
    }

    @Test
    void round_trips_state_timers_and_watermarks() {
        SagaEnvelope<Counter> envelope = new SagaEnvelope<>("s2", new Counter(42), SagaStatus.ACTIVE, 1,
                List.of(new TimerEntry("payment", 7_000)), Map.of("stream-a", 5L), 11L,
                Instant.ofEpochMilli(1), Instant.ofEpochMilli(2), null);
        store.compareAndSave("s2", envelope, 0);

        Optional<SagaEnvelope<Counter>> found = store.find("s2");

        assertThat(found).hasValueSatisfying(e -> {
            assertThat(e.sagaId()).isEqualTo("s2");
            assertThat(e.state()).isEqualTo(new Counter(42));
            assertThat(e.status()).isEqualTo(SagaStatus.ACTIVE);
            assertThat(e.version()).isEqualTo(1);
            assertThat(e.timers()).containsExactly(new TimerEntry("payment", 7_000));
            assertThat(e.streamWatermarks()).containsEntry("stream-a", 5L);
            assertThat(e.positionWatermark()).isEqualTo(11L);
        });
    }

    @Test
    void updates_only_when_the_expected_version_matches() {
        store.compareAndSave("s3", active("s3", new Counter(1), 1, List.of(), 0), 0);

        assertThat(store.compareAndSave("s3", active("s3", new Counter(2), 2, List.of(), 0), 1)).isTrue();
        // Version is now 2, so an update expecting version 1 loses.
        assertThat(store.compareAndSave("s3", active("s3", new Counter(3), 2, List.of(), 0), 1)).isFalse();
        assertThat(store.find("s3")).hasValueSatisfying(e -> assertThat(e.state()).isEqualTo(new Counter(2)));
    }

    @Test
    void finds_active_instances_with_a_due_timer_and_excludes_completed_and_not_yet_due() {
        store.compareAndSave("due", active("due", new Counter(1), 1, List.of(new TimerEntry("t", 1_000)), 0), 0);
        store.compareAndSave("later", active("later", new Counter(1), 1, List.of(new TimerEntry("t", 10_000)), 0), 0);
        store.compareAndSave("done", completed("done", new Counter(1), 1), 0);

        List<SagaEnvelope<Counter>> due = store.findWithDueTimers(Instant.ofEpochMilli(2_000), 10);

        assertThat(due).extracting(SagaEnvelope::sagaId).containsExactly("due");
    }

    @Test
    void deletes_an_instance() {
        store.compareAndSave("gone", active("gone", new Counter(1), 1, List.of(), 0), 0);

        store.delete("gone");

        assertThat(store.find("gone")).isEmpty();
    }

    @Test
    void round_trips_a_scalar_state_that_is_not_stored_as_a_document() {
        SpringMongoSagaStateStore<String> scalarStore =
                new SpringMongoSagaStateStore<>(mongoOperations, "saga-store-test-scalar-" + System.nanoTime(), String.class);
        SagaEnvelope<String> envelope = new SagaEnvelope<>("scalar-1", "AWAITING_PAYMENT", SagaStatus.ACTIVE, 1,
                List.of(new TimerEntry("payment", 9_000)), Map.of("stream-a", 3L), 7L,
                Instant.ofEpochMilli(1), Instant.ofEpochMilli(2), null);

        scalarStore.compareAndSave("scalar-1", envelope, 0);
        Optional<SagaEnvelope<String>> found = scalarStore.find("scalar-1");

        assertThat(found).hasValueSatisfying(e -> {
            assertThat(e.sagaId()).isEqualTo("scalar-1");
            assertThat(e.state()).isEqualTo("AWAITING_PAYMENT");
            assertThat(e.status()).isEqualTo(SagaStatus.ACTIVE);
            assertThat(e.version()).isEqualTo(1);
            assertThat(e.timers()).containsExactly(new TimerEntry("payment", 9_000));
            assertThat(e.streamWatermarks()).containsEntry("stream-a", 3L);
            assertThat(e.positionWatermark()).isEqualTo(7L);
        });
    }

    @Test
    void finds_active_instances_with_a_due_timer_and_excludes_active_instances_with_no_timers() {
        store.compareAndSave("due", active("due", new Counter(1), 1, List.of(new TimerEntry("t", 1_000)), 0), 0);
        store.compareAndSave("later", active("later", new Counter(1), 1, List.of(new TimerEntry("t", 10_000)), 0), 0);
        store.compareAndSave("no-timers", active("no-timers", new Counter(1), 1, List.of(), 0), 0);
        store.compareAndSave("done", completed("done", new Counter(1), 1), 0);

        List<SagaEnvelope<Counter>> due = store.findWithDueTimers(Instant.ofEpochMilli(2_000), 10);

        assertThat(due).extracting(SagaEnvelope::sagaId).containsExactly("due");
    }

    // --- findByStatus: mirrors InMemorySagaStateStoreTest.FindByStatus so the contract is verified identically against
    // both store implementations. No test-jar dependency exists between saga-dsl/common and saga-dsl/mongodb-spring, so
    // the assertions are duplicated by hand rather than shared through a common base class; see the planned Technology
    // Compatibility Kit (issue #395). ---

    @Test
    void findByStatus_returns_instances_with_the_given_status() {
        store.compareAndSave("active-1", activeAt("active-1", Instant.ofEpochMilli(1_000)), 0);
        store.compareAndSave("completed-1", completed("completed-1", new Counter(1), 1), 0);

        List<SagaEnvelope<Counter>> active = store.findByStatus(SagaStatus.ACTIVE, Instant.ofEpochMilli(2_000), 10);

        assertThat(active).extracting(SagaEnvelope::sagaId).containsExactly("active-1");
    }

    @Test
    void findByStatus_updatedBefore_is_exclusive() {
        store.compareAndSave("exact", activeAt("exact", Instant.ofEpochMilli(5_000)), 0);

        assertAll(
                () -> assertThat(store.findByStatus(SagaStatus.ACTIVE, Instant.ofEpochMilli(5_000), 10)).isEmpty(),
                () -> assertThat(store.findByStatus(SagaStatus.ACTIVE, Instant.ofEpochMilli(5_001), 10))
                        .extracting(SagaEnvelope::sagaId).containsExactly("exact")
        );
    }

    @Test
    void findByStatus_orders_ascending_by_updatedAt_so_the_stalest_instance_is_first() {
        store.compareAndSave("newest", activeAt("newest", Instant.ofEpochMilli(3_000)), 0);
        store.compareAndSave("oldest", activeAt("oldest", Instant.ofEpochMilli(1_000)), 0);
        store.compareAndSave("middle", activeAt("middle", Instant.ofEpochMilli(2_000)), 0);

        List<SagaEnvelope<Counter>> found = store.findByStatus(SagaStatus.ACTIVE, Instant.ofEpochMilli(4_000), 10);

        assertThat(found).extracting(SagaEnvelope::sagaId).containsExactly("oldest", "middle", "newest");
    }

    @Test
    void findByStatus_limit_truncates_after_ordering_to_the_stalest_N_not_an_arbitrary_N() {
        store.compareAndSave("oldest", activeAt("oldest", Instant.ofEpochMilli(1_000)), 0);
        store.compareAndSave("middle", activeAt("middle", Instant.ofEpochMilli(2_000)), 0);
        store.compareAndSave("newest", activeAt("newest", Instant.ofEpochMilli(3_000)), 0);

        List<SagaEnvelope<Counter>> found = store.findByStatus(SagaStatus.ACTIVE, Instant.ofEpochMilli(4_000), 2);

        assertThat(found).extracting(SagaEnvelope::sagaId).containsExactly("oldest", "middle");
    }

    @Test
    void findByStatus_rejects_a_limit_below_1() {
        assertThatThrownBy(() -> store.findByStatus(SagaStatus.ACTIVE, Instant.ofEpochMilli(1_000), 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("limit must be positive");
    }

    @Test
    void findByStatus_excludes_an_envelope_with_a_null_updatedAt() {
        SagaEnvelope<Counter> noUpdatedAt = new SagaEnvelope<>("no-updated-at", new Counter(1), SagaStatus.ACTIVE, 1,
                List.of(), Map.of(), null, Instant.ofEpochMilli(1), null, null);
        store.compareAndSave("no-updated-at", noUpdatedAt, 0);

        assertThat(store.findByStatus(SagaStatus.ACTIVE, Instant.ofEpochMilli(60_000), 10)).isEmpty();
    }

    // --- Lifecycle timestamps: no existing test asserted createdAt/updatedAt/completedAt survive persistence, only that
    // the fold sets them. ---

    @Test
    void createdAt_updatedAt_and_completedAt_round_trip_through_find() {
        Instant createdAt = Instant.ofEpochMilli(1_000);
        Instant updatedAt = Instant.ofEpochMilli(2_000);
        Instant completedAt = Instant.ofEpochMilli(3_000);
        SagaEnvelope<Counter> envelope = new SagaEnvelope<>("lifecycle-1", new Counter(1), SagaStatus.COMPLETED, 1,
                List.of(), Map.of(), null, createdAt, updatedAt, completedAt);
        store.compareAndSave("lifecycle-1", envelope, 0);

        SagaEnvelope<Counter> found = store.find("lifecycle-1").orElseThrow();

        assertAll(
                () -> assertThat(found.createdAt()).isEqualTo(createdAt),
                () -> assertThat(found.updatedAt()).isEqualTo(updatedAt),
                () -> assertThat(found.completedAt()).isEqualTo(completedAt)
        );
    }

    @Test
    void an_active_instance_has_no_completedAt() {
        store.compareAndSave("lifecycle-2", active("lifecycle-2", new Counter(1), 1, List.of(), 0), 0);

        assertThat(store.find("lifecycle-2")).hasValueSatisfying(e -> assertThat(e.completedAt()).isNull());
    }

    @Test
    void the_widened_due_timer_projection_returns_the_lifecycle_timestamps_populated_rather_than_null() {
        Instant createdAt = Instant.ofEpochMilli(10);
        Instant updatedAt = Instant.ofEpochMilli(20);
        SagaEnvelope<Counter> envelope = new SagaEnvelope<>("lifecycle-3", new Counter(1), SagaStatus.ACTIVE, 1,
                List.of(new TimerEntry("t", 1_000)), Map.of(), null, createdAt, updatedAt, null);
        store.compareAndSave("lifecycle-3", envelope, 0);

        SagaEnvelope<Counter> found = store.findWithDueTimers(Instant.ofEpochMilli(2_000), 10).getFirst();

        assertAll(
                () -> assertThat(found.createdAt()).isEqualTo(createdAt),
                () -> assertThat(found.updatedAt()).isEqualTo(updatedAt),
                () -> assertThat(found.completedAt()).isNull()
        );
    }

    // --- currentStep(): populated for a flow saga, null for a saga written against the core builder. ---

    @Test
    void a_core_saga_instance_reports_a_null_currentStep() {
        store.compareAndSave("core-1", active("core-1", new Counter(1), 1, List.of(), 0), 0);

        assertThat(store.find("core-1")).hasValueSatisfying(e -> assertThat(e.currentStep()).isNull());
    }

    @Test
    void the_due_timer_projection_carve_out_means_currentStep_reads_null_on_a_poller_envelope() {
        // Deliberate, not a bug: findWithDueTimers projects away state (see the store's javadoc and
        // the_due_timer_poll_projects_away_the_state_so_it_never_decodes_the_flow_saga_received_log below), so the
        // SagaInstance view of a poller-returned envelope cannot answer currentStep(). The authoritative find(sagaId)
        // still has it.
        String collection = "saga-flowpoll-currentstep-" + System.nanoTime();
        SpringMongoSagaStateStore<FlowState<FlowEvent>> flowStore =
                new SpringMongoSagaStateStore<>(mongoOperations, collection, rawFlowStateType(), cloudEventConverter);
        Saga<FlowEvent, FlowState<FlowEvent>, Object> saga = flowSaga();
        FlowState<FlowEvent> withReceived = saga.evolve(saga.initialState(), SagaInput.event(new FlowStarted("flow-poll-step")));
        SagaEnvelope<FlowState<FlowEvent>> envelope = new SagaEnvelope<>("flow-poll-step", withReceived, SagaStatus.ACTIVE, 1,
                List.of(new TimerEntry("step:started", 1_000)), Map.of(), null, Instant.ofEpochMilli(1), Instant.ofEpochMilli(2), null);
        flowStore.compareAndSave("flow-poll-step", envelope, 0);

        SagaEnvelope<FlowState<FlowEvent>> polled = flowStore.findWithDueTimers(Instant.ofEpochMilli(2_000), 10).getFirst();

        assertAll(
                () -> assertThat(polled.currentStep()).as("state is projected away on the poll, so currentStep() cannot be answered here").isNull(),
                () -> assertThat(flowStore.find("flow-poll-step").orElseThrow().currentStep())
                        .as("the authoritative find(...) still has it").isNotNull()
        );
    }

    private static SagaEnvelope<Counter> activeAt(String id, Instant updatedAt) {
        return new SagaEnvelope<>(id, new Counter(1), SagaStatus.ACTIVE, 1, List.of(), Map.of(), null, updatedAt, updatedAt, null);
    }

    /**
     * A flow saga's {@link FlowState#received()} is a {@code List<E>} with {@code E} erased, the hard serialization case:
     * the elements are heterogeneous domain-event records with no static element type. The store serializes them as
     * CloudEvents through the {@code CloudEventConverter}, so they reconstruct to their concrete record types on read, and
     * their persisted type is the stable CloudEvent type (here the simple name) rather than a Java fully-qualified class
     * name, so a domain event can move to a different package without breaking in-flight flow-saga state.
     */
    @Test
    void round_trips_a_flow_saga_flow_state_by_its_stable_cloud_event_type() {
        String collection = "saga-flowtest-" + System.nanoTime();
        SpringMongoSagaStateStore<FlowState<FlowEvent>> flowStore =
                new SpringMongoSagaStateStore<>(mongoOperations, collection, rawFlowStateType(), cloudEventConverter);

        Saga<FlowEvent, FlowState<FlowEvent>, Object> saga = flowSaga();
        FlowStarted startEvent = new FlowStarted("flow-1");
        FlowState<FlowEvent> afterStart = saga.evolve(saga.initialState(), SagaInput.event(startEvent));
        FlowContinued continuedEvent = new FlowContinued("flow-1");
        FlowState<FlowEvent> finalState = saga.step(afterStart, SagaInput.event(continuedEvent)).state();
        assertThat(finalState.received()).containsExactly(startEvent, continuedEvent);

        SagaEnvelope<FlowState<FlowEvent>> envelope = new SagaEnvelope<>("flow-1", finalState, SagaStatus.COMPLETED, 1,
                List.of(), Map.of(), null, Instant.ofEpochMilli(1), Instant.ofEpochMilli(2), Instant.ofEpochMilli(3));
        flowStore.compareAndSave("flow-1", envelope, 0);

        Optional<SagaEnvelope<FlowState<FlowEvent>>> found = flowStore.find("flow-1");
        assertThat(found).hasValueSatisfying(e -> {
            assertThat(e.state().currentStep()).isEqualTo(finalState.currentStep());
            assertThat(e.state().completed()).isEqualTo(finalState.completed());
            assertThat(e.state().received()).containsExactly(startEvent, continuedEvent);
            // The bookkeeping the store must round-trip lives on the internal FlowStateImpl, not the public FlowState surface.
            FlowStateImpl<FlowEvent> reloaded = (FlowStateImpl<FlowEvent>) e.state();
            FlowStateImpl<FlowEvent> expected = (FlowStateImpl<FlowEvent>) finalState;
            assertThat(reloaded.windowStart()).as("the bounded-window offset round-trips").isEqualTo(expected.windowStart());
            assertThat(reloaded.stepEntryIndex()).as("the absolute step-entry index round-trips").isEqualTo(expected.stepEntryIndex());
        });

        // The received events are stored as CloudEvents keyed by the stable simple type name, not the Java FQN.
        Document raw = mongoOperations.findById("flow-1", Document.class, collection);
        List<String> storedReceived = raw.get("state", Document.class).getList("received", String.class);
        assertThat(storedReceived).hasSize(2);
        assertThat(storedReceived.getFirst()).contains("\"type\":\"FlowStarted\"").doesNotContain(FlowStarted.class.getName());
    }

    @Test
    void the_due_timer_poll_projects_away_the_state_so_it_never_decodes_the_flow_saga_received_log() {
        String collection = "saga-flowpoll-" + System.nanoTime();
        SpringMongoSagaStateStore<FlowState<FlowEvent>> flowStore =
                new SpringMongoSagaStateStore<>(mongoOperations, collection, rawFlowStateType(), cloudEventConverter);

        Saga<FlowEvent, FlowState<FlowEvent>, Object> saga = flowSaga();
        FlowState<FlowEvent> withReceived = saga.evolve(saga.initialState(), SagaInput.event(new FlowStarted("flow-poll")));
        SagaEnvelope<FlowState<FlowEvent>> envelope = new SagaEnvelope<>("flow-poll", withReceived, SagaStatus.ACTIVE, 1,
                List.of(new TimerEntry("step:started", 1_000)), Map.of(), null,
                Instant.ofEpochMilli(1), Instant.ofEpochMilli(2), null);
        flowStore.compareAndSave("flow-poll", envelope, 0);

        List<SagaEnvelope<FlowState<FlowEvent>>> due = flowStore.findWithDueTimers(Instant.ofEpochMilli(2_000), 10);

        assertThat(due).singleElement().satisfies(e -> {
            assertThat(e.sagaId()).isEqualTo("flow-poll");
            assertThat(e.timers()).extracting(TimerEntry::name).containsExactly("step:started");
            assertThat(e.state()).as("the projection excludes state, so the received log is never decoded on a poll").isNull();
        });
        // The authoritative find(...) the executor runs before firing still returns the full received log.
        assertThat(flowStore.find("flow-poll")).hasValueSatisfying(e -> assertThat(e.state().received()).containsExactly(new FlowStarted("flow-poll")));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Class<FlowState<FlowEvent>> rawFlowStateType() {
        return (Class) FlowState.class;
    }

    private static Saga<FlowEvent, FlowState<FlowEvent>, Object> flowSaga() {
        return FlowSaga.<FlowEvent, Object>builder()
                .startsOn(FlowStarted.class, FlowStarted::id)
                .correlate(FlowContinued.class, FlowContinued::id)
                .step("started", step -> step.on(FlowContinued.class, Continuation.end(), c -> List.of()))
                .build();
    }

    sealed interface FlowEvent permits FlowStarted, FlowContinued {
        String id();
    }

    record FlowStarted(String id) implements FlowEvent {
    }

    record FlowContinued(String id) implements FlowEvent {
    }

    private static SagaEnvelope<Counter> active(String id, Counter state, long version, List<TimerEntry> timers, long positionWatermark) {
        return new SagaEnvelope<>(id, state, SagaStatus.ACTIVE, version, timers, Map.of(),
                positionWatermark == 0 ? null : positionWatermark, Instant.ofEpochMilli(1), Instant.ofEpochMilli(1), null);
    }

    private static SagaEnvelope<Counter> completed(String id, Counter state, long version) {
        return new SagaEnvelope<>(id, state, SagaStatus.COMPLETED, version, List.of(), Map.of(), null,
                Instant.ofEpochMilli(1), Instant.ofEpochMilli(1), Instant.ofEpochMilli(1));
    }

    record Counter(int value) {
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class MongoDbContainerConfiguration {
        @Bean
        @ServiceConnection
        MongoDBContainer mongoDbContainer() {
            return new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet();
        }
    }

    @SpringBootApplication
    @EnableOccurrent
    static class StoreApplication {

        // A simple-name type mapper (with an explicit reverse map, since the test events are nested classes) so a stored
        // received event carries the stable simple type "FlowStarted", proving package independence.
        @Bean
        CloudEventTypeMapper<FlowEvent> flowEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.simple(type -> switch (type) {
                case "FlowStarted" -> FlowStarted.class;
                case "FlowContinued" -> FlowContinued.class;
                default -> throw new IllegalArgumentException("Unknown cloud event type " + type);
            });
        }

        @Bean
        CloudEventConverter<FlowEvent> flowEventCloudEventConverter(CloudEventTypeMapper<FlowEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<FlowEvent>(new ObjectMapper(), URI.create("urn:occurrent:saga-store-test"))
                    .typeMapper(typeMapper)
                    .idMapper(event -> event.id() + ":" + event.getClass().getSimpleName())
                    .build();
        }
    }
}
