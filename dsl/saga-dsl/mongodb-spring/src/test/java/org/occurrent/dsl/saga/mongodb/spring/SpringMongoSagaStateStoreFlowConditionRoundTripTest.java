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

package org.occurrent.dsl.saga.mongodb.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.bson.Document;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.flow.FlowState;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl.ActionKind;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based. Round-trips a flow saga's {@link FlowStateImpl} through real MongoDB, checking the persisted-field-compat
 * claim ({@code SpringMongoSagaStateStore}'s class javadoc, and ADR 63's compatibility note on the bookkeeping fields)
 * with evidence rather than reasoning, for an instance parked mid-way through a window condition rather than a join.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class SpringMongoSagaStateStoreFlowConditionRoundTripTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    sealed interface ReviewEvent permits ReviewRequested, Approved {
        String eventId();

        String reviewId();
    }

    record ReviewRequested(String eventId, String reviewId) implements ReviewEvent {
    }

    record Approved(String eventId, String reviewId, int score) implements ReviewEvent {
    }

    private MongoOperations mongoOperations() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-cond-" + UUID.randomUUID()));
        return new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase()));
    }

    // FlowState.class erases its type argument, same as any Class literal. The cast is the same shape
    // SpringMongoSagaStateStoreMongoTest's rawFlowStateType uses for the same constructor.
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Class<FlowState<ReviewEvent>> rawFlowStateType() {
        return (Class) FlowState.class;
    }

    private CloudEventConverter<ReviewEvent> converter() {
        return new JacksonCloudEventConverter.Builder<ReviewEvent>(new ObjectMapper(), URI.create("urn:test"))
                .idMapper(ReviewEvent::eventId).build();
    }

    @Test
    void a_partially_fulfilled_window_conditions_received_log_and_bookkeeping_round_trip_through_mongo() {
        SagaStateStore<FlowState<ReviewEvent>> store =
                new SpringMongoSagaStateStore<>(mongoOperations(), "saga-review", rawFlowStateType(), converter());

        List<ReviewEvent> received = List.of(
                new ReviewRequested("e1", "review-1"),
                new Approved("e2", "review-1", 90));
        // A window condition allOf(event(Approved, 2), ...) partially fulfilled, one Approved so far, one short of the
        // count-2 leaf. currentStep/windowStart/stepEntryIndex/lastAction/matchedBranchIndex are the flow lowering's own
        // bookkeeping (ADR 63's compatibility note), the store round-trips them without interpreting them.
        FlowStateImpl<ReviewEvent> original = new FlowStateImpl<>(
                "awaiting-decision", received, 1, 1, false, null, ActionKind.NONE, -1);
        SagaEnvelope<FlowState<ReviewEvent>> envelope = new SagaEnvelope<>(
                "review-1", original, SagaStatus.ACTIVE, 1, List.of(), Map.of(), null, null, null, null, null);

        boolean inserted = store.compareAndSave("review-1", envelope, 0);
        SagaEnvelope<FlowState<ReviewEvent>> roundTripped = store.find("review-1").orElseThrow();

        assertAll(
                () -> assertThat(inserted).isTrue(),
                () -> assertThat(roundTripped.state()).isEqualTo(original),
                () -> assertThat(roundTripped.state().received()).containsExactlyElementsOf(received),
                () -> assertThat(roundTripped.state().currentStep()).isEqualTo("awaiting-decision"),
                () -> assertThat(roundTripped.state().completed()).isFalse(),
                () -> assertThat(roundTripped.currentStep()).as("the envelope re-derives currentStep from the loaded state").isEqualTo("awaiting-decision")
        );
    }

    @Test
    void the_entry_a_reaction_needs_round_trips_through_mongo() {
        SagaStateStore<FlowState<ReviewEvent>> store =
                new SpringMongoSagaStateStore<>(mongoOperations(), "saga-review", rawFlowStateType(), converter());

        List<ReviewEvent> received = List.of(
                new ReviewRequested("e1", "review-2"),
                new Approved("e2", "review-2", 90));
        // Mid-transition, where previousStepEntryIndex is what react slices the fired window with, so a store that loses it
        // hands a reaction the whole retained history instead of its own step's window.
        FlowStateImpl<ReviewEvent> original = new FlowStateImpl<>(
                "next", received, 1, 2, false, "awaiting-decision", 1, ActionKind.BRANCH, 0);
        SagaEnvelope<FlowState<ReviewEvent>> envelope = new SagaEnvelope<>(
                "review-2", original, SagaStatus.ACTIVE, 1, List.of(), Map.of(), null, null, null, null, null);

        store.compareAndSave("review-2", envelope, 0);
        SagaEnvelope<FlowState<ReviewEvent>> roundTripped = store.find("review-2").orElseThrow();

        assertAll(
                () -> assertThat(roundTripped.state()).isEqualTo(original),
                () -> assertThat(((FlowStateImpl<ReviewEvent>) roundTripped.state()).previousStepEntryIndex()).isEqualTo(1)
        );
    }

    @Test
    void reads_a_pre_0_33_0_flow_document_that_never_carried_the_previous_step_entry() {
        // The half an upgrade actually depends on, and the reason the document is inserted by hand, since a document this
        // store wrote a moment ago only proves it agrees with itself. This one is an instance that was already parked mid-flow
        // when the upgrade happened, so it has exactly the eight fields 0.32.0 wrote and no previousStepEntryIndex.
        MongoOperations mongoOperations = mongoOperations();
        CloudEventConverter<ReviewEvent> converter = converter();
        SagaStateStore<FlowState<ReviewEvent>> store =
                new SpringMongoSagaStateStore<>(mongoOperations, "saga-review", rawFlowStateType(), converter);
        List<ReviewEvent> received = List.of(
                new ReviewRequested("e1", "already-waiting"),
                new Approved("e2", "already-waiting", 90));

        mongoOperations.insert(new Document("_id", "already-waiting")
                .append("status", SagaStatus.ACTIVE.name())
                .append("version", 1L)
                .append("state", new Document("currentStep", "awaiting-decision")
                        .append("windowStart", 1)
                        .append("stepEntryIndex", 1)
                        .append("completed", false)
                        .append("previousStep", "awaiting-decision")
                        .append("lastAction", ActionKind.NONE.name())
                        .append("matchedBranchIndex", -1)
                        .append("received", received.stream().map(event -> cloudEventJson(converter, event)).toList()))
                .append("timers", List.of())
                .append("streamWatermarks", new Document())
                .append("createdAt", 1_000L)
                .append("updatedAt", 1_000L), "saga-review");

        FlowStateImpl<ReviewEvent> loaded = (FlowStateImpl<ReviewEvent>) store.find("already-waiting").orElseThrow().state();

        assertAll(
                () -> assertThat(loaded.received()).as("the received log decodes unchanged").containsExactlyElementsOf(received),
                () -> assertThat(loaded.currentStep()).isEqualTo("awaiting-decision"),
                () -> assertThat(loaded.windowStart()).isEqualTo(1),
                () -> assertThat(loaded.stepEntryIndex()).isEqualTo(1),
                () -> assertThat(loaded.previousStepEntryIndex())
                        .as("absent means not known, and a window-condition reaction falls back to the whole retained history")
                        .isEqualTo(-1)
        );
    }

    private static String cloudEventJson(CloudEventConverter<ReviewEvent> converter, ReviewEvent event) {
        return new String(requireNonNull(EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE))
                .serialize(converter.toCloudEvent(event)), StandardCharsets.UTF_8);
    }
}
