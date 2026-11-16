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
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.StreamMarks;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.SagaEnvelope.TypeMark;
import org.occurrent.dsl.saga.SagaFailure;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.flow.FlowState;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl.ActionKind;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl.StepConditionProgress;

import java.net.URI;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

/**
 * The envelope each document in {@code src/test/resources/saga-document-fixtures} stands for, built with the canonical
 * constructors and fixed instants only. It depends on nothing but the envelope records, {@link FlowStateImpl} and the
 * state types below, so it stays compilable when the {@code SagaStateStore} methods change.
 */
final class SagaDocumentFixtures {

    private SagaDocumentFixtures() {
    }

    record OrderState(String orderId, String customer, int quantity) {
    }

    sealed interface ReviewEvent {
        String eventId();
    }

    record ReviewRequested(String eventId, String reviewId) implements ReviewEvent {
    }

    record Approved(String eventId, String reviewId, int score) implements ReviewEvent {
    }

    static final String CORE_ACTIVE_WITH_TIMERS = "core-active-with-timers";
    static final String FLOW_ACTIVE = "flow-active";
    static final String ACTIVE_MARKED = "active-marked";
    static final String QUARANTINED_RECORDING = "quarantined-recording";
    static final String RELEASING = "releasing";
    static final String COMPLETED = "completed";
    static final String NOT_STARTED = "not-started";
    static final String QUARANTINED_0_34_0 = "quarantined-0.34.0";

    static final List<String> GENERATED = List.of(CORE_ACTIVE_WITH_TIMERS, FLOW_ACTIVE, ACTIVE_MARKED, QUARANTINED_RECORDING, RELEASING, COMPLETED, NOT_STARTED);

    private static final Instant CREATED_AT = Instant.parse("2026-03-04T05:06:07.123Z");
    private static final Instant UPDATED_AT = Instant.parse("2026-03-04T05:16:17.456Z");
    private static final Instant COMPLETED_AT = Instant.parse("2026-03-04T06:00:00.789Z");
    private static final Instant FIRST_FAILED_AT = Instant.parse("2026-03-04T05:10:00.250Z");
    private static final Instant RELEASED_AT = Instant.parse("2026-03-04T05:20:00.500Z");
    private static final Instant PENDING_SINCE = Instant.parse("2026-03-04T05:11:12.345Z");

    /**
     * Converts {@link ReviewEvent}s to CloudEvents with a fixed time, so the stored JSON never depends on the clock.
     */
    static CloudEventConverter<ReviewEvent> reviewEventConverter() {
        OffsetDateTime time = OffsetDateTime.of(2026, 3, 4, 5, 6, 7, 0, ZoneOffset.UTC);
        return new JacksonCloudEventConverter.Builder<ReviewEvent>(new ObjectMapper(), URI.create("urn:test"))
                .idMapper(ReviewEvent::eventId)
                .timeMapper(event -> time)
                .build();
    }

    static String sagaId(String fixtureName) {
        return "saga-" + fixtureName;
    }

    static SagaEnvelope<OrderState> coreActiveWithTimers() {
        return new SagaEnvelope<>(sagaId(CORE_ACTIVE_WITH_TIMERS), new OrderState("order-1", "alice", 3), SagaStatus.ACTIVE, 4L,
                List.of(new TimerEntry("reminder", 1_800_000_000_000L), new TimerEntry("expiry", 1_800_000_600_000L, 1_800_000_900_000L)),
                Map.of("stream-a", 12L, "stream-b", 7L), 42L, CREATED_AT, UPDATED_AT, null, null, true, null, Map.of(), null);
    }

    static SagaEnvelope<FlowState<ReviewEvent>> flowActive() {
        FlowStateImpl<ReviewEvent> state = new FlowStateImpl<>("awaiting-decision",
                List.of(new ReviewRequested("e1", "review-1"), new Approved("e2", "review-1", 90)),
                1, 1, false, "submitted", 0, ActionKind.BRANCH, 1, new StepConditionProgress("fingerprint-1", List.of(1, 0)));
        return new SagaEnvelope<>(sagaId(FLOW_ACTIVE), state, SagaStatus.ACTIVE, 2L, List.of(), Map.of("review-1", 1L), null,
                CREATED_AT, UPDATED_AT, null, "awaiting-decision", true, null, Map.of(), null);
    }

    static SagaEnvelope<OrderState> activeMarked() {
        StreamMarks marks = new StreamMarks(5L,
                Map.of("OrderPlaced", new TypeMark(7L, List.of(9L, 11L), 4L)),
                Map.of("OrderPlaced", List.of(8L)),
                List.of(6L),
                Map.of(8L, PENDING_SINCE));
        return new SagaEnvelope<>(sagaId(ACTIVE_MARKED), new OrderState("order-2", "bob", 1), SagaStatus.ACTIVE, 6L, List.of(),
                Map.of("stream-a", 11L), null, CREATED_AT, UPDATED_AT, null, null, true, null, Map.of("stream-a", marks), null);
    }

    static SagaEnvelope<OrderState> quarantinedRecording() {
        SagaFailure failure = new SagaFailure("input-77", 77L, FIRST_FAILED_AT, "java.lang.IllegalStateException", "order is already shipped", null, "quarantine-1");
        return new SagaEnvelope<>(sagaId(QUARANTINED_RECORDING), new OrderState("order-3", "carol", 2), SagaStatus.QUARANTINED, 8L, List.of(),
                Map.of("stream-a", 20L), null, CREATED_AT, UPDATED_AT, null, null, true, failure, Map.of(), 3L);
    }

    static SagaEnvelope<OrderState> releasing() {
        SagaFailure failure = new SagaFailure("input-77", 77L, FIRST_FAILED_AT, "java.lang.IllegalStateException", "order is already shipped", RELEASED_AT, "quarantine-1");
        return new SagaEnvelope<>(sagaId(RELEASING), new OrderState("order-3", "carol", 2), SagaStatus.QUARANTINED, 9L, List.of(),
                Map.of("stream-a", 20L), null, CREATED_AT, UPDATED_AT, null, null, true, failure, Map.of(), 3L);
    }

    static SagaEnvelope<OrderState> completed() {
        return new SagaEnvelope<>(sagaId(COMPLETED), new OrderState("order-4", "dave", 5), SagaStatus.COMPLETED, 10L, List.of(),
                Map.of("stream-a", 30L), null, CREATED_AT, UPDATED_AT, COMPLETED_AT, null, true, null, Map.of(), null);
    }

    static SagaEnvelope<OrderState> notStarted() {
        return new SagaEnvelope<>(sagaId(NOT_STARTED), new OrderState("order-5", "erin", 1), SagaStatus.ACTIVE, 1L, List.of(),
                Map.of(), null, CREATED_AT, UPDATED_AT, null, null, false, null, Map.of(), null);
    }

    // The file quarantined-0.34.0.json was written by hand from 0.34.0's toDocument, not generated by the store.
    static SagaEnvelope<OrderState> quarantined034() {
        SagaFailure failure = new SagaFailure("input-55", 55L, FIRST_FAILED_AT, "java.lang.IllegalStateException", "order is already shipped");
        return new SagaEnvelope<>(sagaId(QUARANTINED_0_34_0), new OrderState("order-6", "frank", 4), SagaStatus.QUARANTINED, 5L,
                List.of(new TimerEntry("reminder", 1_800_000_000_000L)),
                Map.of("stream-a", 15L), 15L, CREATED_AT, UPDATED_AT, null, null, true, failure, Map.of(), null);
    }
}
