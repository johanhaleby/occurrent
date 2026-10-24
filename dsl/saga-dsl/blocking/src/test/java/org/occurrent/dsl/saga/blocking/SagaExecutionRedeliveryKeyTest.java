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

package org.occurrent.dsl.saga.blocking;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.saga.*;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * What the executor does with an event that has no stream key to tell a redelivery by. A global position is not one,
 * because positions are not commit order, and a stream id or version that is present but cannot be read is a broken
 * feed rather than a feed that has no key.
 */
@DisplayName("A saga delivery without a usable redelivery key")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaExecutionRedeliveryKeyTest {

    private static final String ORDER = "order-1";
    private static final Duration BUDGET = Duration.ofMinutes(5);
    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");

    sealed interface OrderEvent permits OrderPlaced, PaymentReserved {
        String eventId();

        String orderId();
    }

    record OrderPlaced(String eventId, String orderId) implements OrderEvent {
    }

    record PaymentReserved(String eventId, String orderId) implements OrderEvent {
    }

    sealed interface OrderCommand permits ShipOrder {
    }

    record ShipOrder(String orderId) implements OrderCommand {
    }

    sealed interface OrderState permits AwaitingPayment, Shipped {
    }

    record AwaitingPayment(String orderId) implements OrderState {
    }

    record Shipped(String orderId) implements OrderState {
    }

    private final AtomicInteger reactionsRun = new AtomicInteger();
    private volatile boolean reactionFails;
    private final AtomicReference<Instant> now = new AtomicReference<>(START);
    private final List<OrderCommand> dispatched = new CopyOnWriteArrayList<>();
    private CloudEventConverter<OrderEvent> converter;
    private SagaStateStore<OrderState> stateStore;
    private ListAppender<ILoggingEvent> appender;
    private Logger executionLog;

    /** Never terminal, so the same instance can be handed the same kind of event again. An empty order id correlates to no instance. */
    private Saga<OrderEvent, OrderState, OrderCommand> orderFulfillment() {
        return Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                .correlateAll(event -> event.orderId().isEmpty() ? null : event.orderId())
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new AwaitingPayment(e.orderId()))
                .evolve(PaymentReserved.class, (state, e) -> new Shipped(e.orderId()))
                .react(PaymentReserved.class, (state, e) -> {
                    reactionsRun.incrementAndGet();
                    if (reactionFails) {
                        throw new IllegalStateException("the reaction cannot handle this payment");
                    }
                    return List.of(SagaEffect.issue(new ShipOrder(e.orderId())));
                })
                .build();
    }

    @BeforeEach
    void createInstances() {
        converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(OrderEvent::eventId).build();
        stateStore = SagaStateStore.inMemory();
        appender = new ListAppender<>();
        appender.start();
        executionLog = (Logger) LoggerFactory.getLogger(SagaExecution.class);
        executionLog.addAppender(appender);
    }

    @AfterEach
    void detachAppender() {
        executionLog.detachAppender(appender);
        appender.stop();
    }

    private SagaExecution<OrderEvent, OrderState, OrderCommand> execution(RedeliveryDetection detection) {
        SagaRunnerConfig config = SagaRunnerConfig.defaults().withRedeliveryDetection(detection).withQuarantineAfter(BUDGET);
        return new SagaExecution<>("orders", orderFulfillment(), stateStore, dispatched::add, converter, config, event -> true, now::get);
    }

    /** An instance that exists and is waiting for its payment, started by an event that has a stream key. */
    private SagaExecution<OrderEvent, OrderState, OrderCommand> executionWithAStartedInstance(RedeliveryDetection detection) {
        SagaExecution<OrderEvent, OrderState, OrderCommand> execution = execution(detection);
        execution.onCloudEvent(streamKeyed(ORDER, 1, new OrderPlaced("placed", ORDER)));
        return execution;
    }

    private List<String> warnings() {
        synchronized (appender) {
            return appender.list.stream()
                    .filter(event -> event.getLevel() == Level.WARN)
                    .map(ILoggingEvent::getFormattedMessage)
                    .toList();
        }
    }

    @Nested
    @DisplayName("when the event carries only a position")
    class WhenTheEventCarriesOnlyAPosition {

        @Test
        void throws_SagaRedeliveryDetectionException_under_REQUIRED_without_running_the_reaction() {
            // Given
            var execution = executionWithAStartedInstance(RedeliveryDetection.REQUIRED);

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(positionOnly(42, new PaymentReserved("paid", ORDER))));

            // Then
            assertAll(
                    () -> assertThat(thrown).isInstanceOf(SagaRedeliveryDetectionException.class).hasMessageContaining("BEST_EFFORT"),
                    () -> assertThat(reactionsRun).hasValue(0),
                    () -> assertThat(dispatched).isEmpty(),
                    () -> assertThat(stateStore.find(ORDER).orElseThrow().state()).isEqualTo(new AwaitingPayment(ORDER))
            );
        }

        @Test
        void throws_SagaRedeliveryDetectionException_under_REQUIRED_for_a_start_event_and_creates_no_instance() {
            // Given
            var execution = execution(RedeliveryDetection.REQUIRED);

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(positionOnly(42, new OrderPlaced("placed", ORDER))));

            // Then
            assertAll(
                    () -> assertThat(thrown).isInstanceOf(SagaRedeliveryDetectionException.class),
                    () -> assertThat(stateStore.find(ORDER)).isEmpty()
            );
        }

        @Test
        void is_applied_once_under_BEST_EFFORT() {
            // Given
            var execution = executionWithAStartedInstance(RedeliveryDetection.BEST_EFFORT);

            // When
            execution.onCloudEvent(positionOnly(42, new PaymentReserved("paid", ORDER)));

            // Then
            assertAll(
                    () -> assertThat(reactionsRun).hasValue(1),
                    () -> assertThat(dispatched).containsExactly(new ShipOrder(ORDER)),
                    () -> assertThat(stateStore.find(ORDER).orElseThrow().state()).isEqualTo(new Shipped(ORDER))
            );
        }

        @Test
        void warns_under_BEST_EFFORT_that_a_redelivery_cannot_be_recognised() {
            // Given
            var execution = executionWithAStartedInstance(RedeliveryDetection.BEST_EFFORT);

            // When
            execution.onCloudEvent(positionOnly(42, new PaymentReserved("paid", ORDER)));

            // Then
            assertThat(warnings()).singleElement().satisfies(warning -> assertAll(
                    () -> assertThat(warning).contains("without both a streamid and a streamversion"),
                    () -> assertThat(warning).contains("BEST_EFFORT")
            ));
        }

        @Test
        void warns_once_per_runner_however_many_position_only_events_arrive() {
            // Given
            var execution = executionWithAStartedInstance(RedeliveryDetection.BEST_EFFORT);

            // When
            execution.onCloudEvent(positionOnly(42, new PaymentReserved("paid-1", ORDER)));
            execution.onCloudEvent(positionOnly(43, new PaymentReserved("paid-2", ORDER)));
            execution.onCloudEvent(positionOnly(44, new PaymentReserved("paid-3", ORDER)));

            // Then
            assertThat(warnings()).hasSize(1);
        }

        @Test
        void is_not_deduplicated_by_its_position_under_BEST_EFFORT() {
            // Given
            var execution = executionWithAStartedInstance(RedeliveryDetection.BEST_EFFORT);

            // When the same position arrives again, once for the same event and once for a different one
            execution.onCloudEvent(positionOnly(42, new PaymentReserved("paid", ORDER)));
            execution.onCloudEvent(positionOnly(42, new PaymentReserved("paid", ORDER)));
            execution.onCloudEvent(positionOnly(42, new PaymentReserved("paid-too", ORDER)));

            // Then every delivery is applied
            assertAll(
                    () -> assertThat(reactionsRun).hasValue(3),
                    () -> assertThat(dispatched).hasSize(3)
            );
        }

        @Test
        void writes_no_position_watermark_when_it_is_applied() {
            // Given
            var execution = executionWithAStartedInstance(RedeliveryDetection.BEST_EFFORT);

            // When
            execution.onCloudEvent(positionOnly(42, new PaymentReserved("paid", ORDER)));

            // Then the instance holds its stream watermark and nothing for the position
            SagaEnvelope<OrderState> instance = stateStore.find(ORDER).orElseThrow();
            assertAll(
                    () -> assertThat(instance.positionWatermark()).isNull(),
                    () -> assertThat(instance.streamWatermarks()).isEqualTo(Map.of(ORDER, 1L))
            );
        }
    }

    @Nested
    @DisplayName("when the streamversion extension cannot be read")
    class WhenTheStreamVersionCannotBeRead {

        @ParameterizedTest
        @EnumSource(RedeliveryDetection.class)
        @DisplayName("throws SagaRedeliveryDetectionException with the read failure as its cause")
        void throws_SagaRedeliveryDetectionException_with_the_read_failure_as_its_cause(RedeliveryDetection detection) {
            // Given
            var execution = executionWithAStartedInstance(detection);

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(unparseableStreamVersion(new PaymentReserved("paid", ORDER))));

            // Then
            assertAll(
                    () -> assertThat(thrown).isInstanceOf(SagaRedeliveryDetectionException.class),
                    () -> assertThat(thrown).hasCauseInstanceOf(IllegalArgumentException.class),
                    () -> assertThat(thrown).hasMessageContaining("'streamversion' extension could not be read")
            );
        }

        @ParameterizedTest
        @EnumSource(RedeliveryDetection.class)
        @DisplayName("does not suggest BEST_EFFORT because it does not apply")
        void does_not_suggest_BEST_EFFORT_because_it_does_not_apply(RedeliveryDetection detection) {
            // Given
            var execution = executionWithAStartedInstance(detection);

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(unparseableStreamVersion(new PaymentReserved("paid", ORDER))));

            // Then
            assertThat(thrown.getMessage()).doesNotContain("BEST_EFFORT").doesNotContain("without both a streamid and a streamversion");
        }

        @Test
        void gives_the_same_message_under_REQUIRED_and_BEST_EFFORT() {
            // Given
            var required = executionWithAStartedInstance(RedeliveryDetection.REQUIRED);

            // When
            Throwable underRequired = catchThrowable(() -> required.onCloudEvent(unparseableStreamVersion(new PaymentReserved("paid", ORDER))));
            Throwable underBestEffort = catchThrowable(() -> execution(RedeliveryDetection.BEST_EFFORT).onCloudEvent(unparseableStreamVersion(new PaymentReserved("paid", ORDER))));

            // Then
            assertThat(underBestEffort).isInstanceOf(SagaRedeliveryDetectionException.class).hasMessage(underRequired.getMessage());
        }

        @ParameterizedTest
        @EnumSource(RedeliveryDetection.class)
        @DisplayName("does not run the reaction")
        void does_not_run_the_reaction(RedeliveryDetection detection) {
            // Given
            var execution = executionWithAStartedInstance(detection);

            // When
            catchThrowable(() -> execution.onCloudEvent(unparseableStreamVersion(new PaymentReserved("paid", ORDER))));

            // Then
            assertAll(
                    () -> assertThat(reactionsRun).hasValue(0),
                    () -> assertThat(dispatched).isEmpty(),
                    () -> assertThat(stateStore.find(ORDER).orElseThrow().state()).isEqualTo(new AwaitingPayment(ORDER))
            );
        }

        @Test
        void is_not_handled_as_an_event_carrying_no_key_under_BEST_EFFORT() {
            // Given
            var execution = executionWithAStartedInstance(RedeliveryDetection.BEST_EFFORT);

            // When
            catchThrowable(() -> execution.onCloudEvent(unparseableStreamVersion(new PaymentReserved("paid", ORDER))));

            // Then the warning about a feed that has no key is not said for a feed that has a broken one
            assertThat(warnings()).noneMatch(warning -> warning.contains("without both a streamid and a streamversion"));
        }

        @ParameterizedTest
        @EnumSource(RedeliveryDetection.class)
        @DisplayName("is not handled as an event that only carries a position")
        void is_not_handled_as_an_event_that_only_carries_a_position(RedeliveryDetection detection) {
            // Given
            var execution = executionWithAStartedInstance(detection);
            CloudEvent withAPositionToo = OccurrentCloudEventExtension.withPosition(unparseableStreamVersion(new PaymentReserved("paid", ORDER)), 42);

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(withAPositionToo));

            // Then
            assertAll(
                    () -> assertThat(thrown).isInstanceOf(SagaRedeliveryDetectionException.class),
                    () -> assertThat(reactionsRun).hasValue(0)
            );
        }

        @ParameterizedTest
        @EnumSource(RedeliveryDetection.class)
        @DisplayName("is never quarantined however long it keeps being offered")
        void is_never_quarantined_however_long_it_keeps_being_offered(RedeliveryDetection detection) {
            // Given
            var execution = executionWithAStartedInstance(detection);
            CloudEvent unreadable = unparseableStreamVersion(new PaymentReserved("paid", ORDER));

            // When the failure is offered again past the quarantine budget
            List<Throwable> thrown = new ArrayList<>();
            for (Duration elapsed : List.of(Duration.ZERO, Duration.ofMinutes(1), BUDGET, BUDGET.plusMinutes(1), Duration.ofHours(1))) {
                now.set(START.plus(elapsed));
                thrown.add(catchThrowable(() -> execution.onCloudEvent(unreadable)));
            }

            // Then
            SagaEnvelope<OrderState> instance = stateStore.find(ORDER).orElseThrow();
            assertAll(
                    () -> assertThat(thrown).allSatisfy(failure -> assertThat(failure).isInstanceOf(SagaRedeliveryDetectionException.class)),
                    () -> assertThat(instance.status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(instance.failure()).isNull()
            );
        }

        @Test
        void is_ignored_when_the_event_correlates_to_no_instance_of_the_saga() {
            // Given
            var execution = execution(RedeliveryDetection.REQUIRED);

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(unparseableStreamVersion(new PaymentReserved("paid", ""))));

            // Then
            assertAll(
                    () -> assertThat(thrown).isNull(),
                    () -> assertThat(reactionsRun).hasValue(0)
            );
        }
    }

    @Nested
    @DisplayName("when the streamid extension cannot be read")
    class WhenTheStreamIdCannotBeRead {

        @ParameterizedTest
        @EnumSource(RedeliveryDetection.class)
        @DisplayName("throws SagaRedeliveryDetectionException with the read failure as its cause")
        void throws_SagaRedeliveryDetectionException_with_the_read_failure_as_its_cause(RedeliveryDetection detection) {
            // Given
            var execution = executionWithAStartedInstance(detection);

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(nonTextStreamId(new PaymentReserved("paid", ORDER))));

            // Then
            assertAll(
                    () -> assertThat(thrown).isInstanceOf(SagaRedeliveryDetectionException.class),
                    () -> assertThat(thrown).hasCauseInstanceOf(IllegalArgumentException.class),
                    () -> assertThat(thrown).hasMessageContaining("'streamid' extension could not be read"),
                    () -> assertThat(thrown.getMessage()).doesNotContain("BEST_EFFORT")
            );
        }

        @ParameterizedTest
        @EnumSource(RedeliveryDetection.class)
        @DisplayName("does not run the reaction")
        void does_not_run_the_reaction(RedeliveryDetection detection) {
            // Given
            var execution = executionWithAStartedInstance(detection);

            // When
            catchThrowable(() -> execution.onCloudEvent(nonTextStreamId(new PaymentReserved("paid", ORDER))));

            // Then
            assertAll(
                    () -> assertThat(reactionsRun).hasValue(0),
                    () -> assertThat(dispatched).isEmpty()
            );
        }

        @ParameterizedTest
        @EnumSource(RedeliveryDetection.class)
        @DisplayName("is never quarantined however long it keeps being offered")
        void is_never_quarantined_however_long_it_keeps_being_offered(RedeliveryDetection detection) {
            // Given
            var execution = executionWithAStartedInstance(detection);
            CloudEvent unreadable = nonTextStreamId(new PaymentReserved("paid", ORDER));

            // When
            for (Duration elapsed : List.of(Duration.ZERO, BUDGET.plusMinutes(1), Duration.ofHours(1))) {
                now.set(START.plus(elapsed));
                catchThrowable(() -> execution.onCloudEvent(unreadable));
            }

            // Then
            SagaEnvelope<OrderState> instance = stateStore.find(ORDER).orElseThrow();
            assertAll(
                    () -> assertThat(instance.status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(instance.failure()).isNull()
            );
        }

        @Test
        void is_ignored_when_the_event_correlates_to_no_instance_of_the_saga() {
            // Given
            var execution = execution(RedeliveryDetection.REQUIRED);

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(nonTextStreamId(new PaymentReserved("paid", ""))));

            // Then
            assertThat(thrown).isNull();
        }
    }

    @Nested
    @DisplayName("when the position extension cannot be read")
    class WhenThePositionCannotBeRead {

        @Test
        void still_recognises_a_redelivery_by_the_stream_key() {
            // Given
            var execution = executionWithAStartedInstance(RedeliveryDetection.REQUIRED);
            CloudEvent paid = CloudEventBuilder.v1(streamKeyed(ORDER, 2, new PaymentReserved("paid", ORDER)))
                    .withExtension(OccurrentCloudEventExtension.POSITION, "not-a-number")
                    .build();

            // When
            execution.onCloudEvent(paid);
            execution.onCloudEvent(paid);

            // Then
            assertAll(
                    () -> assertThat(reactionsRun).hasValue(1),
                    () -> assertThat(dispatched).containsExactly(new ShipOrder(ORDER))
            );
        }
    }

    @Nested
    @DisplayName("when a position-only event keeps failing under BEST_EFFORT with quarantine on")
    class WhenAPositionOnlyEventKeepsFailing {

        @Test
        void never_quarantines_the_instance_and_leaves_no_failure_record() {
            // Given
            var execution = executionWithAStartedInstance(RedeliveryDetection.BEST_EFFORT);
            reactionFails = true;
            CloudEvent poison = positionOnly(42, new PaymentReserved("paid", ORDER));

            // When it is offered again past the budget
            for (Duration elapsed : List.of(Duration.ZERO, Duration.ofMinutes(1), BUDGET, BUDGET.plusMinutes(1), Duration.ofHours(1))) {
                now.set(START.plus(elapsed));
                catchThrowable(() -> execution.onCloudEvent(poison));
            }

            // Then
            SagaEnvelope<OrderState> instance = stateStore.find(ORDER).orElseThrow();
            assertAll(
                    () -> assertThat(instance.status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(instance.failure()).isNull(),
                    () -> assertThat(instance.state()).isEqualTo(new AwaitingPayment(ORDER)),
                    () -> assertThat(reactionsRun).hasValue(5)
            );
        }

        @Test
        void propagates_the_exception_to_the_subscription_model_on_every_delivery() {
            // Given
            var execution = executionWithAStartedInstance(RedeliveryDetection.BEST_EFFORT);
            reactionFails = true;
            CloudEvent poison = positionOnly(42, new PaymentReserved("paid", ORDER));

            // When
            now.set(START.plus(BUDGET.plusMinutes(1)));
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(poison));

            // Then
            assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessage("the reaction cannot handle this payment");
        }

        @Test
        void quarantines_the_same_instance_once_the_failing_event_carries_a_stream_key() {
            // Given the control for the two tests above, so they cannot pass only because quarantine is off
            var execution = executionWithAStartedInstance(RedeliveryDetection.BEST_EFFORT);
            reactionFails = true;
            CloudEvent poison = streamKeyed(ORDER, 2, new PaymentReserved("paid", ORDER));

            // When
            catchThrowable(() -> execution.onCloudEvent(poison));
            now.set(START.plus(BUDGET));
            Throwable secondDelivery = catchThrowable(() -> execution.onCloudEvent(poison));

            // Then it is quarantined and the delivery returns normally
            SagaEnvelope<OrderState> instance = stateStore.find(ORDER).orElseThrow();
            assertAll(
                    () -> assertThat(secondDelivery).isNull(),
                    () -> assertThat(instance.status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(instance.failure().input()).isEqualTo(ORDER + "@2")
            );
        }
    }

    @Nested
    @DisplayName("when the instance holds a failure record written by 0.31.0 to 0.34.0 for a position")
    class WhenTheInstanceHoldsAFailureRecordForAPosition {

        private final Instant failingSince = START.minus(Duration.ofHours(1));

        @BeforeEach
        void storeTheLegacyRecord() {
            SagaFailure legacy = new SagaFailure("position:7", 7L, failingSince, IllegalStateException.class.getName(), "boom");
            boolean stored = stateStore.compareAndSave(ORDER, new SagaEnvelope<>(ORDER, new AwaitingPayment(ORDER), SagaStatus.ACTIVE, 1,
                    List.of(), Map.of(ORDER, 1L), 7L, failingSince, failingSince, null, null, true, legacy), 0);
            assertThat(stored).isTrue();
        }

        @Test
        void stays_active_and_starts_a_fresh_budget_when_a_stream_keyed_event_fails_once() {
            // Given
            var execution = execution(RedeliveryDetection.REQUIRED);
            reactionFails = true;

            // When the failure comes long after the old record's budget ran out
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(streamKeyed(ORDER, 2, new PaymentReserved("paid", ORDER))));

            // Then
            SagaEnvelope<OrderState> instance = stateStore.find(ORDER).orElseThrow();
            assertAll(
                    () -> assertThat(thrown).isInstanceOf(IllegalStateException.class),
                    () -> assertThat(instance.status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(instance.failure().input()).isEqualTo(ORDER + "@2"),
                    () -> assertThat(instance.failure().firstFailedAt()).isEqualTo(START)
            );
        }

        @Test
        void is_cleared_by_the_next_successful_stream_keyed_delivery() {
            // Given
            var execution = execution(RedeliveryDetection.REQUIRED);

            // When
            execution.onCloudEvent(streamKeyed(ORDER, 2, new PaymentReserved("paid", ORDER)));

            // Then
            SagaEnvelope<OrderState> instance = stateStore.find(ORDER).orElseThrow();
            assertAll(
                    () -> assertThat(instance.failure()).isNull(),
                    () -> assertThat(instance.status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(instance.positionWatermark()).isNull(),
                    () -> assertThat(dispatched).containsExactly(new ShipOrder(ORDER))
            );
        }
    }

    // The stream key an event store writes, together with the position it gives the event.
    private CloudEvent streamKeyed(String streamId, long streamVersion, OrderEvent event) {
        return OccurrentCloudEventExtension.withPosition(
                CloudEventBuilder.v1(converter.toCloudEvent(event)).withExtension(OccurrentCloudEventExtension.occurrent(streamId, streamVersion)).build(),
                streamVersion);
    }

    // What a feed that drops the stream extensions but keeps the position delivers.
    private CloudEvent positionOnly(long position, OrderEvent event) {
        return OccurrentCloudEventExtension.withPosition(converter.toCloudEvent(event), position);
    }

    private CloudEvent unparseableStreamVersion(OrderEvent event) {
        return CloudEventBuilder.v1(converter.toCloudEvent(event))
                .withExtension(OccurrentCloudEventExtension.STREAM_ID, ORDER)
                .withExtension(OccurrentCloudEventExtension.STREAM_VERSION, "not-a-number")
                .build();
    }

    // A streamid that is a number rather than text, which is not a string the getter can return.
    private CloudEvent nonTextStreamId(OrderEvent event) {
        return CloudEventBuilder.v1(converter.toCloudEvent(event))
                .withExtension(OccurrentCloudEventExtension.STREAM_ID, 42)
                .withExtension(OccurrentCloudEventExtension.STREAM_VERSION, 2L)
                .build();
    }
}
