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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.dsl.saga.HeldEventRef;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.StreamMarks;
import org.occurrent.dsl.saga.blocking.CommitOrderStubSubscriptionModel.InCommitOrder;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.Order;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.OrderCommand;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.OrderEvent;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.OpenLedger;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.RecordingDispatcher;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.dsl.saga.blocking.HeldOrderSaga.ITEM_ADDED_TYPE;
import static org.occurrent.dsl.saga.blocking.HeldOrderSaga.ORDER;
import static org.occurrent.dsl.saga.blocking.HeldOrderSaga.STREAM;
import static org.occurrent.dsl.saga.blocking.HeldOrderSaga.itemAt;
import static org.occurrent.dsl.saga.blocking.HeldOrderSaga.placedAt;
import static org.occurrent.dsl.saga.blocking.HeldOrderSaga.reserve;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * What a failed version costs on a feed that delivers in commit order and on one that does not. A feed in commit order
 * redelivers a failed event in place, so a version the instance cannot list as pending is logged at ERROR and not
 * kept, while a held copy that failed in a drain is kept on any feed, because the copy exists only if some delivery
 * held it. The window of two puts version 2 behind it once versions 3 to 8 are applied.
 */
@DisplayName("SagaCommitOrderFailure")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaCommitOrderFailureTest {

    private static final String ID = "orders";
    private static final String KEY_OF_TWO = STREAM + "@2";
    private static final SagaRunnerConfig WINDOW_OF_2 = SagaRunnerConfig.defaults().withReorderWindow(2);

    private final ScriptableHeldEventsStore<Order> store = new ScriptableHeldEventsStore<>();
    private final RecordingDispatcher dispatcher = new RecordingDispatcher();
    private final List<SagaSubscription> subscriptions = new ArrayList<>();
    private ListAppender<ILoggingEvent> appender;
    private Logger sagaLog;

    @BeforeEach
    void captureLogs() {
        appender = new ListAppender<>();
        appender.start();
        sagaLog = (Logger) getLogger("org.occurrent.dsl.saga");
        sagaLog.addAppender(appender);
    }

    @AfterEach
    void shutdown() {
        subscriptions.forEach(SagaSubscription::close);
        sagaLog.detachAppender(appender);
        appender.stop();
    }

    private void run(StubSubscriptionModel model) {
        subscriptions.add(SagaRunner.<OrderEvent, OrderCommand>agnostic(model, HeldOrderSaga.converter())
                .run(ID, HeldOrderSaga.saga(), store, dispatcher, null, WINDOW_OF_2));
    }

    private static void deliverItems(StubSubscriptionModel model, int fromVersion, int toVersion) {
        for (int version = fromVersion; version <= toVersion; version++) {
            model.deliver(ID, itemAt(version, "item-" + version));
        }
    }

    private SagaEnvelope<Order> stored() {
        return store.delegate().find(ORDER).orElseThrow();
    }

    private List<Long> pending() {
        StreamMarks marks = stored().streamMarks().get(STREAM);
        return marks == null ? List.of() : marks.pending().getOrDefault(ITEM_ADDED_TYPE, List.of());
    }

    private List<HeldEventRef> heldIndex() {
        return store.delegate().heldIndex(ORDER);
    }

    private HeldEventRef onlyHeldRef() {
        assertThat(heldIndex()).hasSize(1);
        return heldIndex().getFirst();
    }

    private List<String> errors() {
        return appender.list.stream().filter(e -> e.getLevel() == Level.ERROR).map(ILoggingEvent::getFormattedMessage).toList();
    }

    @Nested
    @DisplayName("when a held copy fails in a drain on a node whose rule skips and its listing as pending fails")
    class WhenAHeldCopyFailsInADrainOnANodeWhoseRuleSkipsAndItsListingAsPendingFails {

        /**
         * Version 2 arrives before the start while the model holds, so it is held. The model then answers true, and the
         * start drives a drain whose held copy fails. The write that would list it as pending always loses.
         */
        private CopyFailed afterTheCopyFailedInTheDrainTheStartDrove() {
            CommitOrderStubSubscriptionModel model = CommitOrderStubSubscriptionModel.notInCommitOrderForNow();
            run(model);
            model.deliver(ID, itemAt(2, "two"));
            model.answerInCommitOrder(true);
            dispatcher.failReservationOf("two");
            store.loseEveryWriteWithoutState(true);
            Throwable thrown = catchThrowable(() -> model.deliver(ID, placedAt(1)));
            return new CopyFailed(model, thrown);
        }

        private record CopyFailed(CommitOrderStubSubscriptionModel model, Throwable thrown) {
        }

        @Test
        void the_failure_of_the_held_copy_is_the_cause_of_the_failure_that_reaches_the_feed() {
            // When
            CopyFailed failed = afterTheCopyFailedInTheDrainTheStartDrove();

            // Then
            assertThat(failed.thrown()).hasMessageContaining(KEY_OF_TWO).hasCause(dispatcher.failure);
        }

        @Test
        void the_copy_is_kept_and_named_by_the_instance_as_self_held() {
            // When
            afterTheCopyFailedInTheDrainTheStartDrove();

            // Then
            assertAll(
                    () -> assertThat(stored().selfHeldKeys()).containsExactly(KEY_OF_TWO),
                    () -> assertThat(onlyHeldRef().key()).isEqualTo(KEY_OF_TWO),
                    () -> assertThat(stored().heldEventsChecked()).as("a drain is due").isFalse()
            );
        }

        @Test
        void no_error_is_logged() {
            // When
            afterTheCopyFailedInTheDrainTheStartDrove();

            // Then
            assertThat(errors()).isEmpty();
        }

        @Test
        void the_retried_driving_delivery_applies_the_copy_once() {
            // Given
            CommitOrderStubSubscriptionModel model = afterTheCopyFailedInTheDrainTheStartDrove().model();
            dispatcher.stopFailingReservationOf("two");
            store.loseEveryWriteWithoutState(false);

            // When
            Throwable retried = catchThrowable(() -> model.deliver(ID, placedAt(1)));

            // Then
            assertAll(
                    () -> assertThat(retried).isNull(),
                    () -> assertThat(dispatcher.issued()).containsExactly(new OpenLedger(ORDER), reserve("two")),
                    () -> assertThat(stored().state().items()).containsExactly("two"),
                    () -> assertThat(stored().selfHeldKeys()).isEmpty(),
                    () -> assertThat(heldIndex()).isEmpty(),
                    () -> assertThat(errors()).isEmpty()
            );
        }
    }

    @Nested
    @DisplayName("when a window of the failed event's type is applied before the failure is kept, on a model in commit order")
    class WhenAWindowOfTheFailedEventsTypeIsAppliedBeforeTheFailureIsKept {

        @ParameterizedTest
        @EnumSource(InCommitOrder.class)
        @DisplayName("an event that failed after its delivery saw it as new is not held and its failure is rethrown")
        void an_event_that_failed_after_its_delivery_saw_it_as_new_is_not_held_and_its_failure_is_rethrown(InCommitOrder kind) {
            // Given
            CommitOrderStubSubscriptionModel model = kind.create();
            run(model);
            model.deliver(ID, placedAt(1));
            dispatcher.failReservationOf("two");
            dispatcher.beforeReservationOf("two", () -> deliverItems(model, 3, 8));

            // When
            Throwable thrown = catchThrowable(() -> model.deliver(ID, itemAt(2, "two")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isSameAs(dispatcher.failure),
                    () -> assertThat(stored().selfHeldKeys()).isEmpty(),
                    () -> assertThat(heldIndex()).isEmpty(),
                    () -> assertThat(pending()).isEmpty()
            );
        }

        @ParameterizedTest
        @EnumSource(InCommitOrder.class)
        @DisplayName("an event that failed after its delivery saw it as new is logged at error")
        void an_event_that_failed_after_its_delivery_saw_it_as_new_is_logged_at_error(InCommitOrder kind) {
            // Given
            CommitOrderStubSubscriptionModel model = kind.create();
            run(model);
            model.deliver(ID, placedAt(1));
            dispatcher.failReservationOf("two");
            dispatcher.beforeReservationOf("two", () -> deliverItems(model, 3, 8));

            // When
            catchThrowable(() -> model.deliver(ID, itemAt(2, "two")));

            // Then
            assertThat(errors()).singleElement().satisfies(error -> assertThat(error).contains("saw as new").contains(KEY_OF_TWO));
        }

        @Test
        void a_model_that_holds_self_holds_the_same_event_and_logs_no_error() {
            // Given
            CommitOrderStubSubscriptionModel model = CommitOrderStubSubscriptionModel.notInCommitOrderForNow();
            run(model);
            model.deliver(ID, placedAt(1));
            dispatcher.failReservationOf("two");
            dispatcher.beforeReservationOf("two", () -> deliverItems(model, 3, 8));

            // When
            Throwable thrown = catchThrowable(() -> model.deliver(ID, itemAt(2, "two")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isSameAs(dispatcher.failure),
                    () -> assertThat(stored().selfHeldKeys()).containsExactly(KEY_OF_TWO),
                    () -> assertThat(onlyHeldRef().selfHeld()).isTrue(),
                    () -> assertThat(errors()).isEmpty()
            );
        }

        @ParameterizedTest
        @EnumSource(InCommitOrder.class)
        @DisplayName("an event whose first save lost to a window is not held and the overtaken delivery is rethrown")
        void an_event_whose_first_save_lost_to_a_window_is_not_held_and_the_overtaken_delivery_is_rethrown(InCommitOrder kind) {
            // Given
            CommitOrderStubSubscriptionModel model = kind.create();
            run(model);
            model.deliver(ID, placedAt(1));
            store.beforeNextCompareAndSave(() -> deliverItems(model, 3, 8));

            // When
            Throwable thrown = catchThrowable(() -> model.deliver(ID, itemAt(2, "two")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isNotNull().hasMessageContaining(KEY_OF_TWO),
                    () -> assertThat(stored().selfHeldKeys()).isEmpty(),
                    () -> assertThat(heldIndex()).isEmpty(),
                    () -> assertThat(pending()).isEmpty()
            );
        }

        @ParameterizedTest
        @EnumSource(InCommitOrder.class)
        @DisplayName("an event whose first save lost to a window is logged at error")
        void an_event_whose_first_save_lost_to_a_window_is_logged_at_error(InCommitOrder kind) {
            // Given
            CommitOrderStubSubscriptionModel model = kind.create();
            run(model);
            model.deliver(ID, placedAt(1));
            store.beforeNextCompareAndSave(() -> deliverItems(model, 3, 8));

            // When
            catchThrowable(() -> model.deliver(ID, itemAt(2, "two")));

            // Then
            assertThat(errors()).singleElement().satisfies(error -> assertThat(error).contains("saw as new").contains(KEY_OF_TWO));
        }

        @Test
        void a_model_that_holds_self_holds_the_event_whose_first_save_lost_and_logs_no_error() {
            // Given
            CommitOrderStubSubscriptionModel model = CommitOrderStubSubscriptionModel.notInCommitOrderForNow();
            run(model);
            model.deliver(ID, placedAt(1));
            store.beforeNextCompareAndSave(() -> deliverItems(model, 3, 8));

            // When
            Throwable thrown = catchThrowable(() -> model.deliver(ID, itemAt(2, "two")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isNotNull().hasMessageContaining(KEY_OF_TWO),
                    () -> assertThat(stored().selfHeldKeys()).containsExactly(KEY_OF_TWO),
                    () -> assertThat(onlyHeldRef().selfHeld()).isTrue(),
                    () -> assertThat(errors()).isEmpty()
            );
        }
    }

    @Nested
    @DisplayName("when the rule flips after the size fallback kept an event outstanding")
    class WhenTheRuleFlipsAfterTheSizeFallbackKeptAnEventOutstanding {

        /**
         * Version 2 fails while the model holds and versions 3 to 8 overtake it. The instance is at the store's size
         * limit, so the event is kept as an outstanding held copy and not as a key on the instance.
         */
        private CommitOrderStubSubscriptionModel afterTheSizeFallbackKeptVersionTwoOutstanding() {
            CommitOrderStubSubscriptionModel model = CommitOrderStubSubscriptionModel.notInCommitOrderForNow();
            run(model);
            model.deliver(ID, placedAt(1));
            dispatcher.failReservationOf("two");
            dispatcher.beforeReservationOf("two", () -> deliverItems(model, 3, 8));
            store.refuseMarksCarryingAKey(true);
            Throwable thrown = catchThrowable(() -> model.deliver(ID, itemAt(2, "two")));
            assertThat(thrown).as("the delivery that failed").isSameAs(dispatcher.failure);
            dispatcher.stopFailingReservationOf("two");
            model.answerInCommitOrder(true);
            return model;
        }

        @Test
        void the_event_is_kept_as_an_outstanding_held_copy_before_the_flip() {
            // When
            afterTheSizeFallbackKeptVersionTwoOutstanding();

            // Then
            HeldEventRef ref = onlyHeldRef();
            assertAll(
                    () -> assertThat(ref.key()).isEqualTo(KEY_OF_TWO),
                    () -> assertThat(ref.outstanding()).isTrue(),
                    () -> assertThat(ref.selfHeld()).isFalse(),
                    () -> assertThat(dispatcher.issued()).doesNotContain(reserve("two"))
            );
        }

        @Test
        void the_redelivery_is_skipped() {
            // Given
            CommitOrderStubSubscriptionModel model = afterTheSizeFallbackKeptVersionTwoOutstanding();

            // When
            Throwable thrown = catchThrowable(() -> model.deliver(ID, itemAt(2, "two")));

            // Then
            assertThat(thrown).isNull();
        }

        @Test
        void the_drain_applies_the_outstanding_copy_once() {
            // Given
            CommitOrderStubSubscriptionModel model = afterTheSizeFallbackKeptVersionTwoOutstanding();

            // When
            model.deliver(ID, itemAt(2, "two"));

            // Then
            assertAll(
                    () -> assertThat(dispatcher.issued()).containsOnlyOnce(reserve("two")),
                    () -> assertThat(stored().state().items()).containsOnlyOnce("two")
            );
        }

        @Test
        void the_copy_is_released_and_no_drain_is_left_due() {
            // Given
            CommitOrderStubSubscriptionModel model = afterTheSizeFallbackKeptVersionTwoOutstanding();

            // When
            model.deliver(ID, itemAt(2, "two"));

            // Then
            assertAll(
                    () -> assertThat(heldIndex()).isEmpty(),
                    () -> assertThat(stored().selfHeldKeys()).isEmpty(),
                    () -> assertThat(stored().heldEventsChecked()).isTrue()
            );
        }

        @Test
        void a_further_redelivery_does_not_apply_it_again() {
            // Given
            CommitOrderStubSubscriptionModel model = afterTheSizeFallbackKeptVersionTwoOutstanding();
            model.deliver(ID, itemAt(2, "two"));

            // When
            model.deliver(ID, itemAt(2, "two"));

            // Then
            assertThat(dispatcher.issued()).containsOnlyOnce(reserve("two"));
        }
    }
}
