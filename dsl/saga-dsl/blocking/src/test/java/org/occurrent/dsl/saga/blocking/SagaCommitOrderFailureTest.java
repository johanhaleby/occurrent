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
import org.occurrent.dsl.saga.HeldEventRef;
import org.occurrent.dsl.saga.InMemorySagaStateStore;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.StreamMarks;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.Order;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.OrderCommand;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.OrderEvent;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.OpenLedger;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.RecordingDispatcher;

import java.util.ArrayList;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

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
 * kept, except where the store holds events and the version was overtaken in flight, which is kept as a plain copy
 * because a redelivery of it would be skipped. A held copy that failed is kept on any feed, because the copy exists
 * only if some delivery held it. The window of two puts version 2 behind it once versions 3 to 8 are applied.
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
    @DisplayName("when a held copy fails on a model in commit order")
    class WhenAHeldCopyFailsOnAModelInCommitOrder {

        /**
         * Version 2 arrives before the start while the model holds, so it is held. The saga then runs on a model that
         * declares every event in commit order, and the start applies the held copy, which fails. A write without the
         * state always loses, which no failure of a held copy makes.
         */
        private CopyFailed afterTheCopyFailedWhenTheStartAppliedIt() {
            CommitOrderStubSubscriptionModel holding = CommitOrderStubSubscriptionModel.notInCommitOrderForNow();
            run(holding);
            holding.deliver(ID, itemAt(2, "two"));
            CommitOrderStubSubscriptionModel model = CommitOrderStubSubscriptionModel.declaringEveryEventInCommitOrder();
            run(model);
            dispatcher.failReservationOf("two");
            Throwable thrown = catchThrowable(() -> model.deliver(ID, placedAt(1)));
            return new CopyFailed(model, thrown);
        }

        private record CopyFailed(CommitOrderStubSubscriptionModel model, Throwable thrown) {
        }

        @Test
        void the_failure_of_the_held_copy_is_the_cause_of_the_failure_that_reaches_the_feed() {
            // When
            CopyFailed failed = afterTheCopyFailedWhenTheStartAppliedIt();

            // Then
            assertThat(failed.thrown()).hasMessageContaining(KEY_OF_TWO).hasCause(dispatcher.failure);
        }

        @Test
        void the_copy_stays_held_and_the_instance_names_no_failure_for_it() {
            // When
            afterTheCopyFailedWhenTheStartAppliedIt();

            // Then
            assertAll(
                    () -> assertThat(onlyHeldRef().key()).isEqualTo(KEY_OF_TWO),
                    () -> assertThat(stored().failure()).isNull(),
                    () -> assertThat(pending()).isEmpty()
            );
        }

        @Test
        void no_error_is_logged() {
            // When
            afterTheCopyFailedWhenTheStartAppliedIt();

            // Then
            assertThat(errors()).isEmpty();
        }

        @Test
        void the_retried_driving_delivery_applies_the_copy_once() {
            // Given
            CommitOrderStubSubscriptionModel model = afterTheCopyFailedWhenTheStartAppliedIt().model();
            dispatcher.stopFailingReservationOf("two");

            // When
            Throwable retried = catchThrowable(() -> model.deliver(ID, placedAt(1)));

            // Then
            assertAll(
                    () -> assertThat(retried).isNull(),
                    () -> assertThat(dispatcher.issued()).containsExactly(new OpenLedger(ORDER), reserve("two")),
                    () -> assertThat(stored().state().items()).containsExactly("two"),
                    () -> assertThat(heldIndex()).isEmpty(),
                    () -> assertThat(errors()).isEmpty()
            );
        }
    }

    @Nested
    @DisplayName("when a window of the failed event's type is applied before the failure is kept")
    class WhenAWindowOfTheFailedEventsTypeIsAppliedBeforeTheFailureIsKept {

        @Test
        void an_event_that_failed_after_its_delivery_saw_it_as_new_is_held_as_a_plain_copy_and_its_failure_is_rethrown() {
            // Given
            CommitOrderStubSubscriptionModel model = CommitOrderStubSubscriptionModel.declaringEveryEventInCommitOrder();
            run(model);
            model.deliver(ID, placedAt(1));
            dispatcher.failReservationOf("two");
            dispatcher.beforeReservationOf("two", () -> deliverItems(model, 3, 8));

            // When
            Throwable thrown = catchThrowable(() -> model.deliver(ID, itemAt(2, "two")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isSameAs(dispatcher.failure),
                    () -> assertThat(onlyHeldRef()).satisfies(ref -> assertAll(
                            () -> assertThat(ref.key()).isEqualTo(KEY_OF_TWO),
                            () -> assertThat(ref.startEvent()).isFalse(),
                            () -> assertThat(ref.failingSince()).isNull())),
                    () -> assertThat(pending()).isEmpty()
            );
        }

        @Test
        void an_event_that_failed_after_its_delivery_saw_it_as_new_logs_no_error_and_reacts_once_when_it_is_delivered_again() {
            // Given
            CommitOrderStubSubscriptionModel model = CommitOrderStubSubscriptionModel.declaringEveryEventInCommitOrder();
            run(model);
            model.deliver(ID, placedAt(1));
            dispatcher.failReservationOf("two");
            dispatcher.beforeReservationOf("two", () -> deliverItems(model, 3, 8));
            catchThrowable(() -> model.deliver(ID, itemAt(2, "two")));
            dispatcher.stopFailingReservationOf("two");

            // When
            Throwable redelivery = catchThrowable(() -> model.deliver(ID, itemAt(2, "two")));

            // Then
            assertAll(
                    () -> assertThat(errors()).isEmpty(),
                    () -> assertThat(redelivery).isNull(),
                    () -> assertThat(dispatcher.issued()).containsOnlyOnce(reserve("two")),
                    () -> assertThat(heldIndex()).isEmpty()
            );
        }

        @Test
        void a_model_that_holds_keeps_a_plain_copy_of_the_same_event_and_logs_no_error() {
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
                    () -> assertThat(onlyHeldRef()).satisfies(ref -> assertAll(
                            () -> assertThat(ref.key()).isEqualTo(KEY_OF_TWO),
                            () -> assertThat(ref.startEvent()).isFalse(),
                            () -> assertThat(ref.failingSince()).isNull())),
                    () -> assertThat(errors()).isEmpty()
            );
        }

        @Test
        void an_event_whose_first_save_lost_to_a_window_is_held_as_a_plain_copy_and_the_overtaken_delivery_is_rethrown() {
            // Given
            CommitOrderStubSubscriptionModel model = CommitOrderStubSubscriptionModel.declaringEveryEventInCommitOrder();
            run(model);
            model.deliver(ID, placedAt(1));
            store.beforeNextCompareAndSave(() -> deliverItems(model, 3, 8));

            // When
            Throwable thrown = catchThrowable(() -> model.deliver(ID, itemAt(2, "two")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isNotNull().hasMessageContaining(KEY_OF_TWO),
                    () -> assertThat(onlyHeldRef()).satisfies(ref -> assertAll(
                            () -> assertThat(ref.key()).isEqualTo(KEY_OF_TWO),
                            () -> assertThat(ref.startEvent()).isFalse(),
                            () -> assertThat(ref.failingSince()).isNull())),
                    () -> assertThat(pending()).isEmpty()
            );
        }

        @Test
        void an_event_whose_first_save_lost_to_a_window_logs_no_error_and_is_applied_once_when_it_is_delivered_again() {
            // Given
            CommitOrderStubSubscriptionModel model = CommitOrderStubSubscriptionModel.declaringEveryEventInCommitOrder();
            run(model);
            model.deliver(ID, placedAt(1));
            store.beforeNextCompareAndSave(() -> deliverItems(model, 3, 8));
            catchThrowable(() -> model.deliver(ID, itemAt(2, "two")));

            // When
            Throwable redelivery = catchThrowable(() -> model.deliver(ID, itemAt(2, "two")));

            // Then
            assertAll(
                    () -> assertThat(errors()).isEmpty(),
                    () -> assertThat(redelivery).isNull(),
                    () -> assertThat(stored().state().items()).containsOnlyOnce("two"),
                    () -> assertThat(heldIndex()).isEmpty()
            );
        }

        @Test
        void a_model_that_holds_keeps_a_plain_copy_of_the_event_whose_first_save_lost_and_logs_no_error() {
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
                    () -> assertThat(onlyHeldRef()).satisfies(ref -> assertAll(
                            () -> assertThat(ref.key()).isEqualTo(KEY_OF_TWO),
                            () -> assertThat(ref.startEvent()).isFalse(),
                            () -> assertThat(ref.failingSince()).isNull())),
                    () -> assertThat(errors()).isEmpty()
            );
        }
    }

    @Nested
    @DisplayName("when the store holds no events and a window of the failed event's type is applied before the failure is kept")
    class WhenTheStoreHoldsNoEventsAndAWindowOfTheFailedEventsTypeIsAppliedBeforeTheFailureIsKept {

        private CommitOrderStubSubscriptionModel afterVersionTwoFailedAndVersionsThreeToEightWereApplied(StoreWithoutHeldEvents<Order> storeWithoutHeldEvents, List<Throwable> thrown) {
            CommitOrderStubSubscriptionModel model = CommitOrderStubSubscriptionModel.declaringEveryEventInCommitOrder();
            subscriptions.add(SagaRunner.<OrderEvent, OrderCommand>agnostic(model, HeldOrderSaga.converter())
                    .run(ID, HeldOrderSaga.saga(), storeWithoutHeldEvents, dispatcher, null, WINDOW_OF_2));
            model.deliver(ID, placedAt(1));
            dispatcher.failReservationOf("two");
            dispatcher.beforeReservationOf("two", () -> deliverItems(model, 3, 8));
            thrown.add(catchThrowable(() -> model.deliver(ID, itemAt(2, "two"))));
            return model;
        }

        @Test
        void an_event_that_failed_after_its_delivery_saw_it_as_new_is_rethrown_and_logged_at_error() {
            // Given
            List<Throwable> thrown = new ArrayList<>();

            // When
            afterVersionTwoFailedAndVersionsThreeToEightWereApplied(new StoreWithoutHeldEvents<>(), thrown);

            // Then
            assertAll(
                    () -> assertThat(thrown).singleElement().isSameAs(dispatcher.failure),
                    () -> assertThat(errors()).singleElement().satisfies(error -> assertThat(error).contains("saw as new").contains(KEY_OF_TWO))
            );
        }

        @Test
        void the_redelivery_of_that_event_is_skipped_as_behind_the_reorder_window_and_its_reaction_never_runs() {
            // Given
            List<Throwable> thrown = new ArrayList<>();
            CommitOrderStubSubscriptionModel model = afterVersionTwoFailedAndVersionsThreeToEightWereApplied(new StoreWithoutHeldEvents<>(), thrown);
            dispatcher.stopFailingReservationOf("two");

            // When
            Throwable redelivery = catchThrowable(() -> model.deliver(ID, itemAt(2, "two")));

            // Then
            assertAll(
                    () -> assertThat(redelivery).isNull(),
                    () -> assertThat(dispatcher.issued()).doesNotContain(reserve("two"))
            );
        }
    }

    @Nested
    @DisplayName("when the saga runs on a model in commit order after a plain copy was kept for an event that failed")
    class WhenTheSagaRunsOnAModelInCommitOrderAfterAPlainCopyWasKeptForAnEventThatFailed {

        /**
         * Version 2 fails while the model holds and versions 3 to 8 overtake it, so a plain copy of it is held. The saga
         * then runs on a model that declares every event in commit order, and the returned model is that one.
         */
        private CommitOrderStubSubscriptionModel afterAPlainCopyOfVersionTwoWasKept() {
            CommitOrderStubSubscriptionModel holding = CommitOrderStubSubscriptionModel.notInCommitOrderForNow();
            run(holding);
            holding.deliver(ID, placedAt(1));
            dispatcher.failReservationOf("two");
            dispatcher.beforeReservationOf("two", () -> deliverItems(holding, 3, 8));
            Throwable thrown = catchThrowable(() -> holding.deliver(ID, itemAt(2, "two")));
            assertThat(thrown).as("the delivery that failed").isSameAs(dispatcher.failure);
            dispatcher.stopFailingReservationOf("two");
            CommitOrderStubSubscriptionModel model = CommitOrderStubSubscriptionModel.declaringEveryEventInCommitOrder();
            run(model);
            return model;
        }

        @Test
        void the_event_is_kept_as_a_plain_held_copy_before_the_saga_runs_on_the_model_in_commit_order() {
            // When
            afterAPlainCopyOfVersionTwoWasKept();

            // Then
            HeldEventRef ref = onlyHeldRef();
            assertAll(
                    () -> assertThat(ref.key()).isEqualTo(KEY_OF_TWO),
                    () -> assertThat(ref.startEvent()).isFalse(),
                    () -> assertThat(ref.failingSince()).isNull(),
                    () -> assertThat(dispatcher.issued()).doesNotContain(reserve("two"))
            );
        }

        @Test
        void the_redelivery_is_acknowledged() {
            // Given
            CommitOrderStubSubscriptionModel model = afterAPlainCopyOfVersionTwoWasKept();

            // When
            Throwable thrown = catchThrowable(() -> model.deliver(ID, itemAt(2, "two")));

            // Then
            assertThat(thrown).isNull();
        }

        @Test
        void the_redelivery_applies_the_copy_once() {
            // Given
            CommitOrderStubSubscriptionModel model = afterAPlainCopyOfVersionTwoWasKept();

            // When
            model.deliver(ID, itemAt(2, "two"));

            // Then
            assertAll(
                    () -> assertThat(dispatcher.issued()).containsOnlyOnce(reserve("two")),
                    () -> assertThat(stored().state().items()).containsOnlyOnce("two")
            );
        }

        @Test
        void the_copy_is_released() {
            // Given
            CommitOrderStubSubscriptionModel model = afterAPlainCopyOfVersionTwoWasKept();

            // When
            model.deliver(ID, itemAt(2, "two"));

            // Then
            assertThat(heldIndex()).isEmpty();
        }

        @Test
        void a_further_redelivery_does_not_apply_it_again() {
            // Given
            CommitOrderStubSubscriptionModel model = afterAPlainCopyOfVersionTwoWasKept();
            model.deliver(ID, itemAt(2, "two"));

            // When
            model.deliver(ID, itemAt(2, "two"));

            // Then
            assertThat(dispatcher.issued()).containsOnlyOnce(reserve("two"));
        }
    }

    /** Stores instances and nothing else, so it cannot hold an event. */
    private static final class StoreWithoutHeldEvents<S> implements SagaStateStore<S> {
        private final InMemorySagaStateStore<S> delegate = new InMemorySagaStateStore<>();

        @Override
        public Optional<SagaEnvelope<S>> find(String sagaId) {
            return delegate.find(sagaId);
        }

        @Override
        public boolean compareAndSave(String sagaId, SagaEnvelope<S> envelope, long expectedVersion) {
            return delegate.compareAndSave(sagaId, envelope, expectedVersion);
        }

        @Override
        public List<SagaEnvelope<S>> findWithDueTimers(Instant now, int limit) {
            return delegate.findWithDueTimers(now, limit);
        }

        @Override
        public void delete(String sagaId) {
            delegate.delete(sagaId);
        }
    }
}
