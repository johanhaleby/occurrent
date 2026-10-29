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
import org.occurrent.dsl.saga.InMemorySagaStateStore;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.blocking.CommitOrderStubSubscriptionModel.InCommitOrder;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.Order;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.OrderCommand;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.OrderEvent;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.OpenLedger;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.RecordingDispatcher;
import org.occurrent.subscription.api.blocking.CommitOrderedSubscriptions;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.dsl.saga.blocking.HeldOrderSaga.ORDER;
import static org.occurrent.dsl.saga.blocking.HeldOrderSaga.STREAM;
import static org.occurrent.dsl.saga.blocking.HeldOrderSaga.itemAt;
import static org.occurrent.dsl.saga.blocking.HeldOrderSaga.placedAt;
import static org.occurrent.dsl.saga.blocking.HeldOrderSaga.reserve;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * What the runner asks of a subscription model before it holds a non-start event, and of the state store before it
 * subscribes. Every test goes through {@code SagaRunner.run} and a stub model, so the answer that counts is the one the
 * runner reads from the capability and not one a test hands to the executor.
 */
@DisplayName("SagaCommitOrderFeedRule")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaCommitOrderFeedRuleTest {

    private static final String ID = "orders";
    private static final String KEY_OF_TWO = STREAM + "@2";
    private static final String NOT_DECLARED_WARNING = "does not declare commit order";

    private final InMemorySagaStateStore<Order> store = new InMemorySagaStateStore<>();
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

    private void run(Subscribable model) {
        run(model, store);
    }

    private void run(Subscribable model, SagaStateStore<Order> stateStore) {
        subscriptions.add(SagaRunner.<OrderEvent, OrderCommand>agnostic(model, HeldOrderSaga.converter())
                .run(ID, HeldOrderSaga.saga(), stateStore, dispatcher, null, SagaRunnerConfig.defaults()));
    }

    private List<String> heldKeys() {
        return store.heldIndex(ORDER).stream().map(HeldEventRef::key).sorted().toList();
    }

    private List<String> warnings() {
        return appender.list.stream().filter(e -> e.getLevel() == Level.WARN).map(ILoggingEvent::getFormattedMessage).toList();
    }

    private List<String> commitOrderWarnings() {
        return warnings().stream().filter(message -> message.contains(NOT_DECLARED_WARNING)).toList();
    }

    private Order stateOfTheInstance() {
        return store.find(ORDER).orElseThrow().state();
    }

    @Nested
    @DisplayName("when a non-start event is delivered before its start on a model that answers true")
    class WhenANonStartEventIsDeliveredBeforeItsStartOnAModelThatAnswersTrue {

        @ParameterizedTest
        @EnumSource(InCommitOrder.class)
        @DisplayName("the event is skipped and nothing is held")
        void the_event_is_skipped_and_nothing_is_held(InCommitOrder kind) {
            // Given
            CommitOrderStubSubscriptionModel model = kind.create();
            run(model);

            // When
            Throwable thrown = catchThrowable(() -> model.deliver(ID, itemAt(2, "book")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isNull(),
                    () -> assertThat(store.find(ORDER)).isEmpty(),
                    () -> assertThat(heldKeys()).isEmpty(),
                    () -> assertThat(dispatcher.issued()).isEmpty()
            );
        }

        @ParameterizedTest
        @EnumSource(InCommitOrder.class)
        @DisplayName("the event is not applied when the start arrives")
        void the_event_is_not_applied_when_the_start_arrives(InCommitOrder kind) {
            // Given
            CommitOrderStubSubscriptionModel model = kind.create();
            run(model);
            model.deliver(ID, itemAt(2, "book"));

            // When
            model.deliver(ID, placedAt(1));

            // Then
            assertAll(
                    () -> assertThat(dispatcher.issued()).containsExactly(new OpenLedger(ORDER)),
                    () -> assertThat(stateOfTheInstance().items()).isEmpty(),
                    () -> assertThat(heldKeys()).isEmpty()
            );
        }
    }

    @Nested
    @DisplayName("when a non-start event is delivered before its start on a model that holds")
    class WhenANonStartEventIsDeliveredBeforeItsStartOnAModelThatHolds {

        @Test
        void a_model_that_answers_false_holds_the_event_without_running_its_reaction() {
            // Given
            CommitOrderStubSubscriptionModel model = CommitOrderStubSubscriptionModel.notInCommitOrderForNow();
            run(model);

            // When
            Throwable thrown = catchThrowable(() -> model.deliver(ID, itemAt(2, "book")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isNull(),
                    () -> assertThat(store.find(ORDER)).isEmpty(),
                    () -> assertThat(heldKeys()).containsExactly(KEY_OF_TWO),
                    () -> assertThat(dispatcher.issued()).isEmpty()
            );
        }

        @Test
        void a_model_that_answers_false_applies_the_held_event_after_the_start() {
            // Given
            CommitOrderStubSubscriptionModel model = CommitOrderStubSubscriptionModel.notInCommitOrderForNow();
            run(model);
            model.deliver(ID, itemAt(2, "book"));

            // When
            model.deliver(ID, placedAt(1));

            // Then
            assertAll(
                    () -> assertThat(dispatcher.issued()).containsExactly(new OpenLedger(ORDER), reserve("book")),
                    () -> assertThat(stateOfTheInstance().items()).containsExactly("book"),
                    () -> assertThat(heldKeys()).isEmpty()
            );
        }

        @Test
        void a_model_that_does_not_implement_the_capability_holds_the_event_without_running_its_reaction() {
            // Given
            StubSubscriptionModel model = new StubSubscriptionModel();
            run(model);

            // When
            Throwable thrown = catchThrowable(() -> model.deliver(ID, itemAt(2, "book")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isNull(),
                    () -> assertThat(store.find(ORDER)).isEmpty(),
                    () -> assertThat(heldKeys()).containsExactly(KEY_OF_TWO),
                    () -> assertThat(dispatcher.issued()).isEmpty()
            );
        }

        @Test
        void a_model_that_does_not_implement_the_capability_applies_the_held_event_after_the_start() {
            // Given
            StubSubscriptionModel model = new StubSubscriptionModel();
            run(model);
            model.deliver(ID, itemAt(2, "book"));

            // When
            model.deliver(ID, placedAt(1));

            // Then
            assertAll(
                    () -> assertThat(dispatcher.issued()).containsExactly(new OpenLedger(ORDER), reserve("book")),
                    () -> assertThat(stateOfTheInstance().items()).containsExactly("book"),
                    () -> assertThat(heldKeys()).isEmpty()
            );
        }
    }

    @Nested
    @DisplayName("when the answer changes between two deliveries")
    class WhenTheAnswerChangesBetweenTwoDeliveries {

        @Test
        void an_event_is_held_or_skipped_by_the_answer_at_its_own_delivery() {
            // Given
            CommitOrderStubSubscriptionModel model = CommitOrderStubSubscriptionModel.notInCommitOrderForNow();
            run(model);
            model.deliver(ID, itemAt(2, "book"));
            model.answerInCommitOrder(true);

            // When
            model.deliver(ID, itemAt(3, "pen"));
            model.deliver(ID, placedAt(1));

            // Then
            assertAll(
                    () -> assertThat(dispatcher.issued()).containsExactly(new OpenLedger(ORDER), reserve("book")),
                    () -> assertThat(stateOfTheInstance().items()).containsExactly("book")
            );
        }
    }

    @Nested
    @DisplayName("when a wrapper hides a model that answers true")
    class WhenAWrapperHidesAModelThatAnswersTrue {

        @Test
        void the_saga_holds_the_event_that_arrives_before_its_start() {
            // Given
            CommitOrderStubSubscriptionModel inner = CommitOrderStubSubscriptionModel.declaringEveryEventInCommitOrder();
            run(new WrappingStubSubscriptionModel(inner));

            // When
            inner.deliver(ID, itemAt(2, "book"));

            // Then
            assertThat(heldKeys()).containsExactly(KEY_OF_TWO);
        }

        @Test
        void exactly_one_warning_names_the_wrapper_and_the_model_that_declares_commit_order() {
            // Given
            CommitOrderStubSubscriptionModel inner = CommitOrderStubSubscriptionModel.declaringEveryEventInCommitOrder();

            // When
            run(new WrappingStubSubscriptionModel(inner));
            inner.deliver(ID, itemAt(2, "book"), placedAt(1));

            // Then
            assertThat(commitOrderWarnings()).singleElement().satisfies(warning -> assertAll(
                    () -> assertThat(warning).contains(WrappingStubSubscriptionModel.class.getName()),
                    () -> assertThat(warning).contains(CommitOrderStubSubscriptionModel.class.getName())
            ));
        }

        @Test
        void the_warning_names_the_top_wrapper_when_the_model_is_wrapped_twice() {
            // Given
            CommitOrderStubSubscriptionModel inner = CommitOrderStubSubscriptionModel.declaringEveryEventInCommitOrder();

            // When
            run(new TopWrapper(new WrappingStubSubscriptionModel(inner)));

            // Then
            assertThat(commitOrderWarnings()).singleElement().satisfies(warning -> assertAll(
                    () -> assertThat(warning).contains(TopWrapper.class.getName()),
                    () -> assertThat(warning).doesNotContain(WrappingStubSubscriptionModel.class.getName()),
                    () -> assertThat(warning).contains(CommitOrderStubSubscriptionModel.class.getName())
            ));
        }

        @Test
        void one_warning_names_the_nearest_declaring_model_when_two_models_in_the_chain_declare_commit_order() {
            // Given
            CommitOrderStubSubscriptionModel inner = CommitOrderStubSubscriptionModel.inCommitOrderForNow();

            // When
            run(new TopWrapper(new DeclaringWrapper(inner)));

            // Then
            assertThat(commitOrderWarnings()).singleElement().satisfies(warning -> assertAll(
                    () -> assertThat(warning).contains(TopWrapper.class.getName()),
                    () -> assertThat(warning).contains(DeclaringWrapper.class.getName()),
                    () -> assertThat(warning).doesNotContain(CommitOrderStubSubscriptionModel.class.getName())
            ));
        }

        @Test
        void a_wrapper_over_a_model_that_declares_nothing_is_not_warned_about() {
            // Given
            StubSubscriptionModel inner = new StubSubscriptionModel();

            // When
            run(new WrappingStubSubscriptionModel(inner));

            // Then
            assertThat(commitOrderWarnings()).isEmpty();
        }

        @Test
        void a_wrapper_that_declares_commit_order_itself_is_not_warned_about_and_its_answer_is_used() {
            // Given
            CommitOrderStubSubscriptionModel inner = CommitOrderStubSubscriptionModel.inCommitOrderForNow();
            run(new DeclaringWrapper(inner));

            // When
            inner.deliver(ID, itemAt(2, "book"));

            // Then
            assertAll(
                    () -> assertThat(commitOrderWarnings()).isEmpty(),
                    () -> assertThat(heldKeys()).isEmpty()
            );
        }

        @Test
        void a_model_that_declares_commit_order_without_a_wrapper_is_not_warned_about() {
            // When
            run(CommitOrderStubSubscriptionModel.declaringEveryEventInCommitOrder());

            // Then
            assertThat(commitOrderWarnings()).isEmpty();
        }
    }

    @Nested
    @DisplayName("when the state store has nowhere to hold an event")
    class WhenTheStateStoreHasNowhereToHoldAnEvent {

        private final StoreWithoutHeldEvents storeWithoutHeldEvents = new StoreWithoutHeldEvents();

        @Test
        void throws_IllegalStateException_on_a_model_that_does_not_implement_the_capability() {
            // Given
            StubSubscriptionModel model = new StubSubscriptionModel();

            // When
            Throwable thrown = catchThrowable(() -> run(model, storeWithoutHeldEvents));

            // Then
            assertAll(
                    () -> assertThat(thrown).isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining(ID)
                            .hasMessageContaining(StubSubscriptionModel.class.getName())
                            .hasMessageContaining(StoreWithoutHeldEvents.class.getName()),
                    () -> assertThat(model.subscriptionIds()).isEmpty()
            );
        }

        @Test
        void throws_IllegalStateException_on_a_model_that_answers_false() {
            // Given
            CommitOrderStubSubscriptionModel model = CommitOrderStubSubscriptionModel.notInCommitOrderForNow();

            // When
            Throwable thrown = catchThrowable(() -> run(model, storeWithoutHeldEvents));

            // Then
            assertAll(
                    () -> assertThat(thrown).isInstanceOf(IllegalStateException.class),
                    () -> assertThat(model.subscriptionIds()).isEmpty()
            );
        }

        @Test
        void throws_IllegalStateException_on_a_model_that_answers_true_without_declaring_every_event() {
            // Given
            CommitOrderStubSubscriptionModel model = CommitOrderStubSubscriptionModel.inCommitOrderForNow();

            // When
            Throwable thrown = catchThrowable(() -> run(model, storeWithoutHeldEvents));

            // Then
            assertAll(
                    () -> assertThat(thrown).isInstanceOf(IllegalStateException.class),
                    () -> assertThat(model.subscriptionIds()).isEmpty()
            );
        }

        @Test
        void a_model_that_declares_every_event_in_commit_order_is_accepted_and_subscribed() {
            // Given
            CommitOrderStubSubscriptionModel model = CommitOrderStubSubscriptionModel.declaringEveryEventInCommitOrder();

            // When
            Throwable thrown = catchThrowable(() -> run(model, storeWithoutHeldEvents));

            // Then
            assertAll(
                    () -> assertThat(thrown).isNull(),
                    () -> assertThat(model.subscriptionIds()).containsExactly(ID)
            );
        }

        @Test
        void a_store_that_holds_events_is_accepted_on_a_model_that_can_deliver_out_of_order() {
            // Given
            StubSubscriptionModel model = new StubSubscriptionModel();

            // When
            Throwable thrown = catchThrowable(() -> run(model, store));

            // Then
            assertAll(
                    () -> assertThat(thrown).isNull(),
                    () -> assertThat(model.subscriptionIds()).containsExactly(ID)
            );
        }
    }

    private static final class TopWrapper extends WrappingStubSubscriptionModel {
        private TopWrapper(SubscriptionModel wrapped) {
            super(wrapped);
        }
    }

    private static final class DeclaringWrapper extends WrappingStubSubscriptionModel implements CommitOrderedSubscriptions {
        private final CommitOrderStubSubscriptionModel wrapped;

        private DeclaringWrapper(CommitOrderStubSubscriptionModel wrapped) {
            super(wrapped);
            this.wrapped = wrapped;
        }

        @Override
        public boolean deliversInCommitOrder(String subscriptionId) {
            return wrapped.deliversInCommitOrder(subscriptionId);
        }
    }

    /** Stores instances and nothing else, so it cannot hold an event. */
    private static final class StoreWithoutHeldEvents implements SagaStateStore<Order> {
        private final InMemorySagaStateStore<Order> delegate = new InMemorySagaStateStore<>();

        @Override
        public Optional<SagaEnvelope<Order>> find(String sagaId) {
            return delegate.find(sagaId);
        }

        @Override
        public boolean compareAndSave(String sagaId, SagaEnvelope<Order> envelope, long expectedVersion) {
            return delegate.compareAndSave(sagaId, envelope, expectedVersion);
        }

        @Override
        public List<SagaEnvelope<Order>> findWithDueTimers(Instant now, int limit) {
            return delegate.findWithDueTimers(now, limit);
        }

        @Override
        public void delete(String sagaId) {
            delegate.delete(sagaId);
        }
    }
}
