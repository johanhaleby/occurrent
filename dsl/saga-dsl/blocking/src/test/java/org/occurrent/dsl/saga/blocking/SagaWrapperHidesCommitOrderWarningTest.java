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
import org.occurrent.dsl.saga.InMemorySagaStateStore;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.Order;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.OrderCommand;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.OrderEvent;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.RecordingDispatcher;
import org.occurrent.subscription.api.blocking.Subscribable;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.dsl.saga.blocking.HeldOrderSaga.itemAt;
import static org.occurrent.dsl.saga.blocking.HeldOrderSaga.placedAt;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * The runner warns when a wrapper hides a subscription model that declares commit order, because the saga then holds
 * events the model would have delivered in order. There is nothing hidden when the model below the wrapper implements
 * the capability but does not declare that every event is delivered in commit order, so there is nothing to warn about.
 */
@DisplayName("A wrapper over a subscription model that implements commit order")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaWrapperHidesCommitOrderWarningTest {

    private static final String ID = "orders";
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
        subscriptions.add(SagaRunner.<OrderEvent, OrderCommand>agnostic(model, HeldOrderSaga.converter())
                .run(ID, HeldOrderSaga.saga(), store, dispatcher, null, SagaRunnerConfig.defaults()));
    }

    private List<String> commitOrderWarnings() {
        return appender.list.stream()
                .filter(e -> e.getLevel() == Level.WARN)
                .map(ILoggingEvent::getFormattedMessage)
                .filter(message -> message.contains(NOT_DECLARED_WARNING))
                .toList();
    }

    @Nested
    @DisplayName("that does not declare every event in commit order")
    class ThatDoesNotDeclareEveryEventInCommitOrder {

        @Test
        void it_is_not_warned_about_because_there_is_no_declaration_to_hide() {
            // Given
            CommitOrderStubSubscriptionModel inner = CommitOrderStubSubscriptionModel.inCommitOrderForNow();

            // When
            run(new WrappingStubSubscriptionModel(inner));
            inner.deliver(ID, itemAt(2, "book"), placedAt(1));

            // Then
            assertThat(commitOrderWarnings()).isEmpty();
        }
    }

    @Nested
    @DisplayName("that declares every event in commit order")
    class ThatDeclaresEveryEventInCommitOrder {

        @Test
        void it_is_warned_about_once_because_the_declaration_is_hidden() {
            // Given
            CommitOrderStubSubscriptionModel inner = CommitOrderStubSubscriptionModel.declaringEveryEventInCommitOrder();

            // When
            run(new WrappingStubSubscriptionModel(inner));
            inner.deliver(ID, itemAt(2, "book"), placedAt(1));

            // Then
            assertThat(commitOrderWarnings()).hasSize(1);
        }
    }
}
