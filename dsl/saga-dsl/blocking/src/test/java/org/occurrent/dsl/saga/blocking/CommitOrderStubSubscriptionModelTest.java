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

import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.StartAt;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * The stub has to behave as the contract of {@code CommitOrderedSubscriptions} says, or a saga test that leans on it
 * proves nothing about the subscription models that implement the capability.
 */
@DisplayName("CommitOrderStubSubscriptionModel")
@DisplayNameGeneration(ReplaceUnderscores.class)
class CommitOrderStubSubscriptionModelTest {

    private static final String ID = "orders";

    @Nested
    @DisplayName("when the id is not subscribed")
    class WhenTheIdIsNotSubscribed {

        @Test
        void a_model_that_declares_every_event_in_commit_order_does_not_deliver_it_in_commit_order() {
            // Given
            CommitOrderStubSubscriptionModel model = CommitOrderStubSubscriptionModel.declaringEveryEventInCommitOrder();

            // When
            boolean inCommitOrder = model.deliversInCommitOrder(ID);

            // Then
            assertThat(inCommitOrder).isFalse();
        }

        @Test
        void a_model_that_answers_true_for_now_does_not_deliver_it_in_commit_order() {
            // Given
            CommitOrderStubSubscriptionModel model = CommitOrderStubSubscriptionModel.inCommitOrderForNow();

            // When
            boolean inCommitOrder = model.deliversInCommitOrder(ID);

            // Then
            assertThat(inCommitOrder).isFalse();
        }

        @Test
        void the_id_is_not_delivered_in_commit_order_once_its_subscription_is_cancelled() {
            // Given
            CommitOrderStubSubscriptionModel model = CommitOrderStubSubscriptionModel.inCommitOrderForNow();
            model.subscribe(ID, StartAt.subscriptionModelDefault(), event -> {
            });

            // When
            model.cancelSubscription(ID);

            // Then
            assertThat(model.deliversInCommitOrder(ID)).isFalse();
        }
    }

    @Nested
    @DisplayName("when the id is subscribed")
    class WhenTheIdIsSubscribed {

        @Test
        void a_model_that_declares_every_event_in_commit_order_delivers_it_in_commit_order() {
            // Given
            CommitOrderStubSubscriptionModel model = CommitOrderStubSubscriptionModel.declaringEveryEventInCommitOrder();
            model.subscribe(ID, StartAt.subscriptionModelDefault(), event -> {
            });

            // When
            boolean inCommitOrder = model.deliversInCommitOrder(ID);

            // Then
            assertAll(
                    () -> assertThat(inCommitOrder).isTrue(),
                    () -> assertThat(model.deliversEveryEventInCommitOrder()).isTrue()
            );
        }

        @Test
        void a_model_that_declares_every_event_in_commit_order_cannot_be_made_to_answer_false() {
            // Given
            CommitOrderStubSubscriptionModel model = CommitOrderStubSubscriptionModel.declaringEveryEventInCommitOrder();
            model.subscribe(ID, StartAt.subscriptionModelDefault(), event -> {
            });

            // When
            Throwable thrown = catchThrowable(() -> model.answerInCommitOrder(false));

            // Then
            assertAll(
                    () -> assertThat(thrown).isInstanceOf(IllegalStateException.class),
                    () -> assertThat(model.deliversInCommitOrder(ID)).isTrue()
            );
        }

        @Test
        void a_model_that_answers_true_for_now_does_not_declare_every_event() {
            // Given
            CommitOrderStubSubscriptionModel model = CommitOrderStubSubscriptionModel.inCommitOrderForNow();
            model.subscribe(ID, StartAt.subscriptionModelDefault(), event -> {
            });

            // When
            boolean inCommitOrder = model.deliversInCommitOrder(ID);

            // Then
            assertAll(
                    () -> assertThat(inCommitOrder).isTrue(),
                    () -> assertThat(model.deliversEveryEventInCommitOrder()).isFalse()
            );
        }

        @Test
        void the_answer_of_a_model_that_does_not_declare_every_event_follows_the_last_flip() {
            // Given
            CommitOrderStubSubscriptionModel model = CommitOrderStubSubscriptionModel.notInCommitOrderForNow();
            model.subscribe(ID, StartAt.subscriptionModelDefault(), event -> {
            });
            boolean before = model.deliversInCommitOrder(ID);

            // When
            model.answerInCommitOrder(true);

            // Then
            assertAll(
                    () -> assertThat(before).isFalse(),
                    () -> assertThat(model.deliversInCommitOrder(ID)).isTrue()
            );
        }
    }

    @Nested
    @DisplayName("when events are delivered")
    class WhenEventsAreDelivered {

        @Test
        void the_events_of_one_call_reach_the_listener_in_the_order_given() {
            // Given
            CommitOrderStubSubscriptionModel model = CommitOrderStubSubscriptionModel.inCommitOrderForNow();
            List<CloudEvent> received = new ArrayList<>();
            model.subscribe(ID, StartAt.subscriptionModelDefault(), received::add);
            CloudEvent first = HeldOrderSaga.placedAt(1);
            CloudEvent second = HeldOrderSaga.itemAt(2, "book");
            CloudEvent third = HeldOrderSaga.itemAt(3, "pen");

            // When
            model.deliver(ID, first, second, third);

            // Then
            assertThat(received).containsExactly(first, second, third);
        }

        @Test
        void an_id_nothing_is_subscribed_as_is_an_error_to_deliver_to() {
            // Given
            CommitOrderStubSubscriptionModel model = CommitOrderStubSubscriptionModel.inCommitOrderForNow();

            // When
            Throwable thrown = catchThrowable(() -> model.deliver(ID, HeldOrderSaga.placedAt(1)));

            // Then
            assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining(ID);
        }
    }
}
