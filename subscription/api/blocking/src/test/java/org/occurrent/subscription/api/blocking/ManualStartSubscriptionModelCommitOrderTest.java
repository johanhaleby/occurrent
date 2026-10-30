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

package org.occurrent.subscription.api.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;

import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What {@link ManualStartSubscriptionModel} answers to {@link CommitOrderedSubscriptions#deliversEveryEventInCommitOrder()}.
 * It hands each action to the wrapped model unchanged, so it says what the wrapped model says and nothing more.
 */
@DisplayName("ManualStartSubscriptionModelCommitOrder")
@DisplayNameGeneration(ReplaceUnderscores.class)
class ManualStartSubscriptionModelCommitOrderTest {

    @Test
    void delivers_every_event_in_commit_order_when_the_wrapped_model_declares_that_it_does() {
        // Given
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(new DeclaringModel(true));

        // When
        boolean deliversInCommitOrder = model.deliversEveryEventInCommitOrder();

        // Then
        assertThat(deliversInCommitOrder).isTrue();
    }

    @Test
    void does_not_deliver_every_event_in_commit_order_when_the_wrapped_model_declares_that_it_does_not() {
        // Given
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(new DeclaringModel(false));

        // When
        boolean deliversInCommitOrder = model.deliversEveryEventInCommitOrder();

        // Then
        assertThat(deliversInCommitOrder).isFalse();
    }

    @Test
    void does_not_deliver_every_event_in_commit_order_when_the_wrapped_model_does_not_declare_it() {
        // Given
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(new SilentModel());

        // When
        boolean deliversInCommitOrder = model.deliversEveryEventInCommitOrder();

        // Then
        assertThat(deliversInCommitOrder).isFalse();
    }

    @Test
    void is_found_as_a_commit_ordered_model_by_the_lookup() {
        // Given
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(new DeclaringModel(true));

        // When
        var found = CommitOrderedSubscriptions.findIn(model);

        // Then
        assertThat(found).containsSame(model);
    }

    private static class SilentModel implements SubscriptionModel {
        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
        }

        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public boolean isRunning() {
            return false;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return false;
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }
    }

    private static final class DeclaringModel extends SilentModel implements CommitOrderedSubscriptions {
        private final boolean deliversEveryEventInCommitOrder;

        private DeclaringModel(boolean deliversEveryEventInCommitOrder) {
            this.deliversEveryEventInCommitOrder = deliversEveryEventInCommitOrder;
        }

        @Override
        public boolean deliversEveryEventInCommitOrder() {
            return deliversEveryEventInCommitOrder;
        }
    }
}
