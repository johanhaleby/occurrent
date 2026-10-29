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
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;

import java.util.Optional;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers {@link CommitOrderedSubscriptions#findIn(SubscriptionModelCapability)}. A model is found only when it
 * implements the capability itself, because a wrapper around a commit-ordered model can still break the order.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CommitOrderedSubscriptionsTest {

    @Test
    void finds_the_model_itself_when_it_implements_the_capability() {
        CommitOrderedModel model = new CommitOrderedModel();

        Optional<CommitOrderedSubscriptions> found = CommitOrderedSubscriptions.findIn(model);

        assertThat(found).containsSame(model);
    }

    @Test
    void is_empty_for_a_model_that_declares_nothing() {
        assertThat(CommitOrderedSubscriptions.findIn(new PlainModel())).isEmpty();
    }

    /**
     * The wrapper is not commit ordered just because what it wraps is, since a catch-up handover, a thread pool or a
     * listener that parks deliveries between the two would break the order.
     */
    @Test
    void does_not_find_a_commit_ordered_model_through_a_wrapper_that_does_not_implement_the_capability() {
        Optional<CommitOrderedSubscriptions> found = CommitOrderedSubscriptions.findIn(new Wrapper(new CommitOrderedModel()));

        assertThat(found).isEmpty();
    }

    @Test
    void does_not_find_a_commit_ordered_model_through_several_layers_of_wrapping() {
        Optional<CommitOrderedSubscriptions> found = CommitOrderedSubscriptions.findIn(new Wrapper(new Wrapper(new CommitOrderedModel())));

        assertThat(found).isEmpty();
    }

    @Test
    void a_wrapper_that_implements_the_capability_is_found_over_a_delegate_that_does_not() {
        CommitOrderedWrapper wrapper = new CommitOrderedWrapper(new PlainModel());

        Optional<CommitOrderedSubscriptions> found = CommitOrderedSubscriptions.findIn(wrapper);

        assertThat(found).containsSame(wrapper);
    }

    @Test
    void a_model_that_does_not_say_is_not_taken_to_deliver_every_event_in_commit_order() {
        assertThat(new CommitOrderedModel().deliversEveryEventInCommitOrder()).isFalse();
    }

    @Test
    void a_model_can_declare_that_it_delivers_every_event_in_commit_order() {
        assertThat(new AlwaysCommitOrderedModel().deliversEveryEventInCommitOrder()).isTrue();
    }

    private static class PlainModel implements SubscriptionModel {
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

    // Overrides only the required method, so deliversEveryEventInCommitOrder keeps its default.
    private static class CommitOrderedModel extends PlainModel implements CommitOrderedSubscriptions {
        @Override
        public boolean deliversInCommitOrder(String subscriptionId) {
            return true;
        }
    }

    private static final class AlwaysCommitOrderedModel extends CommitOrderedModel {
        @Override
        public boolean deliversEveryEventInCommitOrder() {
            return true;
        }
    }

    // Both interfaces, the way every real wrapper in this repository is shaped.
    private static class Wrapper extends PlainModel implements SubscriptionModelWrapper {
        private final SubscriptionModel delegate;

        private Wrapper(SubscriptionModel delegate) {
            this.delegate = delegate;
        }

        @Override
        public SubscriptionModel getWrappedSubscriptionModel() {
            return delegate;
        }
    }

    // Declared here to cover the lookup order, not to model a real subscription model.
    private static final class CommitOrderedWrapper extends Wrapper implements CommitOrderedSubscriptions {
        private CommitOrderedWrapper(SubscriptionModel delegate) {
            super(delegate);
        }

        @Override
        public boolean deliversInCommitOrder(String subscriptionId) {
            return true;
        }
    }
}
