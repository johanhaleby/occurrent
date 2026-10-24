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
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;

import java.util.Optional;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers {@link RepositionableSubscriptions#findIn(SubscriptionModelCapability)}, which answers a model that can be
 * resumed at an explicit position and does not look past a {@link SubscriptionModelWrapper} for one, since a resume at a
 * position through the model beneath a wrapper would go around the state the wrapper keeps for the subscription (ADR 151).
 */
@DisplayName("RepositionableSubscriptions")
@DisplayNameGeneration(ReplaceUnderscores.class)
class RepositionableSubscriptionsTest {

    @Test
    void finds_the_model_itself_when_it_is_repositionable() {
        // Given
        RepositionableModel model = new RepositionableModel();

        // When
        Optional<RepositionableSubscriptions> found = RepositionableSubscriptions.findIn(model);

        // Then
        assertThat(found).containsSame(model);
    }

    @Test
    void finds_a_wrapper_that_is_repositionable_itself_rather_than_the_model_it_wraps() {
        // Given
        RepositionableWrapper wrapper = new RepositionableWrapper(new RepositionableModel());

        // When
        Optional<RepositionableSubscriptions> found = RepositionableSubscriptions.findIn(wrapper);

        // Then
        assertThat(found).containsSame(wrapper);
    }

    @Test
    void is_empty_for_a_wrapper_that_is_not_repositionable_although_the_model_it_wraps_is() {
        // Given
        Wrapper wrapper = new Wrapper(new RepositionableModel());

        // When
        Optional<RepositionableSubscriptions> found = RepositionableSubscriptions.findIn(wrapper);

        // Then
        assertThat(found).isEmpty();
    }

    @Test
    void is_empty_through_several_layers_of_wrapping_none_of_which_is_repositionable() {
        // Given
        Wrapper wrapper = new Wrapper(new Wrapper(new RepositionableModel()));

        // When
        Optional<RepositionableSubscriptions> found = RepositionableSubscriptions.findIn(wrapper);

        // Then
        assertThat(found).isEmpty();
    }

    @Test
    void is_empty_for_a_model_that_is_not_repositionable_and_wraps_nothing() {
        // When
        Optional<RepositionableSubscriptions> found = RepositionableSubscriptions.findIn(new PlainModel());

        // Then
        assertThat(found).isEmpty();
    }

    @Test
    void restartsAt_is_false_for_a_model_that_does_not_override_it() {
        // Given
        RepositionableModel model = new RepositionableModel();

        // When
        boolean restartsAt = model.restartsAt(new StringBasedCheckpoint("position"));

        // Then
        assertThat(restartsAt).isFalse();
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

    private static final class RepositionableModel extends PlainModel implements RepositionableSubscriptions {
        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            throw new UnsupportedOperationException();
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

    private static final class RepositionableWrapper extends Wrapper implements RepositionableSubscriptions {
        private RepositionableWrapper(SubscriptionModel delegate) {
            super(delegate);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            throw new UnsupportedOperationException();
        }
    }
}
