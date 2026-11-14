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
 * What {@link ManualStartSubscriptionModel} answers to {@link RedeliveringSubscriptions#redeliversAnAttemptNotMade()}.
 * It hands each action to the wrapped model unchanged, so it says what the wrapped model says and nothing more.
 */
@DisplayName("ManualStartSubscriptionModelRedelivery")
@DisplayNameGeneration(ReplaceUnderscores.class)
class ManualStartSubscriptionModelRedeliveryTest {

    @Test
    void redelivers_an_attempt_not_made_when_the_wrapped_model_declares_that_it_does() {
        // Given
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(new DeclaringModel(true));

        // When
        boolean redelivers = model.redeliversAnAttemptNotMade();

        // Then
        assertThat(redelivers).isTrue();
    }

    @Test
    void does_not_redeliver_an_attempt_not_made_when_the_wrapped_model_declares_that_it_does_not() {
        // Given
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(new DeclaringModel(false));

        // When
        boolean redelivers = model.redeliversAnAttemptNotMade();

        // Then
        assertThat(redelivers).isFalse();
    }

    @Test
    void does_not_redeliver_an_attempt_not_made_when_the_wrapped_model_does_not_declare_it() {
        // Given
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(new SilentModel());

        // When
        boolean redelivers = model.redeliversAnAttemptNotMade();

        // Then
        assertThat(redelivers).isFalse();
    }

    @Test
    void does_not_redeliver_an_attempt_not_made_when_a_wrapper_that_does_not_declare_it_sits_between_it_and_a_model_that_does() {
        // Given
        PlainWrapper wrapper = new PlainWrapper(new DeclaringModel(true));
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(wrapper);

        // When
        boolean redelivers = model.redeliversAnAttemptNotMade();

        // Then
        assertThat(redelivers).isFalse();
    }

    @Test
    void is_found_as_a_redelivering_model_by_the_lookup() {
        // Given
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(new DeclaringModel(true));

        // When
        var found = RedeliveringSubscriptions.findIn(model);

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

    private static final class DeclaringModel extends SilentModel implements RedeliveringSubscriptions {
        private final boolean redeliversAnAttemptNotMade;

        private DeclaringModel(boolean redeliversAnAttemptNotMade) {
            this.redeliversAnAttemptNotMade = redeliversAnAttemptNotMade;
        }

        @Override
        public boolean redeliversAnAttemptNotMade() {
            return redeliversAnAttemptNotMade;
        }
    }

    private static final class PlainWrapper extends SilentModel implements SubscriptionModelWrapper {
        private final SubscriptionModel wrapped;

        private PlainWrapper(SubscriptionModel wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public SubscriptionModel getWrappedSubscriptionModel() {
            return wrapped;
        }
    }
}
