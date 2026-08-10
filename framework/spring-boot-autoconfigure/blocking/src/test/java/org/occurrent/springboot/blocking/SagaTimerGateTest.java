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

package org.occurrent.springboot.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper;
import org.occurrent.subscription.api.blocking.ReplayAwareSubscriptions;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A saga must not issue commands while its own subscription is not running, and it must not issue them while that
 * subscription is still replaying history either. A timeout firing part way through a replay decides against state
 * that is only half rebuilt, which is the one thing catching up before going live is meant to avoid.
 * <p>
 * {@code isRunning(id)} cannot express the second half, because it is true throughout a replay. Only
 * {@link ReplayAwareSubscriptions} can, which is why the gate asks through that rather than through a concrete
 * model class.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaTimerGateTest {

    @Test
    void timers_are_held_while_the_subscription_is_replaying_even_though_it_reports_running() {
        Model model = new Model(true, true);

        BooleanSupplier timersEnabled = SagaAnnotationRegistrar.timersEnabledFor(model, "orders");

        assertThat(model.isRunning("orders")).isTrue();
        assertThat(timersEnabled.getAsBoolean()).isFalse();
    }

    @Test
    void timers_are_enabled_once_the_replay_has_handed_over() {
        Model model = new Model(true, false);

        assertThat(SagaAnnotationRegistrar.timersEnabledFor(model, "orders").getAsBoolean()).isTrue();
    }

    @Test
    void timers_are_held_while_the_subscription_is_not_running_at_all() {
        Model model = new Model(false, false);

        assertThat(SagaAnnotationRegistrar.timersEnabledFor(model, "orders").getAsBoolean()).isFalse();
    }

    @Test
    void the_replay_is_found_through_a_wrapper_rather_than_only_on_the_model_itself() {
        // What an event-store saga actually gets, DurableSubscriptionModel(CatchupSubscriptionModel(..)). Asking the
        // concrete class instead of the capability would miss this and fire timers part way through the replay.
        Model inner = new Model(true, true);

        BooleanSupplier timersEnabled = SagaAnnotationRegistrar.timersEnabledFor(new Wrapper(inner), "orders");

        assertThat(timersEnabled.getAsBoolean()).isFalse();
    }

    @Test
    void a_model_that_cannot_report_a_replay_falls_back_to_whether_it_is_running() {
        assertThat(SagaAnnotationRegistrar.timersEnabledFor(new PlainModel(true), "orders").getAsBoolean()).isTrue();
        assertThat(SagaAnnotationRegistrar.timersEnabledFor(new PlainModel(false), "orders").getAsBoolean()).isFalse();
    }

    @Test
    void a_subscribable_with_no_life_cycle_keeps_firing_timers_as_before() {
        Subscribable noLifeCycle = (subscriptionId, filter, startAt, action) -> {
            throw new UnsupportedOperationException();
        };

        assertThat(SagaAnnotationRegistrar.timersEnabledFor(noLifeCycle, "orders").getAsBoolean()).isTrue();
    }

    private static class PlainModel implements SubscriptionModel {
        private final boolean running;

        private PlainModel(boolean running) {
            this.running = running;
        }

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
            return running;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return running;
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

    private static final class Model extends PlainModel implements ReplayAwareSubscriptions {
        private final boolean catchingUp;

        private Model(boolean running, boolean catchingUp) {
            super(running);
            this.catchingUp = catchingUp;
        }

        @Override
        public boolean isCatchingUp(String subscriptionId) {
            return catchingUp;
        }
    }

    // Reports running itself, so a gate that asked only the wrapper would answer true and fire timers mid-replay.
    private static final class Wrapper extends PlainModel implements SubscriptionModelWrapper {
        private final SubscriptionModel delegate;

        private Wrapper(SubscriptionModel delegate) {
            super(true);
            this.delegate = delegate;
        }

        @Override
        public SubscriptionModel getWrappedSubscriptionModel() {
            return delegate;
        }
    }
}
