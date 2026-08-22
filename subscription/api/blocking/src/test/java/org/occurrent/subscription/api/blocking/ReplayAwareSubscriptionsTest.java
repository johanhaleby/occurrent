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
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;

import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers {@link ReplayAwareSubscriptions#findIn(SubscriptionModelCapability)}, the lookup a caller uses instead of casting to a concrete
 * catch-up model, since the model that knows about the replay is usually behind a {@link SubscriptionModelWrapper}
 * such as {@code DurableSubscriptionModel(CatchupSubscriptionModel(..))}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReplayAwareSubscriptionsTest {

    @Test
    void finds_the_model_itself_when_it_is_replay_aware() {
        ReplayAwareModel model = new ReplayAwareModel(Set.of("orders"));

        Optional<ReplayAwareSubscriptions> found = ReplayAwareSubscriptions.findIn(model);

        assertThat(found).containsSame(model);
    }

    @Test
    void unwraps_a_delegating_model_to_reach_the_replay_aware_one() {
        ReplayAwareModel inner = new ReplayAwareModel(Set.of("orders"));

        Optional<ReplayAwareSubscriptions> found = ReplayAwareSubscriptions.findIn(new Wrapper(inner));

        assertThat(found).containsSame(inner);
        assertThat(found.orElseThrow().isCatchingUp("orders")).isTrue();
    }

    @Test
    void unwraps_through_several_layers_of_wrapping() {
        ReplayAwareModel inner = new ReplayAwareModel(Set.of("orders"));

        Optional<ReplayAwareSubscriptions> found = ReplayAwareSubscriptions.findIn(new Wrapper(new Wrapper(inner)));

        assertThat(found).containsSame(inner);
    }

    @Test
    void is_empty_when_nothing_in_the_chain_replays() {
        Optional<ReplayAwareSubscriptions> found = ReplayAwareSubscriptions.findIn(new Wrapper(new PlainModel()));

        assertThat(found).isEmpty();
    }

    // The default answers false rather than registering, and it validates first, so a model that inherits it refuses
    // a null the same way every model that overrides it does.
    @Test
    void the_default_listener_registration_refuses_a_null_argument() {
        PlainReplayAwareModel model = new PlainReplayAwareModel();

        assertThatThrownBy(() -> model.listenForCatchup(null, new NoopCatchupListener()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("subscriptionId");
        assertThatThrownBy(() -> model.listenForCatchup("orders", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("listener");
        assertThat(model.listenForCatchup("orders", new NoopCatchupListener())).isFalse();
    }

    @Test
    void an_id_the_model_never_saw_is_not_catching_up() {
        ReplayAwareModel model = new ReplayAwareModel(Set.of("orders"));

        // The same answer as a subscription that has handed over, which is what lets a readiness poll ask one question
        // rather than two.
        assertThat(model.isCatchingUp("never-subscribed")).isFalse();
    }

    // Nothing but the two methods the capability requires, so listenForCatchup is the interface default.
    private static final class PlainReplayAwareModel implements ReplayAwareSubscriptions {
        @Override
        public boolean isCatchingUp(String subscriptionId) {
            return false;
        }
    }

    private static final class NoopCatchupListener implements CatchupListener {
        @Override
        public void catchupStarted(Object episode) {
        }

        @Override
        public void historyRead(Object episode) {
        }
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

    private static final class ReplayAwareModel extends PlainModel implements ReplayAwareSubscriptions {
        private final Set<String> replaying;

        private ReplayAwareModel(Set<String> replaying) {
            this.replaying = replaying;
        }

        @Override
        public boolean isCatchingUp(String subscriptionId) {
            return replaying.contains(subscriptionId);
        }
    }

    // Both interfaces, the way every real wrapper in this repository is shaped.
    private static final class Wrapper extends PlainModel implements SubscriptionModelWrapper {
        private final SubscriptionModel delegate;

        private Wrapper(SubscriptionModel delegate) {
            this.delegate = delegate;
        }

        @Override
        public SubscriptionModel getWrappedSubscriptionModel() {
            return delegate;
        }
    }
}
