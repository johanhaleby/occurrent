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

package org.occurrent.subscription.api.reactor;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers {@link SubscriptionModelCapability#capability(Class)} and {@link SubscriptionModelCapability#hasCapability(Class)}
 * on the reactor stack, which has no {@code SubscriptionModelWrapper} to unwrap, so both reduce to a direct
 * {@code instanceof} check against the model itself.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SubscriptionModelCapabilityTest {

    @Test
    void capability_finds_the_model_itself_when_it_directly_implements_the_requested_type() {
        IntrospectableModel model = new IntrospectableModel(Set.of("orders"));

        Optional<IntrospectableSubscriptions> found = model.capability(IntrospectableSubscriptions.class);

        assertThat(found).containsSame(model);
    }

    @Test
    void capability_is_empty_when_the_model_does_not_implement_the_requested_type() {
        Optional<IntrospectableSubscriptions> found = new PlainModel().capability(IntrospectableSubscriptions.class);

        assertThat(found).isEmpty();
    }

    @Test
    void has_capability_is_true_when_capability_would_find_something() {
        IntrospectableModel model = new IntrospectableModel(Set.of("orders"));

        assertThat(model.hasCapability(IntrospectableSubscriptions.class)).isTrue();
    }

    @Test
    void has_capability_is_false_when_capability_would_be_empty() {
        assertThat(new PlainModel().hasCapability(IntrospectableSubscriptions.class)).isFalse();
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

    // Nothing but the one method the capability requires, so listenForCatchup is the interface default.
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
        public SubscriptionHandle subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
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
        public SubscriptionHandle resumeSubscription(String subscriptionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }
    }

    private static final class IntrospectableModel extends PlainModel implements IntrospectableSubscriptions {
        private final Set<String> ids;

        private IntrospectableModel(Set<String> ids) {
            this.ids = ids;
        }

        @Override
        public Set<String> subscriptionIds() {
            return ids;
        }
    }
}
