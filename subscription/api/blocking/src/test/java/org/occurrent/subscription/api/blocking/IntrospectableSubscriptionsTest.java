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
import java.util.Set;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers {@link IntrospectableSubscriptions#findIn(SubscriptionModelCapability)}, the lookup a caller uses instead of casting, since the
 * model it needs is often behind one or more {@link SubscriptionModelWrapper} wrappers.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class IntrospectableSubscriptionsTest {

    @Test
    void finds_the_model_itself_when_it_is_introspectable() {
        IntrospectableModel model = new IntrospectableModel(Set.of("orders"));

        Optional<IntrospectableSubscriptions> found = IntrospectableSubscriptions.findIn(model);

        assertThat(found).containsSame(model);
    }

    @Test
    void unwraps_a_delegating_model_to_reach_the_introspectable_one() {
        IntrospectableModel inner = new IntrospectableModel(Set.of("orders", "shipments"));

        Optional<IntrospectableSubscriptions> found = IntrospectableSubscriptions.findIn(new Wrapper(inner));

        assertThat(found).containsSame(inner);
        assertThat(found.orElseThrow().subscriptionIds()).containsExactlyInAnyOrder("orders", "shipments");
    }

    @Test
    void unwraps_through_several_layers_of_wrapping() {
        IntrospectableModel inner = new IntrospectableModel(Set.of("orders"));

        Optional<IntrospectableSubscriptions> found = IntrospectableSubscriptions.findIn(new Wrapper(new Wrapper(inner)));

        assertThat(found).containsSame(inner);
    }

    @Test
    void is_empty_when_nothing_in_the_chain_can_be_introspected() {
        Optional<IntrospectableSubscriptions> found = IntrospectableSubscriptions.findIn(new Wrapper(new PlainModel()));

        assertThat(found).isEmpty();
    }

    private static class PlainModel implements SubscriptionModel {
        @Override
        public SubscriptionHandle subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
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
