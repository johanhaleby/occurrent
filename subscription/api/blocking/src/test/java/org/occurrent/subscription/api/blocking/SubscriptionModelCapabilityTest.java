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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Covers {@link SubscriptionModelCapability#capability(Class)} and {@link SubscriptionModelCapability#hasCapability(Class)},
 * the instance-side counterpart to the static {@code findIn} methods covered in {@link IntrospectableSubscriptionsTest}
 * and {@link ReplayAwareSubscriptionsTest}.
 * <p>
 * ADR 151 decision 1: a wrapper is looked past only for a capability that observes subscriptions or takes events in,
 * and a capability that acts on a subscription is answered only by a model that implements it.
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
    void capability_unwraps_a_wrapper_chain_to_reach_the_requested_type() {
        IntrospectableModel inner = new IntrospectableModel(Set.of("orders", "shipments"));

        Optional<IntrospectableSubscriptions> found = new Wrapper(new Wrapper(inner)).capability(IntrospectableSubscriptions.class);

        assertThat(found).containsSame(inner);
        assertThat(found.orElseThrow().subscriptionIds()).containsExactlyInAnyOrder("orders", "shipments");
    }

    @Test
    void capability_is_empty_when_nothing_in_the_chain_implements_the_requested_type() {
        Optional<IntrospectableSubscriptions> found = new Wrapper(new PlainModel()).capability(IntrospectableSubscriptions.class);

        assertThat(found).isEmpty();
    }

    @Test
    void has_capability_is_true_when_capability_would_find_something() {
        IntrospectableModel inner = new IntrospectableModel(Set.of("orders"));

        assertThat(new Wrapper(inner).hasCapability(IntrospectableSubscriptions.class)).isTrue();
    }

    @Test
    void has_capability_is_false_when_capability_would_be_empty() {
        assertThat(new Wrapper(new PlainModel()).hasCapability(IntrospectableSubscriptions.class)).isFalse();
    }

    static Stream<Named<Class<? extends SubscriptionModelCapability>>> observingCapabilities() {
        return Stream.of(IntrospectableSubscriptions.class, ReplayAwareSubscriptions.class, HistoryRetainingSubscriptions.class,
                QuietPositionReportingSubscriptions.class, Pushable.class, HistoryLossReportingSubscriptions.class).map(type -> Named.of(type.getSimpleName(), type));
    }

    static Stream<Named<Class<? extends SubscriptionModelCapability>>> actingCapabilities() {
        return Stream.of(Subscribable.class, CancellableSubscriptions.class, SubscriptionModelLifeCycle.class, SubscriptionModel.class,
                CheckpointAwareSubscriptionModel.class, StreamSubscriptionModel.class, DcbSubscriptionModel.class,
                RepositionableSubscriptions.class).map(type -> Named.of(type.getSimpleName(), type));
    }

    @ParameterizedTest
    @MethodSource("actingCapabilities")
    @DisplayName("capability is empty for a capability that acts on a subscription when the wrapper does not implement it, although the model it wraps does")
    void capability_is_empty_for_a_capability_that_acts_on_a_subscription_when_the_wrapper_does_not_implement_it_although_the_model_it_wraps_does(Class<? extends SubscriptionModelCapability> type) {
        // Given
        BareWrapper wrapper = new BareWrapper(modelThatImplementsEveryCapability());

        // When
        Object found = wrapper.capability(type).orElse(null);

        // Then
        assertThat(found).isNull();
    }

    static Stream<Named<Class<? extends SubscriptionModelCapability>>> actingCapabilitiesAWrapperThatActsImplements() {
        return Stream.of(Subscribable.class, CancellableSubscriptions.class, SubscriptionModelLifeCycle.class, SubscriptionModel.class,
                CheckpointAwareSubscriptionModel.class, RepositionableSubscriptions.class).map(type -> Named.of(type.getSimpleName(), type));
    }

    @ParameterizedTest
    @MethodSource("actingCapabilitiesAWrapperThatActsImplements")
    @DisplayName("capability is empty for a capability that acts on a subscription through several layers of wrapping, the innermost one implementing it")
    void capability_is_empty_for_a_capability_that_acts_on_a_subscription_through_several_layers_of_wrapping_the_innermost_one_implementing_it(Class<? extends SubscriptionModelCapability> type) {
        // Given
        BareWrapper wrapper = new BareWrapper(new WrapperThatActsOnSubscriptions(modelThatImplementsEveryCapability()));

        // When
        Object found = wrapper.capability(type).orElse(null);

        // Then
        assertThat(found).isNull();
    }

    @ParameterizedTest
    @MethodSource("actingCapabilities")
    @DisplayName("has capability is false for a capability that acts on a subscription when the wrapper does not implement it")
    void has_capability_is_false_for_a_capability_that_acts_on_a_subscription_when_the_wrapper_does_not_implement_it(Class<? extends SubscriptionModelCapability> type) {
        // Given
        BareWrapper wrapper = new BareWrapper(modelThatImplementsEveryCapability());

        // When
        boolean has = wrapper.hasCapability(type);

        // Then
        assertThat(has).isFalse();
    }

    @ParameterizedTest
    @MethodSource("actingCapabilitiesAWrapperThatActsImplements")
    @DisplayName("capability finds a wrapper that implements a capability that acts on a subscription, rather than the model it wraps")
    void capability_finds_a_wrapper_that_implements_a_capability_that_acts_on_a_subscription_rather_than_the_model_it_wraps(Class<? extends SubscriptionModelCapability> type) {
        // Given
        WrapperThatActsOnSubscriptions wrapper = new WrapperThatActsOnSubscriptions(modelThatImplementsEveryCapability());

        // When
        Object found = wrapper.capability(type).orElse(null);

        // Then
        assertThat(found).isSameAs(wrapper);
    }

    @ParameterizedTest
    @MethodSource("observingCapabilities")
    @DisplayName("capability finds the model beneath a wrapper that does not implement a capability that observes subscriptions or takes events in")
    void capability_finds_the_model_beneath_a_wrapper_that_does_not_implement_a_capability_that_observes_subscriptions_or_takes_events_in(Class<? extends SubscriptionModelCapability> type) {
        // Given
        CheckpointAwareSubscriptionModel beneath = modelThatImplementsEveryCapability();
        BareWrapper wrapper = new BareWrapper(beneath);

        // When
        Object found = wrapper.capability(type).orElse(null);

        // Then
        assertThat(found).isSameAs(beneath);
    }

    @ParameterizedTest
    @MethodSource("observingCapabilities")
    @DisplayName("capability finds the model beneath several layers of wrapping for a capability that observes subscriptions or takes events in")
    void capability_finds_the_model_beneath_several_layers_of_wrapping_for_a_capability_that_observes_subscriptions_or_takes_events_in(Class<? extends SubscriptionModelCapability> type) {
        // Given
        CheckpointAwareSubscriptionModel beneath = modelThatImplementsEveryCapability();
        Wrapper wrapper = new Wrapper(new Wrapper(beneath));

        // When
        Object found = wrapper.capability(type).orElse(null);

        // Then
        assertThat(found).isSameAs(beneath);
    }

    @Test
    void capability_for_a_capability_that_observes_is_found_on_a_wrapper_while_one_that_acts_is_not_on_the_same_model() {
        // Given
        BareWrapper wrapper = new BareWrapper(modelThatImplementsEveryCapability());

        // When
        Optional<IntrospectableSubscriptions> observing = wrapper.capability(IntrospectableSubscriptions.class);
        Optional<RepositionableSubscriptions> acting = wrapper.capability(RepositionableSubscriptions.class);

        // Then
        assertAll(
                () -> assertThat(observing).as("capability that observes").isPresent(),
                () -> assertThat(acting).as("capability that acts").isEmpty()
        );
    }

    // A model that implements every capability, and answers a method that has no sensible answer for it with nothing
    @SuppressWarnings("unchecked")
    private static CheckpointAwareSubscriptionModel modelThatImplementsEveryCapability() {
        Class<?>[] interfaces = {CheckpointAwareSubscriptionModel.class, StreamSubscriptionModel.class, DcbSubscriptionModel.class,
                RepositionableSubscriptions.class, IntrospectableSubscriptions.class, ReplayAwareSubscriptions.class,
                HistoryRetainingSubscriptions.class, QuietPositionReportingSubscriptions.class, Pushable.class,
                HistoryLossReportingSubscriptions.class};
        return (CheckpointAwareSubscriptionModel) Proxy.newProxyInstance(CheckpointAwareSubscriptionModel.class.getClassLoader(), interfaces, (proxy, method, args) -> {
            if (method.isDefault()) {
                return InvocationHandler.invokeDefault(proxy, method, args);
            }
            if (method.getDeclaringClass() == Object.class) {
                return switch (method.getName()) {
                    case "equals" -> proxy == args[0];
                    case "hashCode" -> System.identityHashCode(proxy);
                    default -> "a model that implements every capability";
                };
            }
            throw new UnsupportedOperationException(method.getName());
        });
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

    // A wrapper that implements nothing but SubscriptionModelWrapper, so it answers no capability of its own
    private static final class BareWrapper implements SubscriptionModelWrapper {
        private final SubscriptionModel delegate;

        private BareWrapper(SubscriptionModel delegate) {
            this.delegate = delegate;
        }

        @Override
        public SubscriptionModel getWrappedSubscriptionModel() {
            return delegate;
        }
    }

    // A wrapper that implements the capabilities that act on a subscription itself, as a wrapper that keeps state for a subscription does
    private static final class WrapperThatActsOnSubscriptions extends PlainModel implements SubscriptionModelWrapper, CheckpointAwareSubscriptionModel, RepositionableSubscriptions {
        private final SubscriptionModel delegate;

        private WrapperThatActsOnSubscriptions(SubscriptionModel delegate) {
            this.delegate = delegate;
        }

        @Override
        public SubscriptionModel getWrappedSubscriptionModel() {
            return delegate;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return null;
        }
    }
}
