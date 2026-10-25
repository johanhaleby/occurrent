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

package org.occurrent.subscription.blocking.competingconsumers;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.internal.CatchupReads;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.internal.DcbCatchupReads;
import org.occurrent.subscription.api.blocking.CancellableSubscriptions;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.DcbSubscriptionModel;
import org.occurrent.subscription.api.blocking.HistoryLossReportingSubscriptions;
import org.occurrent.subscription.api.blocking.HistoryRetainingSubscriptions;
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions;
import org.occurrent.subscription.api.blocking.ManualStartSubscriptionModel;
import org.occurrent.subscription.api.blocking.Pushable;
import org.occurrent.subscription.api.blocking.QuietPositionReportingSubscriptions;
import org.occurrent.subscription.api.blocking.ReplayAwareSubscriptions;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.StreamSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionModelCapability;
import org.occurrent.subscription.api.blocking.SubscriptionModelLifeCycle;
import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.occurrent.subscription.blocking.durable.catchup.DcbCatchupModelFactory;
import org.occurrent.subscription.blocking.durable.catchup.StreamCatchupSubscriptionModel;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ADR 151 decision 1, for every blocking {@link SubscriptionModelWrapper} Occurrent ships, over a model beneath it that
 * implements every capability: {@link SubscriptionModelCapability#capability(Class)} answers the wrapper itself for a
 * capability the wrapper implements, the model beneath it for a capability that observes subscriptions or takes events
 * in, and nothing for a capability that acts on a subscription.
 */
@DisplayName("WrapperCapabilityLookup")
@DisplayNameGeneration(ReplaceUnderscores.class)
class WrapperCapabilityLookupTest {

    private static final DcbCriteria QUERY = DcbCriteria.tags(Tag.parse("name:1"));
    private static final CatchupSubscriptionModelConfig CATCHUP_CONFIG = new CatchupSubscriptionModelConfig(100);

    // Every blocking capability a lookup can name
    private static final List<Class<? extends SubscriptionModelCapability>> CAPABILITIES = List.of(
            Subscribable.class, CancellableSubscriptions.class, SubscriptionModelLifeCycle.class, SubscriptionModel.class,
            CheckpointAwareSubscriptionModel.class, StreamSubscriptionModel.class, DcbSubscriptionModel.class,
            SubscriptionModelWrapper.class, RepositionableSubscriptions.class, IntrospectableSubscriptions.class,
            ReplayAwareSubscriptions.class, HistoryRetainingSubscriptions.class, QuietPositionReportingSubscriptions.class,
            Pushable.class, HistoryLossReportingSubscriptions.class);

    // The capabilities a wrapper is looked past for when it doesn't implement them
    private static final Set<Class<?>> UNWRAPPED = Set.of(
            IntrospectableSubscriptions.class, ReplayAwareSubscriptions.class, HistoryRetainingSubscriptions.class,
            QuietPositionReportingSubscriptions.class, Pushable.class, HistoryLossReportingSubscriptions.class);

    // The wrapper, built over a model beneath it, and the capabilities the wrapper's own declaration gives it, taking
    // RepositionableSubscriptions for a wrapper that answers it only when the model beneath it can reposition
    record Wrapper(Function<CheckpointAwareSubscriptionModel, SubscriptionModelCapability> over, Set<Class<?>> implemented) {
    }

    private static final Set<Class<?>> A_SUBSCRIPTION_MODEL = Set.of(Subscribable.class, CancellableSubscriptions.class, SubscriptionModelLifeCycle.class, SubscriptionModel.class, SubscriptionModelWrapper.class);

    static Stream<Named<Wrapper>> wrappers() {
        return Stream.of(
                Named.of("DurableSubscriptionModel", new Wrapper(
                        beneath -> new DurableSubscriptionModel(beneath, new InMemoryCheckpointStorage()),
                        with(A_SUBSCRIPTION_MODEL, CheckpointAwareSubscriptionModel.class, RepositionableSubscriptions.class))),
                Named.of("CompetingConsumerSubscriptionModel", new Wrapper(
                        beneath -> new CompetingConsumerSubscriptionModel(beneath, new NoLeases()),
                        with(A_SUBSCRIPTION_MODEL, IntrospectableSubscriptions.class))),
                Named.of("ManualStartSubscriptionModel", new Wrapper(
                        ManualStartSubscriptionModel::stoppedByDefault,
                        with(A_SUBSCRIPTION_MODEL, IntrospectableSubscriptions.class))),
                Named.of("CatchupSubscriptionModel over a stream", new Wrapper(
                        beneath -> new CatchupSubscriptionModel(beneath, unused(EventStoreQueries.class, CatchupReads.class), CATCHUP_CONFIG),
                        with(A_SUBSCRIPTION_MODEL, ReplayAwareSubscriptions.class, RepositionableSubscriptions.class))),
                Named.of("CatchupSubscriptionModel over a DCB event store", new Wrapper(
                        beneath -> new CatchupSubscriptionModel(beneath, unused(DcbEventStore.class, DcbCatchupReads.class), QUERY, CATCHUP_CONFIG),
                        with(A_SUBSCRIPTION_MODEL, ReplayAwareSubscriptions.class, RepositionableSubscriptions.class))),
                Named.of("StreamCatchupSubscriptionModel", new Wrapper(
                        beneath -> new StreamCatchupSubscriptionModel(beneath, unused(EventStoreQueries.class, CatchupReads.class), CATCHUP_CONFIG),
                        with(A_SUBSCRIPTION_MODEL, ReplayAwareSubscriptions.class, RepositionableSubscriptions.class))),
                Named.of("DcbCatchupSubscriptionModel", new Wrapper(
                        beneath -> DcbCatchupModelFactory.dcbCatchupSubscriptionModel(beneath, unused(DcbEventStore.class, DcbCatchupReads.class), QUERY, CATCHUP_CONFIG),
                        with(A_SUBSCRIPTION_MODEL, ReplayAwareSubscriptions.class, RepositionableSubscriptions.class))));
    }

    static Stream<Arguments> wrappersAndCapabilities() {
        return wrappers().flatMap(wrapper -> CAPABILITIES.stream().map(capability -> Arguments.of(wrapper, Named.of(capability.getSimpleName(), capability))));
    }

    @ParameterizedTest
    @MethodSource("wrappersAndCapabilities")
    @DisplayName("a wrapper answers itself for what it implements, the model beneath it for what observes or takes events in, and nothing otherwise")
    void a_wrapper_answers_itself_for_what_it_implements_the_model_beneath_it_for_what_observes_or_takes_events_in_and_nothing_otherwise(Wrapper wrapper, Class<? extends SubscriptionModelCapability> capability) {
        // Given
        CheckpointAwareSubscriptionModel beneath = modelThatImplementsEveryCapability();
        SubscriptionModelCapability model = wrapper.over().apply(beneath);

        // When
        Optional<? extends SubscriptionModelCapability> found = model.capability(capability);

        // Then
        Optional<SubscriptionModelCapability> expected = wrapper.implemented().contains(capability) ? Optional.of(model)
                : UNWRAPPED.contains(capability) ? Optional.of(beneath) : Optional.empty();
        assertThat(found).as("the answer for " + capability.getSimpleName()).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("wrappers")
    @DisplayName("the capabilities each wrapper is expected to implement are the ones its declaration gives it")
    void the_capabilities_each_wrapper_is_expected_to_implement_are_the_ones_its_declaration_gives_it(Wrapper wrapper) {
        // Given
        SubscriptionModelCapability model = wrapper.over().apply(modelThatImplementsEveryCapability());

        // When
        Set<Class<?>> implemented = new LinkedHashSet<>();
        CAPABILITIES.stream().filter(capability -> capability.isInstance(model)).forEach(implemented::add);

        // Then
        assertThat(implemented).containsExactlyInAnyOrderElementsOf(wrapper.implemented());
    }

    private static Set<Class<?>> with(Set<Class<?>> base, Class<?>... more) {
        Set<Class<?>> all = new LinkedHashSet<>(base);
        all.addAll(Arrays.asList(more));
        return all;
    }

    private static CheckpointAwareSubscriptionModel modelThatImplementsEveryCapability() {
        return unused(CheckpointAwareSubscriptionModel.class, StreamSubscriptionModel.class, DcbSubscriptionModel.class,
                RepositionableSubscriptions.class, IntrospectableSubscriptions.class, ReplayAwareSubscriptions.class,
                HistoryRetainingSubscriptions.class, QuietPositionReportingSubscriptions.class, Pushable.class,
                HistoryLossReportingSubscriptions.class);
    }

    // An implementation of the interfaces that runs the methods an interface defaults, does nothing for a void method,
    // answers false to a boolean one and fails every other call
    @SuppressWarnings("unchecked")
    private static <T> T unused(Class<T> type, Class<?>... moreTypes) {
        Class<?>[] interfaces = new Class<?>[1 + moreTypes.length];
        interfaces[0] = type;
        System.arraycopy(moreTypes, 0, interfaces, 1, moreTypes.length);
        return (T) Proxy.newProxyInstance(type.getClassLoader(), interfaces, (proxy, method, args) -> {
            if (method.isDefault()) {
                return InvocationHandler.invokeDefault(proxy, method, args);
            }
            if (method.getDeclaringClass() == Object.class) {
                return switch (method.getName()) {
                    case "equals" -> proxy == args[0];
                    case "hashCode" -> System.identityHashCode(proxy);
                    default -> "an unused " + type.getSimpleName();
                };
            }
            if (method.getReturnType() == void.class) {
                return null;
            }
            if (method.getReturnType() == boolean.class) {
                return method.getName().equals("isRunning") && method.getParameterCount() == 0;
            }
            throw new UnsupportedOperationException(method.getName());
        });
    }
}
