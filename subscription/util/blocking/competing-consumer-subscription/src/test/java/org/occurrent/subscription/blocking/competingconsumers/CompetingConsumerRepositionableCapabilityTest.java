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

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionModelCapability;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.occurrent.subscription.blocking.durable.catchup.StreamCatchupSubscriptionModel;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * {@link RepositionableSubscriptions#findIn(SubscriptionModelCapability)} finds nothing on a competing consumer model,
 * for each catch-up model it can wrap, over a durable model. A resume at a position through the model beneath it would
 * go around the lease bookkeeping the competing consumer model keeps for the subscription (#1251).
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerRepositionableCapabilityTest {

    private static final DcbCriteria QUERY = DcbCriteria.tags(Tag.parse("name:1"));

    // How the model a competing consumer model wraps is built over a durable model
    static Stream<Named<Function<CheckpointAwareSubscriptionModel, SubscriptionModel>>> wirings() {
        return Stream.concat(wiringsWithoutADispatcher(), dispatcherWirings());
    }

    static Stream<Named<Function<CheckpointAwareSubscriptionModel, SubscriptionModel>>> wiringsWithoutADispatcher() {
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100);
        return Stream.of(
                Named.of("a standalone stream catch-up model", live -> new StreamCatchupSubscriptionModel(live, unused(EventStoreQueries.class), config)),
                Named.of("a durable model", live -> live));
    }

    static Stream<Named<Function<CheckpointAwareSubscriptionModel, SubscriptionModel>>> dispatcherWirings() {
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100);
        return Stream.of(
                Named.of("the dispatching catch-up model over a stream", live -> new CatchupSubscriptionModel(live, unused(EventStoreQueries.class), config)),
                Named.of("the dispatching catch-up model over a DCB event store", live -> new CatchupSubscriptionModel(live, unused(DcbEventStore.class), QUERY, config)));
    }

    @ParameterizedTest
    @MethodSource("wirings")
    void a_competing_consumer_model_over_a_durable_model_over_a_live_model_that_can_be_repositioned_finds_nothing(Function<CheckpointAwareSubscriptionModel, SubscriptionModel> wiring) {
        // Given a competing consumer model over a model over a durable model over a live model that can be repositioned
        SubscriptionModel wrapped = wiring.apply(new DurableSubscriptionModel(live(true), new InMemoryCheckpointStorage()));
        CompetingConsumerSubscriptionModel competingConsumer = new CompetingConsumerSubscriptionModel(wrapped, new NoLeases());

        // When the capability is looked up
        Optional<RepositionableSubscriptions> found = RepositionableSubscriptions.findIn(competingConsumer);

        // Then nothing is found, although the model it wraps can resume at a position
        assertThat(found).as("the repositionable model found").isEmpty();
    }

    @ParameterizedTest
    @MethodSource("wiringsWithoutADispatcher")
    void a_competing_consumer_model_over_a_durable_model_over_a_live_model_that_cannot_be_repositioned_finds_nothing(Function<CheckpointAwareSubscriptionModel, SubscriptionModel> wiring) {
        // Given a competing consumer model over a model over a durable model over a live model that cannot be repositioned
        SubscriptionModel wrapped = wiring.apply(new DurableSubscriptionModel(live(false), new InMemoryCheckpointStorage()));
        CompetingConsumerSubscriptionModel competingConsumer = new CompetingConsumerSubscriptionModel(wrapped, new NoLeases());

        // When the capability is looked up
        Optional<RepositionableSubscriptions> found = RepositionableSubscriptions.findIn(competingConsumer);

        // Then nothing is found
        assertThat(found).as("the repositionable model found").isEmpty();
    }

    @ParameterizedTest
    @MethodSource("dispatcherWirings")
    void a_competing_consumer_model_over_the_dispatching_catch_up_model_over_a_live_model_that_cannot_be_repositioned_finds_nothing_while_the_dispatching_model_refuses_a_resume_at_a_position_naming_the_durable_model(Function<CheckpointAwareSubscriptionModel, SubscriptionModel> wiring) {
        // Given a competing consumer model over the dispatching model over a durable model over a live model that cannot be repositioned
        SubscriptionModel wrapped = wiring.apply(new DurableSubscriptionModel(live(false), new InMemoryCheckpointStorage()));
        CompetingConsumerSubscriptionModel competingConsumer = new CompetingConsumerSubscriptionModel(wrapped, new NoLeases());

        // When the capability is looked up, and the subscription is resumed at a position directly on the dispatching model
        Optional<RepositionableSubscriptions> found = RepositionableSubscriptions.findIn(competingConsumer);
        assertThat(wrapped).as("the dispatching model").isInstanceOf(RepositionableSubscriptions.class);
        Throwable failure = catchThrowable(() -> ((RepositionableSubscriptions) wrapped).resumeSubscription("id", StartAt.subscriptionModelDefault()));

        // Then nothing is found, and the resume on the dispatching model is refused as unsupported, naming the durable model
        assertThat(found).as("the repositionable model found").isEmpty();
        assertThat(failure).as("the resume at a position").isInstanceOf(UnsupportedOperationException.class).hasMessage("DurableSubscriptionModel is not repositionable");
    }

    private static CheckpointAwareSubscriptionModel live(boolean repositionable) {
        return repositionable ? unused(CheckpointAwareSubscriptionModel.class, RepositionableSubscriptions.class) : unused(CheckpointAwareSubscriptionModel.class);
    }

    // An implementation of the interfaces that answers the methods an interface defaults and the ones returning a boolean, and fails every other call
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
            if (method.getReturnType() == boolean.class) {
                return false;
            }
            throw new UnsupportedOperationException(method.getName());
        });
    }
}
