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

package org.occurrent.subscription.blocking.durable.catchup;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.RedeliveringSubscriptions;
import org.occurrent.subscription.api.blocking.SubscriptionModelCapability;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What {@link RedeliveringSubscriptions#findIn(SubscriptionModelCapability)} finds in a blocking catch-up model, and
 * what it answers, for each way the catch-up models are built over a durable model. A catch-up model says what the
 * live model says, and is found only where the live model would be.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupSubscriptionModelRedeliveringCapabilityTest {

    private static final DcbCriteria QUERY = DcbCriteria.tags(Tag.parse("name:1"));

    // The model a caller holds, and the model a lookup of the capability on it is expected to find
    record Wired(SubscriptionModelCapability held, SubscriptionModelCapability found) {
    }

    static Stream<Named<Function<CheckpointAwareSubscriptionModel, Wired>>> catchUpModels() {
        return Stream.of(
                Named.of("a standalone stream catch-up model", live -> {
                    StreamCatchupSubscriptionModel model = new StreamCatchupSubscriptionModel(live, new InMemoryEventStore(), new CatchupSubscriptionModelConfig(100));
                    return new Wired(model, model);
                }),
                Named.of("a standalone DCB catch-up model", live -> {
                    DcbCatchupSubscriptionModel model = new DcbCatchupSubscriptionModel(live, new InMemoryEventStore(), QUERY, new CatchupSubscriptionModelConfig(100));
                    return new Wired(model, model);
                }),
                Named.of("the dispatching catch-up model over a stream", live -> {
                    CatchupSubscriptionModel model = new CatchupSubscriptionModel(live, new InMemoryEventStore(), new CatchupSubscriptionModelConfig(100));
                    return new Wired(model, model);
                }),
                Named.of("the dispatching catch-up model over a DCB event store", live -> {
                    CatchupSubscriptionModel model = new CatchupSubscriptionModel(live, new InMemoryEventStore(), QUERY, new CatchupSubscriptionModelConfig(100));
                    return new Wired(model, model);
                }));
    }

    @ParameterizedTest
    @MethodSource("catchUpModels")
    void a_catch_up_model_over_a_durable_model_over_a_live_model_that_redelivers_is_found_and_redelivers(Function<CheckpointAwareSubscriptionModel, Wired> wiring) {
        // Given a catch-up model over a durable model over a live model that redelivers
        Wired wired = wiring.apply(new DurableSubscriptionModel(liveModel(true), new InMemoryCheckpointStorage()));

        // When the capability is looked up
        Optional<RedeliveringSubscriptions> found = RedeliveringSubscriptions.findIn(wired.held());

        // Then the catch-up model itself is found, and it redelivers
        assertThat(found.orElse(null)).as("the redelivering model found").isSameAs(wired.found());
        assertThat(found.map(RedeliveringSubscriptions::redeliversAnAttemptNotMade)).as("whether it redelivers").contains(true);
    }

    @ParameterizedTest
    @MethodSource("catchUpModels")
    void a_catch_up_model_over_a_durable_model_over_a_live_model_that_says_it_does_not_redeliver_does_not_redeliver(Function<CheckpointAwareSubscriptionModel, Wired> wiring) {
        // Given a catch-up model over a durable model over a live model that says it doesn't redeliver
        Wired wired = wiring.apply(new DurableSubscriptionModel(liveModel(false), new InMemoryCheckpointStorage()));

        // When the capability is looked up
        Optional<RedeliveringSubscriptions> found = RedeliveringSubscriptions.findIn(wired.held());

        // Then the catch-up model is found, and it doesn't redeliver
        assertThat(found.map(RedeliveringSubscriptions::redeliversAnAttemptNotMade)).as("whether it redelivers").contains(false);
    }

    @ParameterizedTest
    @MethodSource("catchUpModels")
    void a_catch_up_model_over_a_durable_model_over_a_live_model_that_does_not_declare_it_is_not_found_and_does_not_redeliver(Function<CheckpointAwareSubscriptionModel, Wired> wiring) {
        // Given a catch-up model over a durable model over a live model that doesn't declare it
        Wired wired = wiring.apply(new DurableSubscriptionModel(liveModel(null), new InMemoryCheckpointStorage()));

        // When the capability is looked up
        Optional<RedeliveringSubscriptions> found = RedeliveringSubscriptions.findIn(wired.held());

        // Then nothing is found, and the catch-up model, asked directly, doesn't redeliver
        assertThat(found).as("the redelivering model found").isEmpty();
        assertThat(wired.held().hasCapability(RedeliveringSubscriptions.class)).as("has the capability").isFalse();
        assertThat(((RedeliveringSubscriptions) wired.held()).redeliversAnAttemptNotMade()).as("whether it redelivers, asked directly").isFalse();
    }

    // Declares RedeliveringSubscriptions with the answer given, or doesn't declare it for null
    private static CheckpointAwareSubscriptionModel liveModel(@Nullable Boolean redelivers) {
        Class<?>[] types = redelivers == null ? new Class<?>[]{CheckpointAwareSubscriptionModel.class} : new Class<?>[]{CheckpointAwareSubscriptionModel.class, RedeliveringSubscriptions.class};
        return (CheckpointAwareSubscriptionModel) Proxy.newProxyInstance(CheckpointAwareSubscriptionModel.class.getClassLoader(), types, (proxy, method, args) -> {
            if (method.getName().equals("redeliversAnAttemptNotMade")) {
                return redelivers;
            }
            if (method.isDefault()) {
                return InvocationHandler.invokeDefault(proxy, method, args);
            }
            if (method.getDeclaringClass() == Object.class) {
                return switch (method.getName()) {
                    case "equals" -> proxy == args[0];
                    case "hashCode" -> System.identityHashCode(proxy);
                    default -> "a live model";
                };
            }
            throw new UnsupportedOperationException(method.getName());
        });
    }
}
