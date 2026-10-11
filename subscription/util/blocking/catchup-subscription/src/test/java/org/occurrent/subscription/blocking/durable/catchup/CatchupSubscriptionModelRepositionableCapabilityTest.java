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

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.SubscriptionModelCapability;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Whether {@link RepositionableSubscriptions#findIn(SubscriptionModelCapability)} finds a blocking catch-up model, and
 * which model it finds, for each way the catch-up models are built over a durable model.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupSubscriptionModelRepositionableCapabilityTest {

    private static final DcbCriteria QUERY = DcbCriteria.tags(Tag.parse("name:1"));

    // The model a caller holds, and the model a lookup of the capability on it is expected to find
    record Wired(SubscriptionModelCapability held, SubscriptionModelCapability found) {
    }

    static Stream<Named<Function<CheckpointAwareSubscriptionModel, Wired>>> catchUpModels() {
        return Stream.concat(standaloneCatchUpModels(), dispatchingCatchUpModels());
    }

    static Stream<Named<Function<CheckpointAwareSubscriptionModel, Wired>>> standaloneCatchUpModels() {
        return Stream.of(
                Named.of("a standalone stream catch-up model", live -> {
                    StreamCatchupSubscriptionModel model = new StreamCatchupSubscriptionModel(live, new InMemoryEventStore(), new CatchupSubscriptionModelConfig(100));
                    return new Wired(model, model);
                }),
                Named.of("a standalone DCB catch-up model", live -> {
                    DcbCatchupSubscriptionModel model = new DcbCatchupSubscriptionModel(live, new InMemoryEventStore(), QUERY, new CatchupSubscriptionModelConfig(100));
                    return new Wired(model, model);
                }));
    }

    static Stream<Named<Function<CheckpointAwareSubscriptionModel, Wired>>> dispatchingCatchUpModels() {
        return Stream.of(
                Named.of("the dispatching catch-up model over a stream", live -> {
                    CatchupSubscriptionModel model = new CatchupSubscriptionModel(live, new InMemoryEventStore(), new CatchupSubscriptionModelConfig(100));
                    return new Wired(model, model);
                }),
                Named.of("the dispatching catch-up model over a DCB event store", live -> {
                    CatchupSubscriptionModel model = new CatchupSubscriptionModel(live, new InMemoryEventStore(), QUERY, new CatchupSubscriptionModelConfig(100));
                    return new Wired(model, model);
                }),
                Named.of("the dispatching catch-up model over both", live -> {
                    InMemoryEventStore store = new InMemoryEventStore();
                    CatchupSubscriptionModel model = new CatchupSubscriptionModel(live, store, store, QUERY, new CatchupSubscriptionModelConfig(100));
                    return new Wired(model, model);
                }));
    }

    @ParameterizedTest
    @MethodSource("catchUpModels")
    void a_catch_up_model_over_a_durable_model_over_a_live_model_that_can_be_repositioned_is_found_as_the_repositionable_model_itself(Function<CheckpointAwareSubscriptionModel, Wired> wiring) {
        // Given a catch-up model over a durable model over a live model that can be repositioned
        Wired wired = wiring.apply(new DurableSubscriptionModel(new CatchupSubscriptionModelStopAndStartTest.FakeLiveModel(), new InMemoryCheckpointStorage()));

        // When the capability is looked up
        Optional<RepositionableSubscriptions> found = RepositionableSubscriptions.findIn(wired.held());

        // Then the catch-up model itself is found, not the live model it resumes in
        assertThat(found.orElse(null)).as("the repositionable model found").isSameAs(wired.found());
    }

    @ParameterizedTest
    @MethodSource("standaloneCatchUpModels")
    void a_standalone_catch_up_model_over_a_durable_model_over_a_live_model_that_cannot_be_repositioned_is_not_found_as_repositionable(Function<CheckpointAwareSubscriptionModel, Wired> wiring) {
        // Given a standalone catch-up model over a durable model over a live model that cannot be repositioned
        Wired wired = wiring.apply(new DurableSubscriptionModel(notRepositionable(), new InMemoryCheckpointStorage()));

        // When the capability is looked up
        Optional<RepositionableSubscriptions> found = RepositionableSubscriptions.findIn(wired.held());

        // Then nothing is found
        assertThat(found).as("the repositionable model found").isEmpty();
        assertThat(wired.held().hasCapability(RepositionableSubscriptions.class)).as("has the capability").isFalse();
    }

    @ParameterizedTest
    @MethodSource("dispatchingCatchUpModels")
    void the_dispatching_catch_up_model_over_a_durable_model_over_a_live_model_that_cannot_be_repositioned_is_still_found_as_repositionable_and_refuses_the_resume_at_a_position_naming_the_live_model(Function<CheckpointAwareSubscriptionModel, Wired> wiring) {
        // Given a dispatching catch-up model over a durable model over a live model that cannot be repositioned
        Wired wired = wiring.apply(new DurableSubscriptionModel(notRepositionable(), new InMemoryCheckpointStorage()));

        // When the capability is looked up, and the subscription is resumed at a position through what is found
        Optional<RepositionableSubscriptions> found = RepositionableSubscriptions.findIn(wired.held());
        Throwable failure = catchThrowable(() -> found.orElseThrow().resumeSubscription("id", StartAt.subscriptionModelDefault()));

        // Then the dispatching model itself is found, and the resume is refused as unsupported, naming the live model
        assertThat(found.orElse(null)).as("the repositionable model found").isSameAs(wired.found());
        assertThat(failure).as("the resume at a position").isInstanceOf(UnsupportedOperationException.class).hasMessage("DurableSubscriptionModel is not repositionable");
    }

    @ParameterizedTest
    @MethodSource("catchUpModels")
    void a_resume_at_a_position_on_a_catch_up_model_over_a_live_model_that_cannot_be_repositioned_is_refused_naming_the_live_model(Function<CheckpointAwareSubscriptionModel, Wired> wiring) {
        // Given a catch-up model over a durable model over a live model that cannot be repositioned
        Wired wired = wiring.apply(new DurableSubscriptionModel(notRepositionable(), new InMemoryCheckpointStorage()));

        // When the subscription is resumed at a position directly on the catch-up model
        assertThat(wired.held()).as("the catch-up model").isInstanceOf(RepositionableSubscriptions.class);
        Throwable failure = catchThrowable(() -> ((RepositionableSubscriptions) wired.held()).resumeSubscription("id", StartAt.subscriptionModelDefault()));

        // Then it is refused as unsupported, naming the model that cannot be repositioned
        assertThat(failure).as("the resume at a position").isInstanceOf(UnsupportedOperationException.class).hasMessage("DurableSubscriptionModel is not repositionable");
    }

    private static CheckpointAwareSubscriptionModel notRepositionable() {
        return (CheckpointAwareSubscriptionModel) Proxy.newProxyInstance(CheckpointAwareSubscriptionModel.class.getClassLoader(), new Class<?>[]{CheckpointAwareSubscriptionModel.class}, (proxy, method, args) -> {
            if (method.isDefault()) {
                return InvocationHandler.invokeDefault(proxy, method, args);
            }
            if (method.getDeclaringClass() == Object.class) {
                return switch (method.getName()) {
                    case "equals" -> proxy == args[0];
                    case "hashCode" -> System.identityHashCode(proxy);
                    default -> "a live model that cannot be repositioned";
                };
            }
            throw new UnsupportedOperationException(method.getName());
        });
    }
}
