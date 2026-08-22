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

package org.occurrent.springboot.mongo.reactor;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.springboot.reactor.ComposedCatchupModel;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.SubscriptionModelCapability;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Mirrors {@link ReactiveCatchupLayerWiringTest}: the composition the durable model wraps is what
 * {@link ComposedCatchupModel} is filled from
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>
 * decision 8), so whenever a catch-up layer is composed the holder hands out the model that owns it, and whenever
 * none is, the holder still counts as filled, which is what tells a caller that the absence is a known fact about
 * this composition rather than an unresolved question.
 * <p>
 * Calls the real {@code occurrentDurableSubscriptionModel} bean method rather than reproducing its composition and
 * calling {@code suppliedBy} directly, so a regression that stops the bean method from filling the holder fails
 * here instead of passing.
 * <p>
 * Container-free for the same reason {@link ReactiveCatchupLayerWiringTest} is: the composition is decided from bean
 * types and properties, so mocks answer it and a real MongoDB adds nothing.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactiveComposedCatchupModelWiringTest {

    @Test
    void the_holder_hands_out_the_catchup_model_when_the_composition_has_a_catchup_layer() {
        try (GenericApplicationContext applicationContext = new GenericApplicationContext()) {
            applicationContext.registerBean("userEventStore", EventStore.class, () -> positionOrderedEventStore(true));
            applicationContext.refresh();

            ComposedCatchupModel holder = fillHolderThroughTheBeanMethod(applicationContext);

            assertThat(holder.isSupplied()).isTrue();
            assertThat(holder.catchupModel()).isPresent();
        }
    }

    @Test
    void the_holder_counts_as_filled_with_no_model_when_the_composition_has_no_catchup_layer() {
        try (GenericApplicationContext applicationContext = new GenericApplicationContext()) {
            applicationContext.registerBean("userEventStore", EventStore.class, () -> positionOrderedEventStore(false));
            applicationContext.refresh();

            ComposedCatchupModel holder = fillHolderThroughTheBeanMethod(applicationContext);

            // Filled, so the absence is this composition's own known fact, which is what separates it from a
            // composition nothing here can see into.
            assertThat(holder.isSupplied()).isTrue();
            assertThat(holder.catchupModel()).isEmpty();
        }
    }

    @Test
    void the_holder_is_identified_with_the_exact_durable_model_the_bean_method_returns() {
        // Issue 903: identifiedAs must be given the durable model the bean method actually returns, not
        // catchupLayer (suppliedBy's argument, one layer further in). A regression that passed catchupLayer to
        // identifiedAs instead would still pass every other test in this file and in
        // ProjectionAnnotationRecordAppliedAppendsWarningTest, since those all build the holder by hand.
        try (GenericApplicationContext applicationContext = new GenericApplicationContext()) {
            applicationContext.registerBean("userEventStore", EventStore.class, () -> positionOrderedEventStore(true));
            applicationContext.refresh();

            ComposedCatchupModel holder = new ComposedCatchupModel();
            Object durableModel = composeThroughTheBeanMethod(applicationContext, holder);

            assertThat(durableModel).isInstanceOf(SubscriptionModelCapability.class);
            assertThat(holder.isDefaultKnownLiveOnlyFor((SubscriptionModelCapability) durableModel)).isTrue();
        }
    }

    // Calls OccurrentReactiveMongoAutoConfiguration.occurrentDurableSubscriptionModel(..) itself, the production
    // bean method, rather than reproducing its composeCatchupLayer(..) call and filling the holder by hand, so a
    // regression that stops the bean method from calling suppliedBy or identifiedAs fails this test instead of
    // passing it.
    private static ComposedCatchupModel fillHolderThroughTheBeanMethod(GenericApplicationContext applicationContext) {
        ComposedCatchupModel holder = new ComposedCatchupModel();
        composeThroughTheBeanMethod(applicationContext, holder);
        return holder;
    }

    private static Object composeThroughTheBeanMethod(GenericApplicationContext applicationContext, ComposedCatchupModel holder) {
        OccurrentReactiveMongoAutoConfiguration<Object> autoConfiguration = new OccurrentReactiveMongoAutoConfiguration<>();
        return autoConfiguration.occurrentDurableSubscriptionModel(mock(ReactiveMongoOperations.class), mock(CheckpointStorage.class),
                new OccurrentProperties(), applicationContext.getBeanProvider(DcbEventStore.class), applicationContext, holder);
    }

    private static EventStore positionOrderedEventStore(boolean writesPosition) {
        EventStore eventStore = mock(EventStore.class, withSettings().extraInterfaces(PositionOrderedReader.class));
        when(((PositionOrderedReader) eventStore).writesPosition()).thenReturn(writesPosition);
        return eventStore;
    }
}
