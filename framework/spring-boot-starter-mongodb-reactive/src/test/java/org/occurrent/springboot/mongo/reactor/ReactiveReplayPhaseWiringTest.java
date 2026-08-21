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
import org.occurrent.dsl.projection.CatchupPhase;
import org.occurrent.dsl.projection.CatchupSnapshot;
import org.occurrent.dsl.projection.ReplayPhase;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.springboot.reactor.ComposedReplayPhase;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Mirrors {@link ReactiveCatchupLayerWiringTest}: the composition the durable model wraps is what
 * {@link ComposedReplayPhase} is filled from
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>
 * decision 8), so whenever a catch-up layer is composed the holder can answer, and whenever none is, the holder
 * still answers, with the known {@link ReplayPhase#neverReplays()} rather than an unresolved empty result.
 * <p>
 * Calls the real {@code occurrentDurableSubscriptionModel} bean method rather than reproducing its composition and
 * calling {@code suppliedBy} directly, so a regression that stops the bean method from filling the holder fails
 * here instead of passing.
 * <p>
 * Container-free for the same reason {@link ReactiveCatchupLayerWiringTest} is: the composition is decided from bean
 * types and properties, so mocks answer it and a real MongoDB adds nothing.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactiveReplayPhaseWiringTest {

    @Test
    void the_holder_answers_for_a_subscription_when_the_composition_has_a_catchup_layer() {
        try (GenericApplicationContext applicationContext = new GenericApplicationContext()) {
            applicationContext.registerBean("userEventStore", EventStore.class, () -> positionOrderedEventStore(true));
            applicationContext.refresh();

            ComposedReplayPhase holder = fillHolderThroughTheBeanMethod(applicationContext);

            assertThat(holder.forSubscription("some-subscription")).isPresent();
        }
    }

    @Test
    void the_holder_answers_neverReplays_when_the_composition_has_no_catchup_layer() {
        try (GenericApplicationContext applicationContext = new GenericApplicationContext()) {
            applicationContext.registerBean("userEventStore", EventStore.class, () -> positionOrderedEventStore(false));
            applicationContext.refresh();

            ComposedReplayPhase holder = fillHolderThroughTheBeanMethod(applicationContext);

            Optional<ReplayPhase> phase = holder.forSubscription("some-subscription");
            assertThat(phase).isPresent();
            assertThat(phase.get().current()).isEqualTo(CatchupSnapshot.LIVE);
        }
    }

    // Calls OccurrentReactiveMongoAutoConfiguration.occurrentDurableSubscriptionModel(..) itself, the production
    // bean method, rather than reproducing its composeCatchupLayer(..) call and filling the holder by hand, so a
    // regression that stops the bean method from calling suppliedBy fails this test instead of passing it.
    private static ComposedReplayPhase fillHolderThroughTheBeanMethod(GenericApplicationContext applicationContext) {
        ComposedReplayPhase holder = new ComposedReplayPhase();
        OccurrentReactiveMongoAutoConfiguration<Object> autoConfiguration = new OccurrentReactiveMongoAutoConfiguration<>();
        autoConfiguration.occurrentDurableSubscriptionModel(mock(ReactiveMongoOperations.class), mock(CheckpointStorage.class),
                new OccurrentProperties(), applicationContext.getBeanProvider(DcbEventStore.class), applicationContext, holder);
        return holder;
    }

    private static EventStore positionOrderedEventStore(boolean writesPosition) {
        EventStore eventStore = mock(EventStore.class, withSettings().extraInterfaces(PositionOrderedReader.class));
        when(((PositionOrderedReader) eventStore).writesPosition()).thenReturn(writesPosition);
        return eventStore;
    }
}
