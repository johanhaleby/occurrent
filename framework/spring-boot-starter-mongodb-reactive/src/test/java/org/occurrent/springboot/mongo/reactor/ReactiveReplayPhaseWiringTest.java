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
import org.occurrent.springboot.reactor.ComposedReplayPhase;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.springframework.context.support.GenericApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Mirrors {@link ReactiveCatchupLayerWiringTest}: the composition the durable model wraps is what
 * {@link ComposedReplayPhase} is filled from
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>
 * decision 8), so whenever a catch-up layer is composed the holder can answer, and whenever none is, it cannot.
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

            CheckpointAwareSubscriptionModel composed = OccurrentReactiveMongoAutoConfiguration.composeCatchupLayer(
                    mock(CheckpointAwareSubscriptionModel.class), new OccurrentProperties().getEventStore(),
                    applicationContext.getBeanProvider(DcbEventStore.class), applicationContext);

            ComposedReplayPhase holder = new ComposedReplayPhase();
            holder.suppliedBy(composed);

            assertThat(holder.forSubscription("some-subscription")).isPresent();
        }
    }

    @Test
    void the_holder_is_empty_when_the_composition_has_no_catchup_layer() {
        CheckpointAwareSubscriptionModel liveModel = mock(CheckpointAwareSubscriptionModel.class);

        try (GenericApplicationContext applicationContext = new GenericApplicationContext()) {
            applicationContext.registerBean("userEventStore", EventStore.class, () -> positionOrderedEventStore(false));
            applicationContext.refresh();

            CheckpointAwareSubscriptionModel composed = OccurrentReactiveMongoAutoConfiguration.composeCatchupLayer(
                    liveModel, new OccurrentProperties().getEventStore(),
                    applicationContext.getBeanProvider(DcbEventStore.class), applicationContext);

            ComposedReplayPhase holder = new ComposedReplayPhase();
            holder.suppliedBy(composed);

            assertThat(holder.forSubscription("some-subscription")).isEmpty();
        }
    }

    private static EventStore positionOrderedEventStore(boolean writesPosition) {
        EventStore eventStore = mock(EventStore.class, withSettings().extraInterfaces(PositionOrderedReader.class));
        when(((PositionOrderedReader) eventStore).writesPosition()).thenReturn(writesPosition);
        return eventStore;
    }
}
