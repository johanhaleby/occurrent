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
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.reactor.durable.catchup.ReactorCatchupSubscriptionModel;
import org.springframework.context.support.GenericApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * Guards the invariant that ties the neutral replay probe to this starter's wiring: whenever the probe reports that
 * history replay is supported, a catch-up model must actually have been layered in. A user-supplied reactive event store
 * used to break it, because the probe asked whether any {@code EventStore} reads in position order while the wiring
 * asked for the concrete MongoDB store, so replay was promised and then silently skipped over a bare change stream.
 * <p>
 * Container-free: the composition is decided from bean types and properties, so mocks answer it and a real MongoDB adds
 * nothing. The live model is mocked for the same reason.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactiveCatchupLayerWiringTest {

    @Test
    void a_user_supplied_event_store_that_reads_in_position_order_gets_a_catchup_layer() {
        try (GenericApplicationContext applicationContext = new GenericApplicationContext()) {
            applicationContext.registerBean("userEventStore", EventStore.class, () -> positionOrderedEventStore(true));
            applicationContext.refresh();

            CheckpointAwareSubscriptionModel model = OccurrentReactiveMongoAutoConfiguration.composeCatchupLayer(
                    mock(CheckpointAwareSubscriptionModel.class), new OccurrentProperties().getEventStore(),
                    applicationContext.getBeanProvider(DcbEventStore.class), applicationContext);

            assertThat(model).isInstanceOf(ReactorCatchupSubscriptionModel.class);
        }
    }

    @Test
    void an_event_store_that_writes_no_position_gets_no_catchup_layer() {
        CheckpointAwareSubscriptionModel liveModel = mock(CheckpointAwareSubscriptionModel.class);

        try (GenericApplicationContext applicationContext = new GenericApplicationContext()) {
            applicationContext.registerBean("userEventStore", EventStore.class, () -> positionOrderedEventStore(false));
            applicationContext.refresh();

            CheckpointAwareSubscriptionModel model = OccurrentReactiveMongoAutoConfiguration.composeCatchupLayer(
                    liveModel, new OccurrentProperties().getEventStore(),
                    applicationContext.getBeanProvider(DcbEventStore.class), applicationContext);

            // The other half of the invariant: no replay wired here, and the probe reports none either, so a
            // BEGINNING_OF_TIME subscription fails at startup rather than losing history.
            assertThat(model).isSameAs(liveModel);
        }
    }

    private static EventStore positionOrderedEventStore(boolean writesPosition) {
        EventStore eventStore = mock(EventStore.class, withSettings().extraInterfaces(PositionOrderedReader.class));
        when(((PositionOrderedReader) eventStore).writesPosition()).thenReturn(writesPosition);
        return eventStore;
    }
}
