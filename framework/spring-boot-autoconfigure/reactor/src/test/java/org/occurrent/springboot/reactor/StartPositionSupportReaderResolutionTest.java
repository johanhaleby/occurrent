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

package org.occurrent.springboot.reactor;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.springboot.common.OccurrentProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * Pins how {@link StartPositionSupport} picks the reader it asks about position support: the event store, not any reader
 * that happens to be in the context. It used to resolve the concrete {@code ReactorMongoEventStore}, which made the
 * lookup store-specific but also guaranteed a single candidate, so the store-neutral lookup has to reject a reader that
 * is not the store and refuse to guess between two that are. Picking the wrong one silently changes whether history is
 * replayed.
 * <p>
 * Container-free: the question is bean selection, so a mock reader answers it and a real MongoDB adds nothing.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StartPositionSupportReaderResolutionTest {

    @Test
    void the_event_stores_reader_is_chosen_over_a_feed_reader_in_the_same_context() {
        new ApplicationContextRunner()
                .withBean("feedReader", PositionOrderedReader.class, () -> reader(false))
                .withBean("eventStore", PositionOrderedReader.class, () -> storeReader(true))
                .run(context -> {
                    StartPositionSupport startPositionSupport = new StartPositionSupport(context);

                    // The feed reader reports false and the store reports true, so a true answer can only come from
                    // having picked the store.
                    assertThat(startPositionSupport.positionReplaySupported()).isTrue();
                });
    }

    @Test
    void a_feed_reader_on_its_own_is_not_mistaken_for_the_event_store() {
        new ApplicationContextRunner()
                .withBean("feedReader", PositionOrderedReader.class, () -> reader(true))
                .run(context -> {
                    StartPositionSupport startPositionSupport = new StartPositionSupport(context);

                    // The feed reader claims it writes a position, but it is not the store, so replay is unsupported.
                    assertThat(startPositionSupport.positionReplaySupported()).isFalse();
                });
    }

    @Test
    void no_reader_at_all_means_replay_is_unsupported() {
        new ApplicationContextRunner().run(context -> {
            StartPositionSupport startPositionSupport = new StartPositionSupport(context);

            assertThat(startPositionSupport.positionReplaySupported()).isFalse();
        });
    }

    @Test
    void a_store_that_does_not_write_a_position_means_replay_is_unsupported() {
        new ApplicationContextRunner()
                .withBean("eventStore", PositionOrderedReader.class, () -> storeReader(false))
                .run(context -> {
                    StartPositionSupport startPositionSupport = new StartPositionSupport(context);

                    assertThat(startPositionSupport.positionReplaySupported()).isFalse();
                });
    }

    @Test
    void two_event_stores_that_both_read_in_position_order_fail_loud_instead_of_picking_one() {
        new ApplicationContextRunner()
                .withBean("firstEventStore", PositionOrderedReader.class, () -> storeReader(true))
                .withBean("secondEventStore", PositionOrderedReader.class, () -> storeReader(true))
                .run(context -> {
                    StartPositionSupport startPositionSupport = new StartPositionSupport(context);

                    // Both write a position, so nothing here depends on writesPosition. The point is that picking by
                    // registration order is a coin flip dressed up as a decision, so both names are reported instead.
                    assertThatThrownBy(startPositionSupport::positionReplaySupported)
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("firstEventStore")
                            .hasMessageContaining("secondEventStore");
                });
    }

    @Test
    void a_store_that_writes_a_position_without_the_stream_capability_does_not_support_stream_history_replay() {
        OccurrentProperties occurrentProperties = new OccurrentProperties();
        occurrentProperties.getEventStore().setCapabilities(Set.of(EventStoreCapability.DCB));

        new ApplicationContextRunner()
                .withBean("eventStore", PositionOrderedReader.class, () -> storeReader(true))
                .withBean(OccurrentProperties.class, () -> occurrentProperties)
                .run(context -> {
                    StartPositionSupport startPositionSupport = new StartPositionSupport(context);

                    // A DCB-only store writes a position, so the reader alone says yes. Stream replay also needs stream
                    // events to replay, which this store has none of.
                    assertThat(startPositionSupport.positionReplaySupported()).isTrue();
                    assertThat(startPositionSupport.streamHistoryReplaySupported()).isFalse();
                });
    }

    private static PositionOrderedReader reader(boolean writesPosition) {
        PositionOrderedReader reader = mock(PositionOrderedReader.class);
        when(reader.writesPosition()).thenReturn(writesPosition);
        return reader;
    }

    // Implements both, which is what makes it the store rather than a feed: ReactorMongoEventStore implements
    // EventStore and PositionOrderedReader the same way.
    private static PositionOrderedReader storeReader(boolean writesPosition) {
        PositionOrderedReader reader = mock(PositionOrderedReader.class, withSettings().extraInterfaces(EventStore.class));
        when(reader.writesPosition()).thenReturn(writesPosition);
        return reader;
    }
}
