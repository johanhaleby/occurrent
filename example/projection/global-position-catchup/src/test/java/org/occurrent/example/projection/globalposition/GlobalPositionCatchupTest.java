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

package org.occurrent.example.projection.globalposition;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.blocking.generic.GenericApplicationService;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.Name;
import org.occurrent.dsl.query.blocking.DomainEventQueries;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.application.composition.command.partial.PartialFunctionApplication.partial;

/**
 * Demonstrates the global position feature: every event gets a single monotonic position no matter which stream it
 * belongs to. The example writes events to three streams (one per person), interleaved, then shows:
 * <p>
 * 1. Reading events across streams in write order via {@code afterPosition} and {@code readInPositionOrder} with a
 * {@link PositionRange}.
 * 2. Rebuilding a projection by replaying events in position order, giving cross-stream write order instead of
 * per-stream grouping.
 * 3. That a store using {@code withoutStreamPosition()} carries no position and rejects the position-based reads.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class GlobalPositionCatchupTest {

    private static final URI SOURCE = URI.create("urn:occurrent:example:global-position");

    @Test
    void events_written_to_different_streams_can_be_read_back_in_a_single_global_position_order() {
        // Given a position-enabled event store
        InMemoryEventStore eventStore = new InMemoryEventStore().withStreamPosition();
        CloudEventConverter<DomainEvent> cloudEventConverter = domainEventConverter();
        ApplicationService<DomainEvent> applicationService = new GenericApplicationService<>(eventStore, cloudEventConverter);
        DomainEventQueries<DomainEvent> queries = new DomainEventQueries<>(eventStore, cloudEventConverter);

        LocalDateTime time = LocalDateTime.now();

        // When events are written to three different streams, interleaved with each other
        applicationService.execute("alice", partial(Name::defineName, "event-1", time, "alice", "Alice"));       // position 1
        applicationService.execute("bob", partial(Name::defineName, "event-2", time, "bob", "Bob"));             // position 2
        applicationService.execute("alice", partial(Name::changeName, "event-3", time, "alice", "Alice Smith")); // position 3
        applicationService.execute("carol", partial(Name::defineName, "event-4", time, "carol", "Carol"));       // position 4
        applicationService.execute("bob", partial(Name::changeName, "event-5", time, "bob", "Bob Jones"));       // position 5

        // Then the query DSL reads events across all three streams in a single, unified position order
        List<DomainEvent> all = queries.readInPositionOrder(Filter.all(), PositionRange.fromBeginning()).toList();
        assertThat(all).extracting(DomainEvent::eventId)
                .containsExactly("event-1", "event-2", "event-3", "event-4", "event-5");

        // And afterPosition lets a reader resume from a known point, again across streams
        List<DomainEvent> afterFirstTwo = queries.afterPosition(2).toList();
        assertThat(afterFirstTwo).extracting(DomainEvent::eventId)
                .containsExactly("event-3", "event-4", "event-5");

        // And currentPosition reports the store's position high-watermark
        assertThat(queries.currentPosition()).isEqualTo(5L);
    }

    @Test
    void a_projection_can_be_rebuilt_from_scratch_by_replaying_events_in_global_position_order_across_streams() {
        // Given a position-enabled event store with events written to multiple streams, interleaved
        InMemoryEventStore eventStore = new InMemoryEventStore().withStreamPosition();
        CloudEventConverter<DomainEvent> cloudEventConverter = domainEventConverter();
        ApplicationService<DomainEvent> applicationService = new GenericApplicationService<>(eventStore, cloudEventConverter);
        DomainEventQueries<DomainEvent> queries = new DomainEventQueries<>(eventStore, cloudEventConverter);

        LocalDateTime time = LocalDateTime.now();

        applicationService.execute("alice", partial(Name::defineName, "event-1", time, "alice", "Alice"));
        applicationService.execute("bob", partial(Name::defineName, "event-2", time, "bob", "Bob"));
        applicationService.execute("alice", partial(Name::changeName, "event-3", time, "alice", "Alice Smith"));
        applicationService.execute("bob", partial(Name::changeName, "event-4", time, "bob", "Bob Jones"));

        // When a new projection is rebuilt from position 0, reading in global position order rather than per stream
        NameProjection projection = new NameProjection();
        queries.readInPositionOrder(Filter.all(), PositionRange.fromBeginning())
                .forEach(projection::apply);

        // Then the projection has replayed every event, across both streams, in the exact order they were written
        assertThat(projection.appliedEventIdsInOrder())
                .containsExactly("event-1", "event-2", "event-3", "event-4");

        // And the projection's final state reflects the last event per person
        assertThat(projection.currentNameOf("alice")).isEqualTo("Alice Smith");
        assertThat(projection.currentNameOf("bob")).isEqualTo("Bob Jones");
    }

    @Test
    void a_store_that_opts_out_of_stream_position_does_not_carry_a_position_and_rejects_position_reads() {
        // Given a store that explicitly opts out of stream position tracking
        InMemoryEventStore eventStoreWithoutPosition = new InMemoryEventStore().withoutStreamPosition();
        CloudEventConverter<DomainEvent> cloudEventConverter = domainEventConverter();
        ApplicationService<DomainEvent> applicationService = new GenericApplicationService<>(eventStoreWithoutPosition, cloudEventConverter);
        DomainEventQueries<DomainEvent> queries = new DomainEventQueries<>(eventStoreWithoutPosition, cloudEventConverter);

        LocalDateTime time = LocalDateTime.now();
        applicationService.execute("alice", partial(Name::defineName, "event-1", time, "alice", "Alice"));

        // Then the position-based read APIs are rejected rather than silently returning an empty or wrong result
        assertThatThrownBy(() -> queries.afterPosition(0))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> queries.readInPositionOrder(Filter.all(), PositionRange.fromBeginning()))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(queries::currentPosition)
                .isInstanceOf(UnsupportedOperationException.class);

        // Whereas ordinary, non-position reads still work fine
        assertThat(queries.all().toList()).extracting(DomainEvent::eventId).containsExactly("event-1");
    }

    private static CloudEventConverter<DomainEvent> domainEventConverter() {
        return new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), SOURCE)
                .idMapper(DomainEvent::eventId)
                .build();
    }
}
