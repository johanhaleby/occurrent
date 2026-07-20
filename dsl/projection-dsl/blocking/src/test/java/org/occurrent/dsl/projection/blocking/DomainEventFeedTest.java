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

package org.occurrent.dsl.projection.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;

import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

@DisplayNameGeneration(ReplaceUnderscores.class)
class DomainEventFeedTest {

    record Counted(String eventId) {
    }

    @Test
    void registering_two_projections_with_the_same_id_throws() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId);

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        feed.register("counter", projection(), repository);

        Throwable thrown = catchThrowable(() -> feed.register("counter", projection(), repository));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("counter").hasMessageContaining("already registered");
    }

    @Test
    void registering_with_a_null_id_throws_instead_of_failing_inside_the_id_set() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId);

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);

        Throwable thrown = catchThrowable(() -> feed.register(null, projection(), repository));

        assertThat(thrown).isInstanceOf(NullPointerException.class).hasMessageContaining("id cannot be null");
    }

    @Test
    void a_failed_registration_does_not_permanently_reserve_the_id() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(readerThatDoesNotWritePosition(store), converter, Counted::eventId);

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);

        Throwable firstAttempt = catchThrowable(() -> feed.register("counter", projection(), repository));
        assertThat(firstAttempt).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("does not write positions");

        // A second attempt with the same id must fail the same way, not with "already registered": the first attempt's
        // feed was never created, so the id must not have been reserved.
        Throwable secondAttempt = catchThrowable(() -> feed.register("counter", projection(), repository));

        assertThat(secondAttempt).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not write positions")
                .hasMessageNotContaining("already registered");
    }

    private static PositionOrderedReader readerThatDoesNotWritePosition(PositionOrderedReader delegate) {
        return new PositionOrderedReader() {
            @Override
            public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return delegate.readInPositionOrder(filter, range);
            }

            @Override
            public long currentPosition() {
                return delegate.currentPosition();
            }

            @Override
            public boolean writesPosition() {
                return false;
            }
        };
    }

    @Test
    void registering_two_projections_with_different_ids_does_not_throw() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId);

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        feed.register("counter-1", projection(), repository);

        Throwable thrown = catchThrowable(() -> feed.register("counter-2", projection(), repository));

        assertThat(thrown).isNull();
    }

    private static Projection<Integer, Counted, String> projection() {
        return Projection.<Integer, Counted, String>builder(0)
                .id(event -> "counter")
                .on(Counted.class, (state, event) -> state + 1)
                .build();
    }

    private static CloudEventConverter<Counted> counterConverter() {
        return new JacksonCloudEventConverter.Builder<Counted>(new ObjectMapper(), URI.create("urn:occurrent:test")).idMapper(Counted::eventId).build();
    }
}
