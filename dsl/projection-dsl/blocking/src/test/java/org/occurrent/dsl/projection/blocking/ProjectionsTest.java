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
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.query.blocking.DomainEventQueries;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.net.URI;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

@DisplayNameGeneration(ReplaceUnderscores.class)
class ProjectionsTest {

    private static final URI SOURCE = URI.create("urn:occurrent:test");

    record Counted(String eventId) {
    }

    @Test
    void project_folds_all_matching_events_on_demand_for_a_singleton_projection() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        store.write("s", converter.toCloudEvents(List.of(new Counted("1"), new Counted("2"), new Counted("3"))));
        DomainEventQueries<Counted> queries = new DomainEventQueries<>(store, converter);

        Integer count = Projections.project(singletonProjection(), queries);

        assertThat(count).isEqualTo(3);
    }

    @Test
    void project_without_an_instance_id_rejects_a_keyed_projection() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        DomainEventQueries<Counted> queries = new DomainEventQueries<>(store, converter);

        Throwable thrown = catchThrowable(() -> Projections.project(keyedProjection(), queries));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("keyed");
    }

    @Test
    void project_with_a_null_instance_id_throws_instead_of_failing_inside_the_filter() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        DomainEventQueries<Counted> queries = new DomainEventQueries<>(store, converter);

        Throwable thrown = catchThrowable(() -> Projections.project(keyedProjection(), queries, null));

        assertThat(thrown).isInstanceOf(NullPointerException.class).hasMessageContaining("instanceId cannot be null");
    }

    @Test
    void project_with_an_instance_id_folds_only_the_events_for_that_instance() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        store.write("s", converter.toCloudEvents(List.of(new Counted("a-1"), new Counted("b-1"), new Counted("a-2"))));
        DomainEventQueries<Counted> queries = new DomainEventQueries<>(store, converter);

        Integer countForA = Projections.project(keyedProjection(), queries, "a");

        assertThat(countForA).isEqualTo(2);
    }

    private static Projection<Integer, Counted, String> singletonProjection() {
        return Projection.<Integer, Counted>singletonBuilder(0)
                .on(Counted.class, (state, event) -> state + 1)
                .build();
    }

    private static Projection<Integer, Counted, String> keyedProjection() {
        return Projection.<Integer, Counted, String>builder(0)
                .id(event -> event.eventId().split("-")[0])
                .on(Counted.class, (state, event) -> state + 1)
                .build();
    }

    private static CloudEventConverter<Counted> countedConverter() {
        return new JacksonCloudEventConverter.Builder<Counted>(new ObjectMapper(), SOURCE).idMapper(Counted::eventId).build();
    }
}
