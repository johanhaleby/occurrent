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

package org.occurrent.dsl.projection.reactor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.query.reactor.DomainEventQueries;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.reactor.EventStoreQueries;
import org.occurrent.filter.Filter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

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
        CloudEventConverter<Counted> converter = countedConverter();
        DomainEventQueries<Counted> queries = queriesOver(converter, new Counted("1"), new Counted("2"), new Counted("3"));

        Mono<Integer> count = Projections.project(singletonProjection(), queries);

        StepVerifier.create(count).expectNext(3).verifyComplete();
    }

    @Test
    void project_without_an_instance_id_rejects_a_keyed_projection() {
        CloudEventConverter<Counted> converter = countedConverter();
        DomainEventQueries<Counted> queries = queriesOver(converter);

        Throwable thrown = catchThrowable(() -> Projections.project(keyedProjection(), queries).block());

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("keyed");
    }

    @Test
    void project_with_a_null_instance_id_throws_instead_of_failing_inside_the_filter() {
        CloudEventConverter<Counted> converter = countedConverter();
        DomainEventQueries<Counted> queries = queriesOver(converter);

        Throwable thrown = catchThrowable(() -> Projections.project(keyedProjection(), queries, null));

        assertThat(thrown).isInstanceOf(NullPointerException.class).hasMessageContaining("instanceId cannot be null");
    }

    @Test
    void project_with_an_instance_id_folds_only_the_events_for_that_instance() {
        CloudEventConverter<Counted> converter = countedConverter();
        DomainEventQueries<Counted> queries = queriesOver(converter, new Counted("a-1"), new Counted("b-1"), new Counted("a-2"));

        Mono<Integer> countForA = Projections.project(keyedProjection(), queries, "a");

        StepVerifier.create(countForA).expectNext(2).verifyComplete();
    }

    @Test
    void project_completes_empty_when_the_folded_state_is_null_even_with_many_events() {
        CloudEventConverter<Counted> converter = countedConverter();
        Counted[] events = new Counted[500];
        for (int i = 0; i < events.length; i++) {
            events[i] = new Counted(String.valueOf(i));
        }
        DomainEventQueries<Counted> queries = queriesOver(converter, events);

        Mono<@Nullable Object> state = Projections.project(nullStateProjection(), queries);

        StepVerifier.create(state).verifyComplete();
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

    private static Projection<@Nullable Object, Counted, String> nullStateProjection() {
        return Projection.<@Nullable Object, Counted>singletonBuilder(null)
                .on(Counted.class, (state, event) -> null)
                .build();
    }

    private static CloudEventConverter<Counted> countedConverter() {
        return new JacksonCloudEventConverter.Builder<Counted>(new ObjectMapper(), SOURCE).idMapper(Counted::eventId).build();
    }

    private static DomainEventQueries<Counted> queriesOver(CloudEventConverter<Counted> converter, Counted... events) {
        List<CloudEvent> cloudEvents = converter.toCloudEvents(List.of(events));
        return new DomainEventQueries<>(new EventStoreQueries() {
            @Override
            public Flux<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
                return Flux.fromIterable(cloudEvents);
            }

            @Override
            public Mono<Long> count(Filter filter) {
                return Mono.just((long) cloudEvents.size());
            }

            @Override
            public Mono<Boolean> exists(Filter filter) {
                return Mono.just(!cloudEvents.isEmpty());
            }
        }, converter);
    }
}
